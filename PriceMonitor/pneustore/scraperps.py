# -*- coding: utf-8 -*-
"""
Scraper PneuStore — com normalização brand/model/size por --config

- Normaliza marca e modelo (aliases/frases conhecidas) via JSON de config
- Extrai 'size' canônico (ex.: 175/70R13) além de 'medida' para pasta (175-70-r13)
- Adiciona brand/model/size no Product e nos arquivos (json/csv/sqlite)
- Mantém filtros por medida/marca/modelo usando as formas canônicas
"""

import abc
import argparse
import json
import logging
import random
import re
import csv
import sqlite3
import time
import unicodedata
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Set, Any, Dict
from urllib.parse import quote_plus

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException

DEFAULT_KNOWN_BRANDS = [
    "goodyear","kelly","pirelli","continental","michelin",
    "bridgestone","firestone","dunlop","maxxis","kumho",
    "yokohama","hankook","bfgoodrich","toyo","cooper","falken",
    "nexen","sumitomo","formula","general"
]

DEFAULT_MODEL_PHRASES = [
    # Goodyear / Kelly
    "assurance maxlife","assurance","wrangler","eagle","eagle sport","efficientgrip","kelly edge",
    # Michelin
    "energy xm2","primacy 4","ltx force",
    # Pirelli
    "cinturato p7","p400","p400 evo","formula evo","scorpion",
    # Continental
    "powercontact",
    # Dunlop
    "sp touring","sp sport","fm800","lm704","enasave ec300",
    # Outras recorrentes
    "direction","f700","bc20"
]

CONFIG_NORM: Dict[str, Dict | List] = {
    "known_brands": DEFAULT_KNOWN_BRANDS.copy(),
    "brand_aliases": { "kelly": "goodyear" },   # exemplo
    "known_model_phrases": DEFAULT_MODEL_PHRASES.copy(),
    "model_aliases": {
        "power contact": "powercontact",
        "powerontact": "powercontact",
        "cint p7": "cinturato p7",
        "scporion": "scorpion",
        "scporion ks": "scorpion",
    },
}

SIZE_CANON_RE = re.compile(r"(\d{3})\s*[/\-\s]?\s*(\d{2,3})\s*[rR]?\s*[-\s]?\s*(\d{2})")
MEDIDA_PATH_RE = re.compile(r"(\d{3})[\/\s-]+(\d{2,3})[\/\s-]*r?(\d{2})", flags=re.I)

def _norm_text(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii","ignore").decode("ascii")
    s = re.sub(r"[^a-z0-9 /\-]", " ", s.lower())
    return re.sub(r"\s+", " ", s).strip()

def _load_config_norm(path: Optional[str]):
    """Carrega e normaliza o JSON de configuração (se existir)."""
    global CONFIG_NORM
    if not path:
        return
    p = Path(path).expanduser().resolve()
    if not p.exists():
        print(f"[WARN] --config não encontrado: {p}. Usando defaults.")
        return
    try:
        with open(p, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        for k in ("known_brands", "brand_aliases", "known_model_phrases", "model_aliases"):
            if k in cfg:
                CONFIG_NORM[k] = cfg[k]

        CONFIG_NORM["known_brands"] = sorted({_norm_text(b) for b in CONFIG_NORM.get("known_brands", []) if b})
        CONFIG_NORM["brand_aliases"] = { _norm_text(k): _norm_text(v) for k,v in CONFIG_NORM.get("brand_aliases", {}).items() }
        CONFIG_NORM["known_model_phrases"] = sorted({_norm_text(m) for m in CONFIG_NORM.get("known_model_phrases", []) if m})
        CONFIG_NORM["model_aliases"] = { _norm_text(k): _norm_text(v) for k,v in CONFIG_NORM.get("model_aliases", {}).items() }
    except Exception as e:
        print(f"[WARN] Falha ao ler --config: {e}. Usando defaults.")

def _canon_brand(s: str) -> str:
    s = _norm_text(s)
    if not s:
        return ""
    if s in CONFIG_NORM["brand_aliases"]:
        return CONFIG_NORM["brand_aliases"][s]
    for kb in CONFIG_NORM["known_brands"]:
        if s == kb:
            return kb
    for kb in CONFIG_NORM["known_brands"]:
        if f" {kb} " in f" {s} ":
            return kb
    return s.split()[0]

def _brand_from_title(title: str, expected: str = "") -> str:
    t = _norm_text(title)
    exp = _canon_brand(expected)
    if exp:
        return exp
    for alias, target in CONFIG_NORM["brand_aliases"].items():
        if f" {alias} " in f" {t} ":
            return target
    for kb in CONFIG_NORM["known_brands"]:
        if f" {kb} " in f" {t} ":
            return kb
    return ""

def _canon_model(s: str) -> str:
    s = _norm_text(s)
    if not s:
        return ""
    if s in CONFIG_NORM["model_aliases"]:
        return CONFIG_NORM["model_aliases"][s]
    return s

def _model_from_title(title: str, brand: str = "", expected: str = "") -> str:
    t = _norm_text(title)
    if expected:
        return _canon_model(expected)
    for phrase in CONFIG_NORM["known_model_phrases"]:
        if phrase in t:
            return _canon_model(phrase)
    if brand and brand in t:
        after = t.split(brand, 1)[1].strip()
        toks = [w for w in after.split() if w not in {
            "pneu","aro","r12","r13","r14","r15","r16","r17","r18","r19","r20",
            "175/70r13","175/70","175-70","p","t","h","v","xl","runflat","rf","aro"
        }]
        if toks:
            return _canon_model(" ".join(toks[:2]))
    return ""

def _size_canonical(s: str) -> str:
    m = SIZE_CANON_RE.search(_norm_text(s))
    if not m:
        return ""
    return f"{m.group(1)}/{m.group(2)}R{m.group(3)}".upper()


PALAVRAS_KIT = [
    "kit","kits","conjunto","conjuntos","par","pares","04","4","duas","dois","quatro",
    "dupla","duplas","combo","combos","pack","packs","promoção","promocao","jogo","oferta","pacote","pacotes","lote","lotes",
]

def eh_kit_ou_multiplos_pneus(texto: str) -> bool:
    if not texto:
        return False
    texto_normalizado = unicodedata.normalize("NFD", texto)
    texto_limpo = "".join(c for c in texto_normalizado if unicodedata.category(c) != "Mn").lower()
    texto_sem_pontuacao = ''.join(char if char.isalnum() or char.isspace() else ' ' for char in texto_limpo)
    if any(p in texto_sem_pontuacao.split() for p in PALAVRAS_KIT):
        return True
    padroes_kit = [
        r'\b(kit|conjunto|par|pack|combo|lote|jogo)\b',
        r'\b(04|4)\s*(pneu|pneus|unidade|unidades)\b',
        r'\b(dois|duas|quatro)\s*(pneu|pneus)\b',
        r'\b(dupla|duplas)\s*(de\s*)?(pneu|pneus)\b',
        r'\b(promoção|promocao|oferta)\s*(kit|conjunto|par)\b',
        r'\b(kit|conjunto)\s*(com|de)\s*(04|4)\b'
    ]
    return any(re.search(p, texto_sem_pontuacao) for p in padroes_kit)

def _extrair_medida_path(termo_ou_titulo: str) -> Optional[str]:
    m = MEDIDA_PATH_RE.search(termo_ou_titulo or "")
    return f"{m[1]}-{m[2]}-r{m[3]}".lower() if m else None

def _extrair_preco_texto(texto: str) -> Optional[float]:
    if not texto: return None
    pats = [
        re.compile(r"R\$\s*([\d\.]+),(\d{2})", re.I),
        re.compile(r"([\d\.]+),(\d{2})", re.I),
        re.compile(r"R\$\s*([\d,]+\.\d{2})", re.I),
        re.compile(r"(\d{1,3}(?:\.\d{3})*),(\d{2})", re.I),
    ]
    for pattern in pats:
        match = pattern.search(texto)
        if match:
            try:
                if len(match.groups()) == 2:
                    inteiro, dec = match.groups()
                    return float(inteiro.replace(".","") + "." + dec)
                elif len(match.groups()) == 1:
                    return float(match.group(1).replace(",", ""))
            except Exception:
                continue
    return None

def _slugify_termo(termo: str) -> str:
    slug = unicodedata.normalize("NFKD", termo).encode("ascii","ignore").decode("ascii")
    slug = re.sub(r"[^\w\s-]", "", slug).strip().lower()
    slug = re.sub(r"[\s/]+", "-", slug)
    slug = re.sub(r"-{2,}", "-", slug)
    return slug

def construir_url(base, termo: str, page: int = 1, sort: str = "relevance"):
    termo_codificado = quote_plus(termo)
    termo_q_param = f"{termo_codificado}%3Arelevance" if sort == "relevance" else termo_codificado
    url = f"{base}/search/?sort={sort}&q={termo_q_param}"
    if page > 1:
        url += f"&page={page}"
    return url

def extrair_filtros_busca(termo: str):
    termo_low = _norm_text(termo or "")
    medida_path = _extrair_medida_path(termo_low)
    # brand esperado a partir do termo (token/alias)
    brand = ""
    for alias, target in CONFIG_NORM["brand_aliases"].items():
        if f" {alias} " in f" {termo_low} ":
            brand = target; break
    if not brand:
        for kb in CONFIG_NORM["known_brands"]:
            if f" {kb} " in f" {termo_low} ":
                brand = kb; break
    model = ""
    for phrase in CONFIG_NORM["known_model_phrases"]:
        if phrase in termo_low:
            model = CONFIG_NORM["model_aliases"].get(phrase, phrase)
            break
    return medida_path, brand, model

# =========================
# Dados
# =========================

@dataclass
class Product:
    titulo: str
    preco: Optional[float]
    link: str
    marketplace: str
    brand: str = ""  
    model: str = ""   
    size: str  = ""  
    medida: str = ""              
    aro: Optional[int] = None
    termo_busca: str = ""
    categoria: str = ""
    marca: str = ""              
    marca_filho: str = ""         
    local: str = ""
    vendedor: str = ""
    condicao: str = ""
    frete_gratis: bool = False
    data_coleta: str = ""
    caracteristicas: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

# =========================
# Base Scraper
# =========================

class ScraperBase(abc.ABC):
    marketplace: str = "base"

    @abc.abstractmethod
    def _construir_busca_url(self, termo: str, page: int = 1, sort: str = "relevance") -> str: ...

    @abc.abstractmethod
    def _coletar_produtos_pagina(self, links_vistos: Set[str]) -> List[Product]: ...

    def __init__(self, *, headless: bool = True, timeout: int = 15, delay_scroll: float = 0.8,
                 max_scrolls: int = 8, logger: Optional[logging.Logger] = None) -> None:
        self.headless = headless
        self.timeout = timeout
        self.delay_scroll = delay_scroll
        self.max_scrolls = max_scrolls
        self.driver = None
        self.logger = logger or self._setup_logger()
        self.termo_busca_atual = ""
        self.filtro_medida = self.filtro_marca = self.filtro_modelo = None

    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
            h = logging.StreamHandler()
            h.setFormatter(fmt)
            logger.addHandler(h)
        return logger

    def _configurar_driver(self):
        options = Options()
        if self.headless:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        return self.driver

    def _delay_aleatorio(self, min_delay: float = 0.5, max_delay: float = 2.0) -> float:
        return random.uniform(min_delay, max_delay)

    def _rolar_pagina(self) -> None:
        height = self.driver.execute_script("return document.body.scrollHeight")
        step = max(height // self.max_scrolls, 700)
        pos = 0
        for _ in range(self.max_scrolls):
            pos += step
            self.driver.execute_script("window.scrollTo(0, arguments[0]);", pos)
            time.sleep(self._delay_aleatorio())

    def buscar(self, termo: str, *, max_resultados: int = 100, max_paginas: int = 10,
               sort: str = "relevance") -> List[Product]:
        self.logger.info("🔍 Buscando '%s' em %s", termo, self.marketplace)
        self.termo_busca_atual = termo
        self.filtro_medida, self.filtro_marca, self.filtro_modelo = extrair_filtros_busca(termo)
        self.logger.info("Filtros: medida=%s | marca=%s | modelo=%s",
                         self.filtro_medida, self.filtro_marca, self.filtro_modelo)

        produtos: List[Product] = []
        vistos: Set[str] = set()

        try:
            self._configurar_driver()
            url_atual = self._construir_busca_url(termo, page=1, sort=sort)
            self.driver.get(url_atual)
            self._aceitar_cookies()

            sem_novos_seguidos = 0

            for pagina in range(1, max_paginas + 1):
                self._rolar_pagina()
                novos = self._coletar_produtos_pagina(vistos)
                if novos:
                    produtos.extend(novos)
                    sem_novos_seguidos = 0
                else:
                    sem_novos_seguidos += 1

                if len(produtos) >= max_resultados:
                    break
                if sem_novos_seguidos >= 2:
                    self.logger.info("Sem novos produtos em duas páginas consecutivas. Encerrando.")
                    break
                if pagina >= max_paginas:
                    break

                proxima_url = self._construir_busca_url(termo, page=pagina+1, sort=sort)
                if proxima_url == url_atual:
                    self.logger.info("URL próxima igual à atual. Parando.")
                    break
                url_atual = proxima_url
                self.driver.get(url_atual)
                time.sleep(self._delay_aleatorio())

            for prod in produtos[:max_resultados]:
                self._coletar_detalhes_produto(prod)

            if self.filtro_modelo:
                fm = _canon_model(self.filtro_modelo)
                produtos = [p for p in produtos if (p.model and fm in p.model) or (fm in _norm_text(p.titulo))]
            if self.filtro_marca:
                fb = _canon_brand(self.filtro_marca)
                produtos = [p for p in produtos if (p.brand and p.brand == fb)]

            if self.filtro_medida:
                produtos = [p for p in produtos if p.medida == self.filtro_medida or p.size.replace("/", "-").lower() == self.filtro_medida]

            return produtos[:max_resultados]

        except Exception as e:
            self.logger.error(f"Erro inesperado durante a busca: {e}", exc_info=True)
            return []
        finally:
            if self.driver:
                self.driver.quit()

    def _aceitar_cookies(self) -> None:
        pass

# =========================
# Scraper PneuStore
# =========================

class ScraperPneuStore(ScraperBase):
    marketplace = "pneustore"

    def __init__(self, headless: bool = True, delay_scroll: float = 1.0):
        super().__init__(headless=headless, delay_scroll=delay_scroll)
        self.base_url = "https://www.pneustore.com.br"

        self.card_selectors = [
            'div.product-grid-item.psNewUX',
            'div[data-testid="product-card"]',
            'div.product-item',
        ]
        self.price_selectors = [
            '.highlight','.price-highlight','.current-price',
            '.price-main','.price-value','.price'
        ]
        self.brand_selectors = ['div.area-brand','.brand-name','.product-brand']
        self.line_selectors  = ['[data-line]','.product-line','.variant-name']
        self.title_selectors = [
            'h3.product-name-title','h3[data-testid="product-card-title"]',
            '.product-name','.product-title'
        ]
        self.link_selectors  = ['a[href*="/produto/"]','a[data-testid="product-card-link"]']

    def _construir_busca_url(self, termo: str, page: int = 1, sort: str = "relevance") -> str:
        url = construir_url(self.base_url, termo, page, sort)
        self.logger.info(f"Construindo URL de busca para '{termo}': {url}")
        return url

    def _aceitar_cookies(self) -> None:
        try:
            time.sleep(2)
            cookie_selectors = [
                "#onetrust-accept-btn-handler",".onetrust-accept-btn-handler",
                "[id*='cookie'][id*='accept']","[class*='cookie'][class*='accept']",
                "button[aria-label*='aceitar']","button[aria-label*='Aceitar']","button.close-dialog"
            ]
            for selector in cookie_selectors:
                try:
                    btn = self.driver.find_element(By.CSS_SELECTOR, selector)
                    btn.click()
                    time.sleep(self._delay_aleatorio())
                    return
                except NoSuchElementException:
                    continue
        except Exception:
            pass

    # ------- helpers de scraping -------
    def _encontrar_elemento_com_fallback(self, parent, selectors: List[str],
                                         required: bool = True) -> Optional[Any]:
        for selector in selectors:
            try:
                element = parent.find_element(By.CSS_SELECTOR, selector)
                if element and (element.text.strip() or element.get_attribute("href")):
                    return element
            except NoSuchElementException:
                continue
        return None

    def _extrair_preco_card(self, card) -> Optional[float]:
        elem = self._encontrar_elemento_com_fallback(card, self.price_selectors, required=False)
        if elem:
            v = _extrair_preco_texto(elem.text.strip())
            if v: return v
        try:
            txt = card.get_attribute("innerText") or card.text
            return _extrair_preco_texto(txt)
        except Exception:
            return None

    def _coletar_produtos_pagina(self, links_vistos: Set[str]) -> List[Product]:
        try:
            WebDriverWait(self.driver, self.timeout).until(
                lambda d: any(d.find_elements(By.CSS_SELECTOR, s) for s in self.card_selectors)
            )
            time.sleep(self._delay_aleatorio(2, 3))
        except TimeoutException:
            self.logger.warning("Timeout aguardando cards de produtos")
            return []

        cards = []
        for selector in self.card_selectors:
            try:
                found = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if found:
                    cards = found; break
            except Exception:
                continue

        self.logger.info(f">>> Encontrados {len(cards)} cards na página")
        produtos: List[Product] = []

        for i, card in enumerate(cards):
            try:
                time.sleep(self._delay_aleatorio(0.8, 1.2))

                link_el = self._encontrar_elemento_com_fallback(card, self.link_selectors)
                if not link_el: continue
                link = link_el.get_attribute("href")
                if not link or link in links_vistos: continue

                title_el = self._encontrar_elemento_com_fallback(card, self.title_selectors)
                if not title_el: continue
                titulo = (title_el.text or "").strip()
                if not titulo: continue

                if eh_kit_ou_multiplos_pneus(titulo): continue
                if card.find_elements(By.CSS_SELECTOR, ".out-of-stock,.soldout,.esgotado,[data-stock='0']"):
                    continue

                size_canon = _size_canonical(titulo) or ""
                medida_path = _extrair_medida_path(titulo) or ""
                brand = _brand_from_title(titulo, expected=self.filtro_marca or "")
                model = _model_from_title(titulo, brand=brand, expected=self.filtro_modelo or "")

                if self.filtro_marca and brand != _canon_brand(self.filtro_marca):
                    continue
                if self.filtro_modelo:
                    fm = _canon_model(self.filtro_modelo)
                    if not (model and fm in model) and fm not in _norm_text(titulo):
                        continue
                if self.filtro_medida:
                    if not medida_path or (medida_path != self.filtro_medida and size_canon.replace("/", "-").lower() != self.filtro_medida):
                        continue

                preco = self._extrair_preco_card(card)

                prod = Product(
                    titulo=titulo,
                    preco=preco,
                    link=link,
                    marketplace=self.marketplace,
                    data_coleta=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    # canônicos
                    brand=brand,
                    model=model,
                    size=size_canon,
                    # compat
                    marca=brand,
                    marca_filho=model.title() if model else "",
                    medida=medida_path,
                    aro=int(size_canon.split("R")[-1]) if "R" in size_canon else None,
                    termo_busca=self.termo_busca_atual,
                )
                produtos.append(prod)
                links_vistos.add(link)

            except Exception as e:
                self.logger.warning(f"Erro ao processar card {i+1}: {e}")
                continue

        self.logger.info(f"Coletados {len(produtos)} produtos válidos nesta página")
        return produtos

    def _coletar_detalhes_produto(self, product: Product) -> None:
        if not self.driver:
            self.driver = self._configurar_driver()
        try:
            self.driver.get(product.link)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h1"))
            )
            time.sleep(self._delay_aleatorio(1.5, 2.5))

            # reforça título/medida/brand/model
            try:
                titulo_det = (self.driver.find_element(By.CSS_SELECTOR, "h1").text or "").strip()
                if titulo_det:
                    product.titulo = titulo_det
                    if not product.size:
                        product.size = _size_canonical(titulo_det) or product.size
                    if not product.medida:
                        product.medida = _extrair_medida_path(titulo_det) or product.medida
                    if not product.brand:
                        product.brand = _brand_from_title(titulo_det, expected=product.brand)
                        product.marca = product.brand
                    if not product.model:
                        product.model = _model_from_title(titulo_det, brand=product.brand, expected=product.model)
                        product.marca_filho = product.model.title() if product.model else product.marca_filho
                    if not product.aro and product.size and "R" in product.size:
                        product.aro = int(product.size.split("R")[-1])
            except Exception:
                pass

            price_detail_selectors = [
                'div[data-testid="product-price"] p.text-3xl',
                '.product-price .price-value','.price-current','.price-highlight'
            ]
            for sel in price_detail_selectors:
                els = self.driver.find_elements(By.CSS_SELECTOR, sel)
                if els:
                    v = _extrair_preco_texto(els[0].text.strip())
                    if v: product.preco = v; break

            # specs (opcional)
            specs_selectors = [
                'div[data-testid="drawer-technical-details"]',
                '.technical-details','.product-specs','.specifications'
            ]
            for sel in specs_selectors:
                try:
                    cont = self.driver.find_element(By.CSS_SELECTOR, sel)
                    rows = cont.find_elements(By.CSS_SELECTOR, "div.flex.justify-between")
                    for row in rows:
                        divs = row.find_elements(By.XPATH, "./div")
                        if len(divs) >= 2:
                            key = (divs[0].text or "").strip()
                            val = (divs[1].text or "").strip()
                            if key and val:
                                product.caracteristicas[key] = val
                    break
                except Exception:
                    continue

        except Exception as e:
            self.logger.warning(f"[detalhes] Falha em {product.link}: {e}")

# =========================
# Persistência
# =========================

def _base_out(termo: str, output_dir: str = "dados") -> tuple[Path, str, str]:
    medida = _extrair_medida_path(termo) or "medida_desconhecida"
    termo_slug = _slugify_termo(termo)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_path = Path(output_dir) / "raw" / medida
    base_path.mkdir(parents=True, exist_ok=True)
    return base_path, termo_slug, timestamp

def salvar_produtos_json(produtos: List[Product], termo: str, output_dir: str = "dados") -> Optional[Path]:
    if not produtos: return None
    base_path, termo_slug, ts = _base_out(termo, output_dir)
    p = base_path / f"{termo_slug}_{ts}.json"
    with p.open("w", encoding="utf-8") as f:
        json.dump([x.to_dict() for x in produtos], f, ensure_ascii=False, indent=2)
    return p

def salvar_produtos_csv(produtos: List[Product], termo: str, output_dir: str = "dados") -> Optional[Path]:
    if not produtos: return None
    base_path, termo_slug, ts = _base_out(termo, output_dir)
    p = base_path / f"{termo_slug}_{ts}.csv"
    with p.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=produtos[0].to_dict().keys())
        writer.writeheader()
        for x in produtos:
            writer.writerow(x.to_dict())
    return p

def salvar_produtos_sqlite(produtos: List[Product], termo: str, output_dir: str = "dados") -> Optional[Path]:
    if not produtos: return None
    base_path, termo_slug, ts = _base_out(termo, output_dir)
    p = base_path / f"{termo_slug}_{ts}.sqlite"
    conn = sqlite3.connect(p)
    cur = conn.cursor()
    d = produtos[0].to_dict()
    columns = ', '.join([f"{k} TEXT" for k in d.keys()])
    cur.execute(f"CREATE TABLE IF NOT EXISTS produtos ({columns})")
    placeholders = ', '.join('?' for _ in d.keys())
    for x in produtos:
        cur.execute(f"INSERT INTO produtos VALUES ({placeholders})", tuple(str(v) for v in x.to_dict().values()))
    conn.commit(); conn.close()
    return p

def salvar_produtos_multiformato(produtos: List[Product], termo: str, output_dir: str = "dados", formatos=None) -> dict:
    if formatos is None: formatos = ["json"]
    caminhos = {}
    if "json" in formatos:
        p = salvar_produtos_json(produtos, termo, output_dir)
        if p: caminhos["json"] = str(p)
    if "csv" in formatos:
        p = salvar_produtos_csv(produtos, termo, output_dir)
        if p: caminhos["csv"] = str(p)
    if "sqlite" in formatos:
        p = salvar_produtos_sqlite(produtos, termo, output_dir)
        if p: caminhos["sqlite"] = str(p)
    return caminhos

# =========================
# CLI
# =========================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper PneuStore com normalização brand/model/size")
    parser.add_argument("--termo", help="Termo de busca")
    parser.add_argument("--max", type=int, default=100, help="Máximo de resultados")
    parser.add_argument("--output-dir", default="dados", help="Pasta de saída")
    parser.add_argument("--sort", default="relevance",
                        choices=["relevance","price-asc","price-desc","name-asc","name-desc","top-sellers"],
                        help="Critério de ordenação.")
    parser.add_argument("--window", action="store_true", help="Mostrar navegador (não-headless)")
    parser.add_argument("--lote-json", type=str, help="Caminho do JSON de queries (ex: query_products.json)")
    parser.add_argument("--formatos", nargs="+", choices=["json","csv","sqlite"], default=["csv"],
                        help="Formatos de saída")
    parser.add_argument("--config", help="JSON com known_brands/brand_aliases/known_model_phrases/model_aliases")

    args = parser.parse_args()
    _load_config_norm(args.config)

    if args.lote_json:
        with open(args.lote_json, "r", encoding="utf-8") as f:
            queries = json.load(f)
        for idx, item in enumerate(queries):
            termo = (
                item.get("query_flex")
                or item.get("query_strict")
                or item.get("termo")
                or f"pneu {item.get('width')}/{item.get('aspect')}R{item.get('rim')} {item.get('brand')} {item.get('line_model')}"
            )
            print(f"\n=== {idx+1}/{len(queries)}: {termo} ===")
            scraper = ScraperPneuStore(headless=not args.window)
            produtos = scraper.buscar(termo, max_resultados=args.max, sort=args.sort)
            caminhos = salvar_produtos_multiformato(produtos, termo, args.output_dir, args.formatos)
            if not caminhos:
                print("⚠️ Nenhum produto encontrado, nada salvo.")
            else:
                for formato, caminho in caminhos.items():
                    print(f"✅ {len(produtos)} produtos salvos em {caminho}")
        exit(0)

    if not args.termo:
        print("Você deve passar --termo ou --lote-json.")
        exit(1)

    scraper = ScraperPneuStore(headless=not args.window)
    produtos = scraper.buscar(args.termo, max_resultados=args.max, sort=args.sort)
    caminhos = salvar_produtos_multiformato(produtos, args.termo, args.output_dir, args.formatos)
    if not caminhos:
        print("⚠️ Nenhum produto encontrado, nada salvo.")
    else:
        for formato, caminho in caminhos.items():
            print(f"✅ {len(produtos)} produtos salvos em {caminho}")
