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
from typing import List, Optional, Set, Any
from urllib.parse import quote_plus



from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException

# ===== Listas determinísticas (multi‑palavra primeiro para capturar modelo composto) =====
MARCAS = [
    "goodyear", "pirelli", "michelin", "dunlop", "bridgestone", "continental",
    "hankook", "bfgoodrich", "firestone", "kumho", "atras", "maxxis", "formula",
    "yokohama", "toyo", "nitto", "general", "cooper", "falken", "nexen", "sumitomo"
]

MODELOS = [
    "kelly edge", "formula evo", "sp touring",
    "assurance", "maxlife", "efficientgrip",
    "wrangler", "eagle", "energy", "direction","kelly", "p400", "bc20"
    "direction", "f700"
]

PALAVRAS_KIT = [
    "kit", "kits", "conjunto", "conjuntos", "par", "pares",
    "04", "4", "duas", "dois", "quatro", "dupla",
    "duplas", "combo", "combos", "pack", "packs", "promoção",
    "promocao", "jogo", "oferta", "pacote", "pacotes", "lote", "lotes",
]

PRICE_PATTERNS = [
    re.compile(r"R\$\s*([\d\.]+),(\d{2})", re.IGNORECASE),
    re.compile(r"([\d\.]+),(\d{2})", re.IGNORECASE),
    re.compile(r"R\$\s*([\d,]+\.\d{2})", re.IGNORECASE),
    re.compile(r"(\d{1,3}(?:\.\d{3})*),(\d{2})", re.IGNORECASE),
]

BRAND_PATTERNS = [
    re.compile(r"\b(pneu\s+)?(" + "|".join(MARCAS) + r")\b", re.IGNORECASE),
    re.compile(r"^([a-zA-Z]+)", re.IGNORECASE),
]

MEDIDA_RE = re.compile(r"(\d{3})[\/\s-]+(\d{2,3})[\/\s-]*r?(\d{2})", flags=re.I)


def _extrair_informacao_de_aro(medida_str: str) -> Optional[int]:
    if medida_str:
        m = re.search(r"r(\d{2,3})$", medida_str, re.IGNORECASE)
        if m:
            return int(m[1])
    return None


def eh_kit_ou_multiplos_pneus(texto: str) -> bool:
    if not texto:
        return False
    texto_normalizado = unicodedata.normalize("NFD", texto)
    texto_limpo = "".join(c for c in texto_normalizado if unicodedata.category(c) != "Mn").lower()
    texto_sem_pontuacao = ''.join(char if char.isalnum() or char.isspace() else ' ' for char in texto_limpo)
    palavras = texto_sem_pontuacao.split()
    for palavra in palavras:
        if palavra in PALAVRAS_KIT:
            return True
    padroes_kit = [
        r'\b(kit|conjunto|par|pack|combo|lote|jogo)\b',
        r'\b(04|4)\s*(pneu|pneus|unidade|unidades)\b',
        r'\b(dois|duas|quatro)\s*(pneu|pneus)\b',
        r'\b(dupla|duplas)\s*(de\s*)?(pneu|pneus)\b',
        r'\b(promoção|promocao|oferta)\s*(kit|conjunto|par)\b',
        r'\b(kit|conjunto)\s*(com|de)\s*(04|4)\b'
    ]
    return any(re.search(p, texto_limpo) for p in padroes_kit)


def _extrair_medida(termo: str):
    m = MEDIDA_RE.search(termo)
    return f"{m[1]}-{m[2]}-r{m[3]}" if m else None


def _extrair_preco_texto(texto: str) -> Optional[float]:
    if not texto:
        return None
    for pattern in PRICE_PATTERNS:
        match = pattern.search(texto)
        if match:
            try:
                if len(match.groups()) == 2:
                    inteiro, decimais = match.groups()
                    preco_str = inteiro.replace(".", "") + "." + decimais
                    return float(preco_str)
                elif len(match.groups()) == 1:
                    preco_str = match.group(1).replace(",", "")
                    return float(preco_str)
            except (ValueError, IndexError):
                continue
    return None


def _extrair_marca_titulo(titulo: str) -> str:
    if not titulo:
        return ""
    titulo_clean = titulo.lower().strip()
    for pattern in BRAND_PATTERNS:
        match = pattern.search(titulo_clean)
        if match:
            marca_candidata = match.group(2) if len(match.groups()) > 1 else match.group(1)
            marca_candidata = marca_candidata.strip().lower()
            if marca_candidata in MARCAS:
                return marca_candidata.upper()
    primeira_palavra = titulo_clean.split()[0] if titulo_clean.split() else ""
    return primeira_palavra.upper() if primeira_palavra in MARCAS else ""


def _slugify_termo(termo: str) -> str:
    slug = unicodedata.normalize("NFKD", termo).encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^\w\s-]", "", slug).strip().lower()
    slug = re.sub(r"[\s/]+", "-", slug)
    slug = re.sub(r"-{2,}", "-", slug)
    return slug


def construir_url(base, termo: str, page: int = 1, sort: str = "relevance"):
    termo_codificado = quote_plus(termo)
    if sort == "relevance":
        termo_q_param = f"{termo_codificado}%3Arelevance"
    else:
        termo_q_param = termo_codificado
    url = f"{base}/search/?sort={sort}&q={termo_q_param}"
    if page > 1:
        url += f"&page={page}"
    return url


def extrair_filtros_busca(termo: str):
    termo_low = termo.lower()
    medida = _extrair_medida(termo_low)
    tokens = re.findall(r'\w+', termo_low)

    marca = None
    for m in MARCAS:
        if m in tokens:
            marca = m
            break

    modelo = None
    for mod in MODELOS:
        if mod in termo_low:
            modelo = mod
            break
    return medida, marca, modelo


@dataclass
class Product:
    titulo: str
    preco: Optional[float]
    link: str
    marketplace: str
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


class ScraperBase(abc.ABC):
    marketplace: str = "base"

    @abc.abstractmethod
    def _construir_busca_url(self, termo: str, page: int = 1, sort: str = "relevance") -> str:
        ...

    @abc.abstractmethod
    def _coletar_produtos_pagina(self, links_vistos: Set[str]) -> List[Product]:
        ...

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
        self.logger.info(
            "Filtros: medida=%s | marca=%s | modelo=%s",
            self.filtro_medida, self.filtro_marca, self.filtro_modelo
        )
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
                    
                proxima_url = self._construir_busca_url(termo, page=pagina + 1, sort=sort)
                if proxima_url == url_atual:
                    self.logger.info("URL próxima igual à atual. Parando.")
                    break
                url_atual = proxima_url
                self.driver.get(url_atual)
                time.sleep(self._delay_aleatorio())

            
            for prod in produtos[:max_resultados]:
                self._coletar_detalhes_produto(prod)

            if self.filtro_modelo:
                produtos = [
                    p for p in produtos
                    if (p.marca_filho and self.filtro_modelo.lower() in p.marca_filho.lower())
                    or (self.filtro_modelo.lower() in p.titulo.lower())
                ]
            if self.filtro_marca:
                produtos = [p for p in produtos if p.marca.lower() == self.filtro_marca]

            if self.filtro_medida:
                produtos = [p for p in produtos if p.medida == self.filtro_medida]

            return produtos[:max_resultados]

        except Exception as e:
            self.logger.error(f"Erro inesperado durante a busca: {e}", exc_info=True)
            return []
        finally:
            if self.driver:
                self.driver.quit()

    def _aceitar_cookies(self) -> None: 
        pass


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
            '.highlight', '.price-highlight', '.current-price',
            '.price-main', '.price-value', '.price'
        ]
        self.brand_selectors = ['div.area-brand', '.brand-name', '.product-brand']
        self.line_selectors = ['[data-line]', '.product-line', '.variant-name']
        self.title_selectors = [
            'h3.product-name-title',
            'h3[data-testid="product-card-title"]',
            '.product-name', '.product-title'
        ]
        self.link_selectors = ['a[href*="/produto/"]', 'a[data-testid="product-card-link"]']

    def _construir_busca_url(self, termo: str, page: int = 1, sort: str = "relevance") -> str:
        url = construir_url(self.base_url, termo, page, sort)
        self.logger.info(f"Construindo URL de busca para termo '{termo}': {url}")
        return url

    def _aceitar_cookies(self) -> None:
        try:
            time.sleep(2)
            cookie_selectors = [
                "#onetrust-accept-btn-handler",
                ".onetrust-accept-btn-handler",
                "[id*='cookie'][id*='accept']",
                "[class*='cookie'][class*='accept']",
                "button[aria-label*='aceitar']",
                "button[aria-label*='Aceitar']",
                "button.close-dialog"
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
        price_element = self._encontrar_elemento_com_fallback(card, self.price_selectors, required=False)
        if price_element:
            preco = _extrair_preco_texto(price_element.text.strip())
            if preco:
                return preco
        try:
            texto_completo = card.get_attribute("innerText") or card.text
            preco = _extrair_preco_texto(texto_completo)
            if preco:
                return preco
        except Exception:
            pass
        return None

    def _extrair_marca_card(self, card, titulo: str) -> str:
        brand_element = self._encontrar_elemento_com_fallback(card, self.brand_selectors, required=False)
        if brand_element:
            marca_txt = brand_element.text.strip().lower()
            if marca_txt in MARCAS:
                return marca_txt.upper()
        return _extrair_marca_titulo(titulo)

    def _extrair_marca_filho(self, card, titulo: str) -> str:
        line_element = self._encontrar_elemento_com_fallback(card, self.line_selectors, required=False)
        if line_element:
            txt = line_element.text.strip()
            for m in MODELOS:
                if m.lower() in txt.lower():
                    return m.title()
            return txt
        titulo_low = titulo.lower()
        for m in MODELOS:
            if m.lower() in titulo_low:
                return m.title()
        return ""

    def _encontrar_cards_produtos(self) -> List[Any]:
        for selector in self.card_selectors:
            try:
                cards = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if cards:
                    return cards
            except Exception:
                continue
        return []

    def _coletar_produtos_pagina(self, links_vistos: Set[str]) -> List[Product]:
        try:
            WebDriverWait(self.driver, self.timeout).until(
                lambda d: any(d.find_elements(By.CSS_SELECTOR, s) for s in self.card_selectors)
            )
            time.sleep(self._delay_aleatorio(2, 3))
        except TimeoutException:
            self.logger.warning("Timeout aguardando cards de produtos")
            return []

        cards = self._encontrar_cards_produtos()
        self.logger.info(f">>> Encontrados {len(cards)} cards na página")

        produtos: List[Product] = []

        for i, card in enumerate(cards):
            try:
                time.sleep(self._delay_aleatorio(0.8, 1.2))

                link_element = self._encontrar_elemento_com_fallback(card, self.link_selectors)
                if not link_element:
                    continue
                link = link_element.get_attribute("href")
                if not link or link in links_vistos:
                    continue

                title_element = self._encontrar_elemento_com_fallback(card, self.title_selectors)
                if not title_element:
                    continue
                titulo = title_element.text.strip()
                if not titulo:
                    continue

                if eh_kit_ou_multiplos_pneus(titulo):
                    continue
                if card.find_elements(By.CSS_SELECTOR, ".out-of-stock, .soldout, .esgotado, [data-stock='0']"):
                    continue

                medida_produto = _extrair_medida(titulo)
                aro_produto = _extrair_informacao_de_aro(medida_produto)

                if self.filtro_medida:
                    if not medida_produto or medida_produto != self.filtro_medida:
                        continue

                marca = self._extrair_marca_card(card, titulo)
                if not marca:
                    continue
                marca_filho = self._extrair_marca_filho(card, titulo)

                if self.filtro_marca and marca.lower() != self.filtro_marca:
                    continue

                if self.filtro_modelo:
                    modelo_ok = False
                    if marca_filho and self.filtro_modelo.lower() in marca_filho.lower():
                        modelo_ok = True
                    if self.filtro_modelo.lower() in titulo.lower():
                        modelo_ok = True
                    if not modelo_ok:
                        continue

                preco = self._extrair_preco_card(card)

                produto = Product(
                    titulo=titulo,
                    preco=preco,
                    link=link,
                    marca=marca,
                    marca_filho=marca_filho,
                    marketplace=self.marketplace,
                    data_coleta=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    medida=medida_produto or "",
                    aro=aro_produto,
                    termo_busca=self.termo_busca_atual,
                )
                produtos.append(produto)
                links_vistos.add(link)

            except Exception as e:
                self.logger.warning(f"Erro ao processar card {i + 1}: {e}")
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

            price_detail_selectors = [
                'div[data-testid="product-price"] p.text-3xl',
                '.product-price .price-value', '.price-current', '.price-highlight'
            ]
            price_element = self._encontrar_elemento_com_fallback(self.driver, price_detail_selectors, required=False)
            if price_element:
                preco_extraido = _extrair_preco_texto(price_element.text.strip())
                if preco_extraido:
                    product.preco = preco_extraido

            try:
                titulo_det = self.driver.find_element(By.CSS_SELECTOR, "h1").text.strip()
                if titulo_det:
                    product.titulo = titulo_det  # atualiza
                    if not product.medida:
                        product.medida = _extrair_medida(titulo_det) or product.medida
                    if not product.aro:
                        product.aro = _extrair_informacao_de_aro(product.medida)
                    # tenta reforçar marca_filho no detalhe
                    for m in MODELOS:
                        if m.lower() in titulo_det.lower():
                            product.marca_filho = m.title()
                            break
            except Exception:
                pass

            specs = {}
            specs_selectors = [
                'div[data-testid="drawer-technical-details"]',
                '.technical-details', '.product-specs', '.specifications'
            ]
            specs_container = self._encontrar_elemento_com_fallback(self.driver, specs_selectors, required=False)
            if specs_container:
                for row in specs_container.find_elements(By.CSS_SELECTOR, "div.flex.justify-between"):
                    try:
                        divs = row.find_elements(By.XPATH, "./div")
                        if len(divs) >= 2:
                            key = divs[0].text.strip()
                            val = divs[1].text.strip()
                            if key and val:
                                specs[key] = val
                    except Exception:
                        continue
            if specs:
                product.caracteristicas.update(specs)
        except Exception as e:
            self.logger.warning(f"[detalhes] Falha em {product.link}: {e}")


def salvar_produtos_csv(produtos: List[Product], termo: str, output_dir: str = "data") -> Optional[Path]:
    if not produtos:
        return None

    medida = _extrair_medida(termo) or "medida_desconhecida"
    termo_slug = _slugify_termo(termo)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_path = Path(output_dir) / "raw" / medida
    base_path.mkdir(parents=True, exist_ok=True)
    file_path = base_path / f"{termo_slug}_{timestamp}.csv"

    with file_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=produtos[0].to_dict().keys())
        writer.writeheader()
        for p in produtos:
            writer.writerow(p.to_dict())

    return file_path

def salvar_produtos_sqlite(produtos: List[Product], termo: str, output_dir: str = "data") -> Optional[Path]:
    if not produtos:
        return None

    medida = _extrair_medida(termo) or "medida_desconhecida"
    termo_slug = _slugify_termo(termo)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_path = Path(output_dir) / "raw" / medida
    base_path.mkdir(parents=True, exist_ok=True)
    file_path = base_path / f"{termo_slug}_{timestamp}.sqlite"

    conn = sqlite3.connect(file_path)
    cursor = conn.cursor()
    d = produtos[0].to_dict()
    columns = ', '.join([f"{k} TEXT" for k in d.keys()])
    cursor.execute(f"CREATE TABLE IF NOT EXISTS produtos ({columns})")

    for p in produtos:
        values = tuple(str(v) for v in p.to_dict().values())
        placeholders = ', '.join('?' for _ in d.keys())
        cursor.execute(f"INSERT INTO produtos VALUES ({placeholders})", values)

    conn.commit()
    conn.close()
    return file_path


def salvar_produtos_json(produtos: List[Product], termo: str, output_dir: str = "data") -> Optional[Path]:
    if not produtos:
        print("Nenhum produto encontrado para salvar.")
        return None
    
    medida = _extrair_medida(termo) or "medida_desconhecida"
    termo_slug = _slugify_termo(termo)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_path = Path(output_dir) / "raw" / medida
    base_path.mkdir(parents=True, exist_ok=True)
    file_path = base_path / f"{termo_slug}_{timestamp}.json"
    with file_path.open("w", encoding="utf-8") as f:
        json.dump([p.to_dict() for p in produtos], f, ensure_ascii=False, indent=2)
    return file_path

def salvar_produtos_multiformato(produtos: List[Product], termo: str, output_dir: str = "data", formatos=None) -> dict:
    if formatos is None:
        formatos = ["json"]

    caminhos = {}

    if "json" in formatos:
        caminho_json = salvar_produtos_json(produtos, termo, output_dir)
        if caminho_json:
            caminhos["json"] = str(caminho_json)

    if "csv" in formatos:
        caminho_csv = salvar_produtos_csv(produtos, termo, output_dir)
        if caminho_csv:
            caminhos["csv"] = str(caminho_csv)

    if "sqlite" in formatos:
        caminho_sqlite = salvar_produtos_sqlite(produtos, termo, output_dir)
        if caminho_sqlite:
            caminhos["sqlite"] = str(caminho_sqlite)

    return caminhos

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--termo", help="Termo de busca")
    parser.add_argument("--max", type=int, default=100, help="Máximo de resultados")
    parser.add_argument("--output-dir", default="dados", help="Pasta de saída")
    parser.add_argument("--sort", default="relevance",
                        choices=["relevance", "price-asc", "price-desc", "name-asc", "name-desc", "top-sellers"],
                        help="Critério de ordenação.")
    parser.add_argument("--window", action="store_true", help="Mostrar navegador (não-headless)")
    parser.add_argument("--lote-json", type=str, help="Caminho do JSON de queries (ex: query_products.json)")
    parser.add_argument("--formatos", nargs="+", choices=["json", "csv", "sqlite"], default=["csv"],
                        help="Formatos de saída (csv, sqlite, json).")
    args = parser.parse_args()

    if args.lote_json:
        with open(args.lote_json, "r", encoding="utf-8") as f:
            queries = json.load(f)
        for idx, item in enumerate(queries):
            termo = (
                item.get("query_flex") or
                item.get("query_strict") or
                item.get("termo") or
                f"pneu {item.get('width')}/{item.get('aspect')}R{item.get('rim')} {item.get('brand')} {item.get('line_model')}"
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
