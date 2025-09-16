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
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from Scraper_em_geral._common.canon import to_canonical
from Scraper_em_geral._common.io_utils import write_jsonl
from Scraper_em_geral._common.validate import validate_or_warn

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
]

DEFAULT_KNOWN_BRANDS = [
    'aderenza', 'anlas', 'anteo', 'aplus', 'aptany', 'atlander', 'austone', 'barum','advance','agriking','alliance', 'amazon','ascenso', 'bf goodrich', 'bfgoodrich', 
    'blacklion', 'bkt', 'borilli', 'bridgestone','bkt', 'carlisle', 'ceat', 'chengshan', 'chituma', 'comforser', 'compasal', 
    'continental', 'cooper', 'davanti', 'dayton', 'delmax', 'dewostone', 'double king', 'doubleking', 'doublestar', 'dunlop', 
    'durable', 'dyna', 'dynamo', 'ecovision', 'falken', 'farroad', 'fate', 'federal', 'firemax', 'firestone', 'forceland', 
    'formula','forerunner' 'general', 'goform', 'goodride', 'goodyear', 'gt radial', 'gripmaster', 'habilead', 'hankook', 'hifly', 'hilfy', 'horizon', 
    'infinity', 'invovic', 'ironman', 'itaro', 'jk tyre', 'jktyre', 'kenda', 'kingtyre', 'kpatos', 'kumho', 'kumho tire', 
    'landspider', 'lanvigator', 'lavigator', 'levorin', 'linglong', 'luistone', 'magnum', 'maxxis', 'mazzini', 'michelin', 
    'milever', 'minerva', 'nankang', 'nexen', 'nitto', 'nokian', 'onyx', 'otani', 'petlas', 'pirelli', 'power trac', 'primewell', 
    'radar', 'roadcruza', 'roadstone', 'routeway', 'royal black', 'sailun', 'sava', 'semperit', 'speedmax', 'sumitomo', 
    'sumitomo tire', 'sumitomo tires', 'sunfull', 'sunny', 'sunset', 'sunset tires', 'sunwide', 'tbb tires', 'toyo', 'towin', 
    'tracmax', 'trazano', 'triangle', 'valeo', 'vee rubber', 'ventus', 'versatyre', 'viemar', 'viking', 'vitour', 'wanli', 
    'westlake', 'windforce', 'winrun', 'xbri', 'yokohama', 'zmax', 'zptire'
]

DEFAULT_MODEL_PHRASES = [
    '503112', 'a/t csr34', 'a607', 'a609', 'a609 (100h)', 'a610', 'a610 (103y)', 'a919', 'agilis', 'agilis 3', 'alenza001', 
    'all terrain', 'all terrain t/a', 'all terrain ta', 'assur. maxlife', 'assurance', 'assurance maxlife', 'at59', 'at78', 
    'athena sp302', 'atrezzo', 'barum bravuris 4x4', 'barum bravuris 5hm', 'bc100', 'bc20', 'blazer hp', 'blazer uhp', 
    'blazer uhp 2', 'brutus all terrain', 'brutus t/a', 'cargo marathon 2', 'catchfors t/a', 'catchpower plus', 'cf1100', 
    'cf2000', 'cf500', 'cf510', 'cint p1 plus', 'cint p7', 'city dc01', 'citytraxx', 'comfort 2', 'comfort ii', 'comfort ii xl', 
    'confort ii', 'conticrosscontact lx2', 'contisport contact', 'controlmax', 'cp-16', 'cp16', 'cr976a', 'crosswind a/t', 'd300', 
    'destination a/t', 'destination atx', 'destination h/t', 'destination le3', 'dh02', 'dh03', 'direction 2 suv', 
    'direction touring', 'direzza dz102', 'dk365 ht', 'dk365 tl', 'dk558', 'dk728', 'dk798', 'dr755', 'dsrs01', 'dsu02', 
    'dueler a/t revo 2', 'dueler at693', 'dueler h/t 684 ii', 'dueler h/t 684 iii', 'dueler h/t 684 iii ecopia', 'dynapro at2', 
    'dynapro mt2', 'dz102', 'eagle', 'eagle sport', 'eagle sport 2', 'eco307', 'eco603', 'eco603 xl', 'ecoblue ry26', 'ecoblue ry6', 
    'ecodrive', 'ecology', 'ecopia ep150', 'ecosaver ht', 'edge suv 2', 'edge suv 2 sl', 'efficientgrip', 'efficientgrip suv', 
    'el601', 'enasave ec300', 'enasave ec300+', 'enasave ec350+', 'energy', 'energy xm2', 'enzo b2', 'ep150', 'es31', 'evo', 
    'expresspro', 'f-600', 'f700', 'fastdrive', 'fastway a5', 'fm601', 'fm800', 'fortitude ht', 'forza 2 a/t', 'forza a/t 2', 
    'forza a/t f2', 'forza ht 2 extra', 'frd26', 'frd66', 'frd96', 'fs558', 'furious s1', 'g32 cargo', 'gallopro ht', 
    'generaltire altimax one', 'giornata', 'grandt at5', 'grandtrek at20', 'grandtrek at25', 'grandtrek at5', 'grandtrek mt2', 
    'grantek at5', 'grantrek at5', 'green-max van', 'grip master c/s', 'gs03', 'h188', 'h220', 'hf261', 'hh102', 'hh301', 'hr805', 
    'ht wrgl territory', 'ht782', 'hu901', 'it101', 'it203', 'it01', 'itr01t', 'kelly edge', 'kelly edge sport', 'kelly edge sport 2',
    'kelly edge touring 2', 'kinergy gt', 'kl33', 'landgema', 'linam r51', 'llf86', 'lm 704', 'ltx force', 'ltx trail', 
    'ltx trail st', 'ma349', 'marathon 2', 'matrix sport ii', 'maximum dh03', 'mh01', 'mp270', 'mu069', 'n92018', 'na305', 
    'new sense', 'nu025', 'nu025 h/t', 'ny-20', 'ny805', 'ny901', 'opteco s1', 'ottima plus', 'over cargo b3 8pr', 'p400', 
    'p400 evo', 'pangea all terrain', 'pangea at', 'perform', 'performax ht', 'pilot sport 4 suv', 'power contact 2', 
    'powercontact 2', 'powergy', 'powermax', 'premium f1', 'primacy 4', 'primacy 4+', 'protoura sport', 'r330', 'r380', 'ra1100', 
    'ra1100 at', 'ra301', 'ra305', 'ra7000', 'reinforced bc100', 'rl101', 'roadian at pro', 'roadian gtx', 'robusto', 'royal a/t', 
    'royal comfort', 'royal mile', 'royal mile xl', 'royal performance', 'rp18', 'rp203', 'rs zero', 'rs-one', 'rs21', 'ru025', 
    'ru025 ht', 'ru025y', 'ru101', 'ru101 expedite', 's526', 'sa302', 'sa37', 'scorpion', 'scorpion atr xl', 'scorpion ks', 
    'scorpion seal inside', 'scorpion str', 'sentiva ar360', 'sf600', 'sf688', 'sl106', 'sl106 (ec)', 'smacher', 'sp fm800', 
    'sp sport', 'sp touring', 'sp touring r1', 'sp026', 'sp320', 'sp801', 'sp835', 'speedline e1', 'spm305', 'sport 2 direction', 
    'sport direction', 'sportcat csc302 a/t', 'sportmacro ra301', 'steel ags', 'su009 a/t', 'su025', 'su025 rangetour plus', 
    'super 2000', 't005', 'te301', 'te307', 'touring direction', 'touring r1', 'tp-16', 'trail life a/t', 'trail terrain', 
    'turanza t005', 'ultima royal', 'ultimaplus', 'ultimapro up1', 'ultimato pro up1', 'ultimato up1', 'vancontact ap', 'vanmax', 
    'varenna s01', 'vectra', 'ventus v12 evo2', 'versant a/t', 'vf26', 'vi386 hp', 'vigorous at601', 'vigorous ht601', 'vitality f22', 
    'wdl0', 'wildpeak a/t', 'wildwolf w01', 'wr9001 at', 'wr9086a ht', 'wr9096', 'wrangler', 'wrangler fortitude h/t', 
    'wrangler fortitude ht', 'wrangler rt/s', 'wrangler territory', 'wrangler territory ht', 'wrangler workhorse at', 'xforza', 
    'xl tl primacy 4 mi', 'xlt a/s', 'xlt a/s 2', 'xport-66', 'xprivilo ht', 'xsport66', 'yda266', 'yda286 at', 'z-108', 'z108', 
    'zealion', 'ziex ze914', 'zupereco z108'
]

CONFIG_NORM: Dict[str, Dict | List] = {
    "known_brands": DEFAULT_KNOWN_BRANDS.copy(),
    "brand_aliases": { "kelly": "goodyear" },   
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

    if exp and f" {exp} " in f" {t} ":
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
        exp = _canon_model(expected)
        if exp and exp in t:
            return exp

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


def _product_to_raw(p: Product) -> dict:
    return {
        "url": p.link,
        "title": p.titulo,
        "price": p.preco,
        "currency": "BRL",
        "brand": p.brand or p.marca,
        "model": p.model or p.marca_filho,
        "availability": "in_stock", 
        "seller": p.vendedor or None,
        "extra_text": p.titulo,
    }

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

    def _configurar_driver(self, navegador: str = "chrome", headless: bool = True, user_agent: str | None = None):
        if not user_agent:
            user_agent = random.choice(USER_AGENTS)

        if navegador.lower() == "chrome":
            options = ChromeOptions()
            if headless:
                options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option("useAutomationExtension", False)
            options.add_argument("--incognito")
            options.add_argument("--window-size=1366,768")
            if user_agent:
                options.add_argument(f"--user-agent={user_agent}")

            options.page_load_strategy = "eager"

            service = ChromeService(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=options)

            try:
                self.driver.execute_cdp_cmd(
                    "Page.addScriptToEvaluateOnNewDocument",
                    {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"},
                )
            except Exception:
                pass

            try:
                self.driver.set_page_load_timeout(25)
                self.driver.implicitly_wait(3)
            except Exception:
                pass

            return self.driver

        elif navegador.lower() == "firefox":
            options = FirefoxOptions()
            if headless:
                options.add_argument("-headless")

            options.set_preference("intl.accept_languages", "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7")
            options.set_preference("dom.webdriver.enabled", False)
            options.set_preference("dom.webnotifications.enabled", False)
            if user_agent:
                options.set_preference("general.useragent.override", user_agent)

            options.set_preference("browser.tabs.remote.autostart", True)

            service = FirefoxService(GeckoDriverManager().install())
            self.driver = webdriver.Firefox(service=service, options=options)

            try:
                self.driver.set_window_size(1366, 768)
                self.driver.set_page_load_timeout(25)
                self.driver.implicitly_wait(3)
            except Exception:
                pass

            return self.driver

        else:
            raise ValueError(f"Navegador '{navegador}' não suportado")

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
    
    def _should_retry_exception(self, e: Exception) -> bool:
        msg = (str(e) or "").lower()
        rede = (
            "connectionrefusederror",
            "failed to establish a new connection",
            "max retries exceeded",
            "timed out while",
            "proxy",
            "certificate verify failed",
        )
        if any(k in msg for k in rede):
            return False

        return isinstance(e, TimeoutException)


    def _pagina_parece_quebrada(self) -> bool:
        try:
            html = (self.driver.page_source or "").lower()
        except Exception:
            return True

        if not html.strip():
            return True

        ignorar_ruido = (
            "registration response error message:",
            "deprecated_endpoint",
            "phone_registration_error",
        )
        for r in ignorar_ruido:
            html = html.replace(r, "")

        sinais = (
            "access denied", "forbidden", "captcha",
            "temporarily unavailable", "too many requests",
            "verifique que você é humano", "are you a robot", "blocked"
        )
        acertos = [s for s in sinais if s in html]

        if "captcha" in acertos:
            return True
        return len(acertos) >= 2

    def buscar(self, termo: str, *, max_resultados: int = 100, max_paginas: int = 10,
           sort: str = "relevance", expected_brand: Optional[str] = None,
           expected_model: Optional[str] = None, strict_brand: bool = True,
           retry_tries: int = 2) -> List[Product]:
        medida, brand_q, model_q = extrair_filtros_busca(termo)
        if expected_brand:
            brand_q = expected_brand
        if expected_model:
            model_q = expected_model

        self.filtro_medida = medida or None
        self.filtro_marca  = brand_q or None
        self.filtro_modelo = model_q or None

        strict_brand = bool(self.filtro_marca)

        self.logger.info("🔍 Buscando '%s' em %s | Filtros: medida=%s | marca=%s | modelo=%s",
                        termo, self.marketplace, self.filtro_medida, self.filtro_marca, self.filtro_modelo)
        self.termo_busca_atual = termo

        produtos_final: List[Product] = []
        vistos: Set[str] = set()
        navegadores = ["chrome"]
        navegador = "chrome"
        user_agent = random.choice(USER_AGENTS)
        self.logger.info("🌐 Primeira tentativa → %s | UA: %s", navegador, user_agent)

        max_tentativas = max(1, retry_tries)
        tentativa = 1

        while tentativa <= max_tentativas:
            produtos: List[Product] = []
            trocou_por_falha = False
            try:
                self.logger.info("🌐 Tentativa %d → %s | UA: %s", tentativa, navegador, user_agent)
                self._configurar_driver(navegador=navegador, headless=self.headless, user_agent=user_agent)

                try:
                    w = random.choice([1280, 1366, 1440, 1536])
                    h = random.choice([720, 768, 800, 864])
                    self.driver.set_window_size(w, h)
                except Exception:
                    pass

                url_atual = self._construir_busca_url(termo, page=1, sort=sort)
                self.driver.get(url_atual)
                self._aceitar_cookies()

                if self._pagina_parece_quebrada():
                    self.logger.warning("Heurística suspeitou de bloqueio; tentando coletar mesmo assim (sem trocar driver/UA).")


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
                        self.logger.info("Sem novos produtos em duas páginas consecutivas. Encerrando paginação.")
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

                filtrados = produtos

                if self.filtro_modelo:
                    fm = _canon_model(self.filtro_modelo)
                    filtrados = [p for p in filtrados if (p.model and fm in p.model) or (fm in _norm_text(p.titulo))]

                if self.filtro_medida:
                    alvo = self.filtro_medida.lower()
                    filtrados = [p for p in filtrados if (
                        (p.medida and p.medida.lower() == alvo) or
                        (p.size and p.size.replace("/", "-").lower() == alvo)
                    )]

                if self.filtro_marca:
                    fb = _canon_brand(self.filtro_marca)
                    if strict_brand:
                        filtrados = [p for p in filtrados if (p.brand and _canon_brand(p.brand) == fb)]
                    else:
                        prefer = [p for p in filtrados if (p.brand and _canon_brand(p.brand) == fb)]
                        outros  = [p for p in filtrados if p not in prefer]
                        filtrados = prefer + outros
                else:
                    try:
                        default_brands = getattr(self, "default_brands", None) or []
                    except Exception:
                        default_brands = []
                    if default_brands:
                        alvo = {_canon_brand(b) for b in default_brands}
                        prefer = [p for p in filtrados if p.brand and _canon_brand(p.brand) in alvo]
                        outros  = [p for p in filtrados if p not in prefer]
                        filtrados = prefer + outros

                produtos_final = filtrados[:max_resultados]

                if produtos_final:
                    return produtos_final
                else:
                    if strict_brand and self.filtro_marca:
                        self.logger.warning("Sem itens com marca estrita. Relaxando marca (mantém medida/modelo).")
                        fb = _canon_brand(self.filtro_marca)
                        base = produtos
                        if self.filtro_modelo:
                            fm = _canon_model(self.filtro_modelo)
                            base = [p for p in base if (p.model and fm in p.model) or (fm in _norm_text(p.titulo))]
                        if self.filtro_medida:
                            alvo = self.filtro_medida.lower()
                            base = [p for p in base if (
                                (p.medida and p.medida.lower() == alvo) or
                                (p.size and p.size.replace("/", "-").lower() == alvo)
                            )]
                        prefer = [p for p in base if (p.brand and _canon_brand(p.brand) == fb)]
                        outros  = [p for p in base if p not in prefer]
                        produtos_final = (prefer + outros)[:max_resultados]
                        if produtos_final:
                            return produtos_final

                    self.logger.info("Tentativa não retornou itens. Não haverá retry por UA/driver.")
                    return produtos_final

            except Exception as e:
                if self._should_retry_exception(e):
                    self.logger.warning(f"Falha técnica na tentativa {tentativa}: {e}. Trocando driver/UA…")
                    trocou_por_falha = True
                else:
                    self.logger.error(f"Erro na tentativa {tentativa}: {e}", exc_info=True)
                    return produtos_final
            finally:
                try:
                    if self.driver:
                        self.driver.quit()
                except Exception:
                    pass
            
            if trocou_por_falha:
                tentativa += 1
                navegador = random.choice(navegadores)
                user_agent = random.choice(USER_AGENTS)
                time.sleep(0.8 + random.random() * 0.8)
            else:
                break

        return produtos_final

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

    def _encontrar_elemento_com_fallback(self, parent, selectors: List[str], required: bool = True) -> Optional[Any]:
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
                time.sleep(self._delay_aleatorio(0.2, 0.5))

                link_el = self._encontrar_elemento_com_fallback(card, self.link_selectors)
                if not link_el:
                    continue
                link = link_el.get_attribute("href")
                if not link or link in links_vistos:
                    continue

                title_el = self._encontrar_elemento_com_fallback(card, self.title_selectors)
                if not title_el:
                    continue
                titulo = (title_el.text or "").strip()
                if not titulo:
                    continue

                if eh_kit_ou_multiplos_pneus(titulo):
                    continue
                
                if card.find_elements(By.CSS_SELECTOR, ".out-of-stock,.soldout,.esgotado,[data-stock='0']"):
                    continue

                size_canon = _size_canonical(titulo) or ""
                medida_path = _extrair_medida_path(titulo) or ""

                brand_dom = None
                try:
                    el_brand = self._encontrar_elemento_com_fallback(card, self.brand_selectors, required=False)
                    if el_brand:
                        brand_dom = (el_brand.text or "").strip()
                        if not brand_dom:
                            brand_dom = el_brand.get_attribute("innerText") or ""
                except Exception:
                    pass

                model_dom = None
                try:
                    el_line = self._encontrar_elemento_com_fallback(card, self.line_selectors, required=False)
                    if el_line:
                        model_dom = (el_line.text or "").strip()
                        if not model_dom:
                            model_dom = el_line.get_attribute("innerText") or ""
                except Exception:
                    pass

                brand = _canon_brand(brand_dom or _brand_from_title(titulo))  # [NOVO] prioriza DOM
                model = _canon_model(model_dom or _model_from_title(titulo, brand=brand))

                if self.filtro_marca and _canon_brand(brand) != _canon_brand(self.filtro_marca):  # [AJUSTE]
                    self.logger.debug("descartado: marca '%s' != filtro '%s' (titulo: %s)", brand, self.filtro_marca, titulo)
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
                    brand=brand,
                    model=model,
                    size=size_canon,
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

            try:
                titulo_det = (self.driver.find_element(By.CSS_SELECTOR, "h1").text or "").strip()
                if titulo_det:
                    product.titulo = titulo_det
                    if not product.size:
                        product.size = _size_canonical(titulo_det) or product.size
                    if not product.medida:
                        product.medida = _extrair_medida_path(titulo_det) or product.medida
                    if not product.brand:
                        detected_brand = _brand_from_title(titulo_det)
                        if detected_brand:
                            product.brand = detected_brand
                            product.marca = product.brand
                    if not product.model:
                        detected_model = _model_from_title(titulo_det, brand=product.brand)
                        if detected_model:
                            product.model = detected_model
                            product.marca_filho = product.model.title()
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
    parser.add_argument("--formatos", nargs="+", choices=["json","csv","sqlite"], default=["csv"], help="Formatos de saída")
    parser.add_argument("--config", help="JSON com known_brands/brand_aliases/known_model_phrases/model_aliases")
    parser.add_argument("--debug", action="store_true", help="Ativa logs de depuração")
    parser.add_argument("--run-id", required=False, default=None)
    parser.add_argument("--out-jsonl", required=False, default=None)
    parser.add_argument("--idx-from", type=int, default=0)
    parser.add_argument("--idx-to", type=int, default=None)
    parser.add_argument("--retry-tries", type=int, default=2, help="Número de tentativas em falhas técnicas (troca driver/UA apenas se cair/bloquear).")

    args = parser.parse_args()
    _load_config_norm(args.config)

    if args.lote_json:
        with open(args.lote_json, "r", encoding="utf-8") as f:
            queries = json.load(f)

        i0 = max(0, int(args.idx_from or 0))
        i1 = int(args.idx_to) if args.idx_to is not None else len(queries)
        subset = queries[i0:i1]
        print(f"[Lote] slice {i0}:{i1} -> {len(subset)} itens")

        run_id = args.run_id or datetime.utcnow().strftime("%Y%m%d_%H%M%S") + "_pneustore"
        out_jsonl = args.out_jsonl or str(Path(args.output_dir) / "jsonl" / "pneustore" / f"{run_id}.jsonl")
        batch_docs = []

        scraper = ScraperPneuStore(headless=not args.window)
        if args.debug:
            scraper.logger.setLevel(logging.INFO)

        total_itens = 0
        for pos, item in enumerate(subset, start=1):
            termo = (
                item.get("query_flex")
                or item.get("query_strict")
                or item.get("termo")
                or f"pneu {item.get('width')}/{item.get('aspect')}R{item.get('rim')} {item.get('brand')} {item.get('line_model')}"
            )

            print(f"\n=== {pos}/{len(subset)}: {termo} ===")

            produtos = scraper.buscar(
                termo,
                max_resultados=args.max,
                sort=args.sort,
                expected_brand=item.get("brand"),
                expected_model=item.get("line_model"),
                retry_tries=args.retry_tries,
            )
            caminhos = salvar_produtos_multiformato(produtos, termo, args.output_dir, args.formatos)
            if not caminhos:
                print("⚠️ Nenhum produto encontrado, nada salvo.")
            else:
                for formato, caminho in caminhos.items():
                    print(f"✅ {len(produtos)} produtos salvos em {caminho}")

            for p in produtos:
                raw = _product_to_raw(p)
                doc = to_canonical(raw, "pneustore", item.get("cod_prod", "") or "", run_id)
                ok, msg = validate_or_warn(doc)
                if not ok and args.debug:
                    print("[WARN]", msg, doc.get("url"))
                batch_docs.append(doc)

            total_itens += len(produtos)

        print(f"\nTotal coletado no lote: {total_itens} itens")
        if batch_docs:
            write_jsonl(out_jsonl, batch_docs)
            print("[OUT_JSONL]", out_jsonl, "itens:", len(batch_docs))
        exit(0)

    if not args.termo:
        print("Você deve passar --termo ou --lote-json.")
        exit(1)

    scraper = ScraperPneuStore(headless=not args.window)
    produtos = scraper.buscar(args.termo, max_resultados=args.max, sort=args.sort, retry_tries=args.retry_tries)
    caminhos = salvar_produtos_multiformato(produtos, args.termo, args.output_dir, args.formatos)
    if not caminhos:
        print("⚠️ Nenhum produto encontrado, nada salvo.")
    else:
        for formato, caminho in caminhos.items():
            print(f"✅ {len(produtos)} produtos salvos em {caminho}")

    if args.out_jsonl:
        run_id = args.run_id or datetime.utcnow().strftime("%Y%m%d_%H%M%S") + "_pneustore"
        docs = []
        for p in produtos:
            raw = _product_to_raw(p)
            doc = to_canonical(raw, "pneustore", "", run_id)
            ok, msg = validate_or_warn(doc)
            if not ok and args.debug:
                print("[WARN]", msg, doc.get("url"))
            docs.append(doc)
        if docs:
            write_jsonl(args.out_jsonl, docs)
            print("[OUT_JSONL]", args.out_jsonl, "itens:", len(docs))

