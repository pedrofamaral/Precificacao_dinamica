from __future__ import annotations
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from Scraper_em_geral._common.canon import to_canonical
from Scraper_em_geral._common.io_utils import write_jsonl
from Scraper_em_geral._common.validate import validate_or_warn
import argparse
import csv
import json
import logging
import os
import random
import re
import sys
import time
import requests
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import List, Set, Optional, Dict
from decimal import Decimal

from urllib.parse import urlencode, quote_plus
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver import ActionChains
from selenium.common.exceptions import MoveTargetOutOfBoundsException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import (
    NoSuchElementException, TimeoutException, ElementClickInterceptedException,
    StaleElementReferenceException, WebDriverException
)
from bs4 import BeautifulSoup
from dataclasses import dataclass, asdict, field

# =========================
# Helpers do seu projeto (mantidos)
# =========================
try:
    from utils.helpers import (
        extrair_medida,              
        construir_dim_pattern,       
        detectar_marca,              
        eh_kit_ou_multiplos_pneus,
        slugify,
        _parse_valor,
        _delay_between_cards,
    )
except ImportError:
    from utils.helpers import (
        extrair_medida,
        construir_dim_pattern,
        detectar_marca,
        eh_kit_ou_multiplos_pneus,
        slugify,
        _parse_valor,
        _delay_between_cards,
    )

# =========================
# Normalização (brand/model/size) — novo
# =========================

DEFAULT_KNOWN_BRANDS = [
    "goodyear", "kelly", "pirelli", "continental", "michelin",
    "bridgestone", "firestone", "dunlop", "maxxis", "kumho",
    "yokohama", "hankook", "bfgoodrich", "toyo", "cooper", "falken", "nexen", "sumitomo", "formula",
    "xbri","Westlake", "Zmax", "Wanli", "chituma", "aderenza", "anteo", "aplus", "aptany", "atlander", "austone", "Bf Goodrich",
    "Bfgoodrich", "Blackarrow", "Bridgestone", "Ceat", "Chengshan", "Chituma", "Comforser", "Compasal", "ferentino"

]

DEFAULT_MODEL_PHRASES = [
    # Goodyear / Kelly
    "assurance maxlife", "assurance", "wrangler", "eagle", "efficientgrip", "eagle sport", "kelly edge",
    # Michelin
    "energy xm2", "primacy 4", "ltx force",
    # Pirelli
    "cinturato p7", "p400 evo", "p400", "formula evo",
    # Continental
    "powercontact",
    # Dunlop
    "sp touring", "sp sport", "fm800", "lm704", "enasave ec300",
    # Outros recorrentes
    "direction", "f700", "bc20", "scorpion"

    "verso"
]

SPEED_IDX_RE = re.compile(r"\b\d{2,3}[A-Z]{1,2}\b", re.I)

CONFIG_NORM: Dict[str, Dict | List] = {
    "known_brands": DEFAULT_KNOWN_BRANDS.copy(),
    "brand_aliases": { "a plus": "aplus",
                       "a-plus": "aplus",
                       "bf goodrich": "bfgoodrich",
                       "b f goodrich": "bfgoodrich"
                     }, 
    "known_model_phrases": DEFAULT_MODEL_PHRASES.copy(),
    "model_aliases": {
        "power contact": "powercontact",
        "powerontact": "powercontact",
        "cint p7": "cinturato p7",
        "scporion": "scorpion",
        "scporion ks": "scorpion",
        "assurance max life": "assurance maxlife",
        "assurance max-life": "assurance maxlife",
        "xsport": "xport",
        "xport": "xport"
    },
}

SIZE_CANON_RE = re.compile(r"(\d{3})\s*[/\-\s]?\s*(\d{2,3})\s*[rR]?\s*[-\s]?\s*(\d{2})")
DEFAULT_LOTE_PATH = Path(__file__).resolve().parents[2] / "Extracao_e_Insercao" / "data" / "query_products.json"

def _extract_speed_index(text: str) -> str:
    if not text:
        return ""
    m = SPEED_IDX_RE.search(text.upper())
    return m.group(0).upper() if m else ""

def _bootstrap_norm_from_lote(lote: list[dict]):
    brands = set(CONFIG_NORM.get("known_brands", []))
    models = set(CONFIG_NORM.get("known_model_phrases", []))

    for it in lote:
        b = _norm_text(it.get("brand", ""))
        m = _norm_text(it.get("line_model", ""))
        if b and not SPEED_IDX_RE.fullmatch(b.replace(" ", "")):  
            brands.add(b)
        if m:
            m = re.sub(r"\(\s*(?:\d{1,3}[a-z]{1,2}|[a-z])\s*\)", "", m, flags=re.I).strip()
            if m:
                models.add(_norm_text(m))

    CONFIG_NORM["known_brands"] = sorted(brands)
    CONFIG_NORM["known_model_phrases"] = sorted(models)


def _size_regex_from_norm(size_norm: str):
    if not size_norm:
        return None
    m = re.findall(r"\d{2,3}", size_norm)
    if len(m) < 3:
        return None
    w, a, r = m[0], m[1], m[-1]
    return re.compile(rf"\b{w}\D*{a}\D*[Rr]?\D*{r}\b", re.I)

def _strict_match(title: str, brand_expected: str, model_expected: str, size_norm: str) -> bool:
    t = _norm_text(title)
    be = _canon_brand(brand_expected)
    aliases = [k for k, v in CONFIG_NORM.get("brand_aliases", {}).items() if v == be]
    allowed = [x for x in [be, *aliases] if x]

    if be and not any(re.search(rf"\b{re.escape(x)}\b", t) for x in allowed):
        return False

    """for b in CONFIG_NORM.get("known_brands", []):
        if b in allowed:
            continue
        if re.search(rf"\b{re.escape(b)}\b", t):
            return False"""#Filtro para caso apareça outras marcas nao pegue

    parts = _tokens_from_model(model_expected)
    if parts and not all(re.search(rf"\b{re.escape(p)}\b", t) for p in parts):
        return False

    sr = _size_regex_from_norm(size_norm)
    if sr and not sr.search(title):
        return False

    return True

def _tokens_from_model(value: str):
    t = _norm_text(value or "")
    t = re.sub(r"\b\d{2,3}[a-z]{1,2}\b", " ", t)
    return [x for x in re.split(r"[\s\-_/]+", t) if x]


def _norm_text(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode("ascii")
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
    
    if re.fullmatch(r"\d{2,3}[a-z]{1,2}", s):
        return ""

    if s in CONFIG_NORM["brand_aliases"]:
        return CONFIG_NORM["brand_aliases"][s]
    for kb in CONFIG_NORM["known_brands"]:
        if s == kb:
            return kb
    for kb in CONFIG_NORM["known_brands"]:
        if f" {kb} " in f" {s} ":
            return kb
    for tok in s.split():
        if not re.fullmatch(r"\d{2,3}[a-z]{1,2}", tok):
            return tok
    return ""


def _brand_from_title(title: str, expected: str = "") -> str:
    t = _norm_text(title)
    exp = _canon_brand(expected)

    if exp:
        if re.search(rf"\b{re.escape(exp)}\b", t):
            return exp
        for alias, target in CONFIG_NORM.get("brand_aliases", {}).items():
            if target == exp and re.search(rf"\b{re.escape(alias)}\b", t):
                return exp

    for kb in CONFIG_NORM.get("known_brands", []):
        if re.search(rf"\b{re.escape(kb)}\b", t):
            return kb

    try:
        det = detectar_marca(title)
        c = _canon_brand(det)
        if c:
            return c
    except Exception:
        pass
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
        parts = [p for p in re.split(r"[\s\-_/]+", _norm_text(expected)) if p]
        parts = [p for p in parts if not SPEED_IDX_RE.fullmatch(p)]
        if parts and all(re.search(rf"\b{re.escape(p)}\b", t) for p in parts):
            return _canon_model(" ".join(parts))
        return ""
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

def _product_to_raw(p: Product) -> dict:
    return {
        "url": p.link,
        "title": p.titulo,
        "price": p.preco,
        "promo_price": p.preco_desconto,
        "currency": "BRL",
        "brand": p.brand or p.marca,
        "model": p.model or p.line_expected,
        "availability": "unknown",
        "seller": p.vendedor or None,
        "extra_text": p.titulo,
        "speed_index": p.speed_index
    }

# =========================
# Logger
# =========================
def _setup_logger(debug: bool = False) -> logging.Logger:
    os.makedirs("logs", exist_ok=True)
    logger = logging.getLogger("mlscraper")
    level = logging.DEBUG if debug else logging.INFO
    logger.setLevel(level)
    if logger.handlers:
        for h in list(logger.handlers):
            logger.removeHandler(h)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh = logging.FileHandler("logs/scraper.log", encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(level)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger

@dataclass
class Product:
    titulo: str
    link: str
    preco: float | None
    brand: str = ""
    model: str = ""
    size: str = ""        
    speed_index: str = ""
    query_strict: str = ""
    size_norm: str = ""   
    brand_expected: str = ""
    line_expected: str = ""
    size_ok: bool = True
    free_ship: bool = False
    frete: float | None = None
    local: str = ""
    vendedor: str = ""
    condicao: str = "Novo"
    frete_gratis: bool = False
    marketplace: str = "mercadolivre"
    marca: str = ""       
    data_coleta: str = ""
    preco_original: float | None = None
    preco_desconto: float | None = None
    desconto_pct: float | None = None
    shipping: Dict[str, float | None] = field(default_factory=dict)

    def __post_init__(self):
        if self.free_ship and (self.frete in (None, 0.0)):
            self.frete_gratis = True
        elif self.frete_gratis:
            self.free_ship = True
        if self.brand and not self.marca:
            self.marca = self.brand

    def to_dict(self) -> dict:
        return asdict(self)


class ScraperBase:
    def __init__(self, headless: bool = True, delay_scroll: float = 1.0, logger: Optional[logging.Logger] = None):
        self.headless = headless
        self.delay_scroll = delay_scroll
        self.logger = logger or _setup_logger(False)
        self.driver = self._criar_driver(headless=self.headless)

    @staticmethod
    def _criar_driver(headless: bool = True, proxy: str | None = None):
        User_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36",
        ]
        opt = Options()
        if headless:
            opt.add_argument("--headless=new")
        opt.add_argument("--disable-blink-features=AutomationControlled")
        opt.add_argument("--lang=pt-BR")
        opt.add_argument("--window-size=1920,1080")
        opt.add_argument("--user-agent=" + random.choice(User_agents))
        opt.add_experimental_option("excludeSwitches", ["enable-automation"])
        opt.add_experimental_option("useAutomationExtension", False)
        if proxy:
            opt.add_argument(f"--proxy-server={proxy}")
        driver = webdriver.Chrome(options=opt)
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"},
        )
        return driver

    def fechar(self):
        try:
            self.driver.quit()
        except Exception:
            pass

    def _delay_aleatorio(self, a: float = 0.3, b: float = 30) -> float:
        return random.uniform(a, b)

class ScraperMercadoLivre(ScraperBase):
    base_url = "https://lista.mercadolivre.com.br"
    marketplace = "mercadolivre"
    MAX_RETRIES = 4

    CARD_SEL_CANDIDATOS = (
        "div.ui-search-result__wrapper",
        "div.ui-search-result",
        "li.ui-search-layout__item",
        "div.ui-search-results__item",
        "article.ui-search-result",
        "div.shops__layout-item",
        "li.shops__layout-item",
        "div[data-testid='result-item']",
        "div.andes-card",
        ".ui-search-result__content"
    )
    NEXT_BTN_SELS = (
        "a.andes-pagination__link--next",
        "li.andes-pagination__button--next a",
        "a[title='Seguinte']",
        "a[title='Próxima']",
        "a.ui-search-link[aria-label*='Seguinte']",
        "button.andes-pagination__button--next",
        ".andes-pagination__button--next"
    )
    TITULO_SELETORES = (
        "h2.ui-search-item__title",
        "h2.poly-component__title",
        "a.ui-search-item__group__element",
        "h3.ui-search-item__title",
        ".ui-search-item__title",
        ".poly-component__title",
        "a.ui-search-link",
        "[data-testid='item-title']",
        "h2.ui-search-item__group__element"
    )
    PRECO_INTEIRO_SELETORES = (
        ".andes-money-amount__fraction",
        ".ui-search-price__fraction",
        ".price-tag-fraction",
        ".andes-money-amount__currency-symbol + .andes-money-amount__fraction",
        "span.andes-money-amount__fraction",
        ".ui-search-price__part--medium"
    )
    PRECO_CENTS_SELETORES = (
        ".andes-money-amount__cents",
        ".ui-search-price__decimals",
        ".price-tag-cents",
        "span.andes-money-amount__cents"
    )
    PRECO_ATTR_SELETORES = (
        "[data-price]",
        "[itemprop='price']",
        "meta[itemprop='price']",
        "[data-testid='price']"
    )
    PRECO_ANTIGO_SELS = (
        ".price-tag-previous", ".ui-search-price__original-value",
        ".andes-money-amount--previous", ".price-tag-line-through",
        ".andes-money-amount__discount"
    )
    PDP_PRECO_ATUAL_SELS = (
        ".ui-pdp-price__current-price",
        ".andes-money-amount__fraction",
        "[itemprop='price']",
        "meta[itemprop='price']",
        ".andes-money-amount.ui-pdp-price__part.andes-money-amount--cents-superscript.andes-money-amount--compact"
    )
    PDP_PRECO_ANTIGO_SELS = (
        ".ui-pdp-price__original-value",
        ".andes-money-amount--previous",
        ".ui-pdp-price__second-line span.line-through",
        ".ui-pdp-price__discount-price",
        ".andes-money-amount.ui-pdp-price__part.ui-pdp-price__original-value"
        ".andes-money-amount--previous.andes-money-amount--cents-superscript"
        ".andes-money-amount--compact"
    )
    PRECO_CLASSES_ANTIGOS = ("price-tag-previous","andes-money-amount--previous","line-through","ui-search-price__original-value","ui-pdp-price__original-value")
    CARD_PRECO_BLOCOS = (
        ".ui-search-price__second-line",
        ".ui-search-price__part:first-child",
        ".ui-search-price__primary",
        ".price-tag",
        ".ui-search-result__content-wrapper .price-tag",
    )
    PDP_SHIPPING_RESULT_SELS = (
        ".ui-pdp-shipping__options", ".ui-pdp-shipping__summary", ".ui-pdp-srp__shipping",
        ".ui-pdp-shipping__item", ".ui-pdp-shipping__text"
    )
    _PRECO_REGEX = re.compile(r"(?:R\$|\$)?\s*([\d\.\,]+(?:[\,\.]\d{1,2})?)")

    def __init__(self, headless: bool = True, delay_scroll: float = 1.0, modo: str = "click", dump_html: bool = False, logger: Optional[logging.Logger] = None,
                 min_delay: float = 1.0, max_delay: float = 2.0, page_delay_min: float = 5.0, page_delay_max: float = 10.0,
                 pages_before_cooldown: int = 5, cooldown_delay: float = 60.0, **kwargs):
        super().__init__(headless=headless, delay_scroll=delay_scroll, logger=logger)

        self._query_meta: Dict = dict(kwargs.get("query_meta", {}))

        self._refresh_norms()
        self._compile_dim()

        self.modo = modo
        self.dump_html = dump_html
        self._slug_termo = ""
        self._pagina_atual = 0
        self._cookies_ok = False
        self._dim_pattern = self._dim_pattern
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.page_delay_min = page_delay_min
        self.page_delay_max = page_delay_max
        self.pages_before_cooldown = pages_before_cooldown
        self.cooldown_delay = cooldown_delay
        self.pages_scraped = 0

    def _refresh_norms(self) -> None:
        self._brand_norm = _norm_text(self._query_meta.get("brand",""))
        self._model_norm = _norm_text(self._query_meta.get("line_model",""))
        self._size_norm  = _norm_text(self._query_meta.get("size_norm",""))

    def _compile_dim(self) -> None:
        rx = self._query_meta.get("size_regex")
        if not rx:
            w = str(self._query_meta.get("width") or "").strip()
            a = str(self._query_meta.get("aspect") or "").strip()
            r = str(self._query_meta.get("rim") or "").strip()
            if w and a and r:
                rx = rf"\b{re.escape(w)}[/\s-]?{re.escape(a)}\s*R?\s*{re.escape(r)}\b"
        self._dim_pattern = re.compile(rx, re.I) if rx else None

    def set_query_meta(self, qm: Dict) -> None:
        self._query_meta = dict(qm or {})
        self._refresh_norms()
        self._compile_dim()

    def _construir_url_busca(self, termo: str, pagina: int = 1, ordenacao: str | None = None, usar_offset: bool = True, page_size: int = 48, filtros: dict | None = None) -> str:
        slug = slugify(termo)
        path = slug
        if usar_offset and pagina > 1:
            offset = (pagina - 1) * page_size + 1
            path += f"_Desde_{offset}"
        qs: dict[str, str] = {}
        if ordenacao:
            qs["sort"] = ordenacao
        if filtros:
            qs.update(filtros)
        querystring = f"?{urlencode(qs)}" if qs else ""
        return f"{self.base_url}/{quote_plus(path)}{querystring}"

    def _aceitar_cookies(self):
        if self._cookies_ok:
            return
        sels = [
            "button[data-testid='action:understood']",
            "button[data-testid='action:dismiss']",
            "button[data-testid='cookies-banner-accept']",
            "button[data-testid='cookies-banner-understood']",
            "button.cookie-consent-banner-opt-out__action--accept-all",
            ".cookie-consent-banner-opt-out__action--accept-all",
            "button.andes-button--quiet",
            "button[data-testid='cookies-dismiss-button']"
        ]
        for sel in sels:
            try:
                WebDriverWait(self.driver, 3).until(EC.element_to_be_clickable((By.CSS_SELECTOR, sel)))
                btn = self.driver.find_element(By.CSS_SELECTOR, sel)
                if btn.is_displayed():
                    btn.click()
                    time.sleep(self._delay_aleatorio())
                    self._cookies_ok = True
                    self.logger.info("cookies aceitos sel=%s", sel)
                    return
            except Exception:
                continue

    def _rolar_pagina(self):
        n_scrolls = random.randint(3, 7)
        for _ in range(n_scrolls):
            self.driver.execute_script("window.scrollBy(0, window.innerHeight * 0.8);")
            time.sleep(random.uniform(0.5, 1.2))
            self._move_mouse_randomly()

    def _move_mouse_randomly(self):
        try:
            width = self.driver.execute_script("return window.innerWidth")
            height = self.driver.execute_script("return window.innerHeight")
            x = random.randint(0, width)
            y = random.randint(0, height)
            actions = ActionChains(self.driver)
            actions.move_by_offset(x, y).pause(random.uniform(0.5, 1.5)).perform()
            actions.move_by_offset(-x, -y).perform()
        except MoveTargetOutOfBoundsException:
            pass

    def _find_cards(self):
        for sel in self.CARD_SEL_CANDIDATOS:
            try:
                els = self.driver.find_elements(By.CSS_SELECTOR, sel)
                if els:
                    valids = []
                    for el in els:
                        try:
                            if el.find_elements(By.TAG_NAME, "a") or el.find_elements(By.CSS_SELECTOR, "h1,h2,h3,.title"):
                                valids.append(el)
                        except Exception:
                            continue
                    if valids:
                        return valids, sel
            except Exception:
                continue
        return [], None

    def _esperar_cards(self, timeout: float = 15) -> tuple[int, str | None]:
        fim = time.time() + timeout
        while time.time() < fim:
            els, sel = self._find_cards()
            if els:
                self.logger.info("cards encontrados page=%s n=%s sel=%s", self._pagina_atual, len(els), sel)
                return len(els), sel
            time.sleep(0.5)
        src_len = len(self.driver.page_source or "")
        current_url = self.driver.current_url
        self.logger.warning("cards NAO encontrados page=%s html_len=%s title=%s url=%s", self._pagina_atual, src_len, self.driver.title, current_url)
        return 0, None

    def _esperar_dom_estavel(self, timeout: float = 10, estabilidade: float = 0.5) -> int:
        fim = time.time() + timeout
        ultimo_len = None
        ultimo_ts = time.time()
        while time.time() < fim:
            els, _ = self._find_cards()
            atual = len(els)
            agora = time.time()
            if atual == ultimo_len:
                if agora - ultimo_ts >= estabilidade:
                    return atual
            else:
                ultimo_len = atual
                ultimo_ts = agora
            time.sleep(0.1)
        return ultimo_len or 0

    def _snap_cards_html(self) -> tuple[list[str], int, str | None]:
        est = self._esperar_dom_estavel()
        cards, sel_usado = self._find_cards()
        htmls = []
        stale = 0
        
        for idx, el in enumerate(cards):
            try:
                html = el.get_attribute("outerHTML")
                if html:
                    htmls.append(html)
            except StaleElementReferenceException:
                stale += 1
                try:
                    el2 = self.driver.find_elements(By.CSS_SELECTOR, sel_usado)[idx]
                    html = el2.get_attribute("outerHTML")
                    if html:
                        htmls.append(html)
                except Exception:
                    pass
            except Exception:
                pass
        
        anchors = sum(1 for h in htmls if "href=" in h)
        self.logger.info("snapshot page=%s cards=%s stale=%s est=%s anchors=%s sel=%s", self._pagina_atual, len(cards), stale, est, anchors, sel_usado)
        return htmls, anchors, sel_usado

    def _first_text(self, soup, seletores):
        for sel in seletores:
            try:
                el = soup.select_one(sel)
                if el:
                    t = (el.get_text(strip=True) or "").strip()
                    if t:
                        return t
            except Exception:
                continue
        return ""

    def _extrair_precos_multi(self, soup: BeautifulSoup) -> tuple[float|None, float|None]:
        preco_antigo = None
        preco_atual  = None

        if soup.select_one("div.poly-price__current"):
            orig_frac = soup.select_one("div.poly-price__original .andes-money-amount__fraction")
            orig_cents = soup.select_one("div.poly-price__original .andes-money-amount__cents")
            
            if orig_frac:
                i = re.sub(r"\D", "", orig_frac.get_text(strip=True))
                c = re.sub(r"\D", "", orig_cents.get_text(strip=True)) if orig_cents else "00"
                preco_antigo = float( Decimal(f"{int(i)}.{int(c):02d}") )

            curr_frac = soup.select_one("div.poly-price__current .andes-money-amount__fraction")
            curr_cents = soup.select_one("div.poly-price__current .andes-money-amount__cents")
            
            if curr_frac:
                i = re.sub(r"\D", "", curr_frac.get_text(strip=True))
                c = re.sub(r"\D", "", curr_cents.get_text(strip=True)) if curr_cents else "00"
                preco_atual = float( Decimal(f"{int(i)}.{int(c):02d}") )

            return preco_antigo, preco_atual

        script = soup.find("script", {"type": "application/ld+json"})
        if script and script.string:
            try:
                data   = json.loads(script.string)
                offers = data.get("offers", {}) or {}
                if "price" in offers:
                    preco_atual = float(offers["price"])
                for comp in offers.get("priceSpecification", {}) \
                                .get("priceComponent", []) or []:
                    nome = comp.get("name", "").lower()
                    if "anterior" in nome:
                        preco_antigo = float(comp.get("price"))
                if preco_atual is not None:
                    if preco_antigo is not None and preco_atual >= preco_antigo:
                        preco_antigo = None
                    return preco_antigo, preco_atual
            except Exception:
                pass

        m = soup.select_one("meta[itemprop='price'], meta[property='og:price:amount']")
        if m and m.has_attr("content"):
            try:
                preco_atual = float(m["content"])
                return preco_antigo, preco_atual
            except Exception:
                pass

        for sel in ("[data-price]", "[itemprop='price']"):
            el = soup.select_one(sel)
            if el:
                raw = (
                    el.get("data-price")
                    or el.get("content")
                    or el.get("value")
                    or el.get_text(" ", strip=True)
                )
                v = _parse_valor(raw)
                if v is not None:
                    preco_atual = v
                    break

        for sel in (*self.PRECO_ANTIGO_SELS, "s"):
            for el in soup.select(sel):
                v = _parse_valor(el.get_text(" ", strip=True))
                if v is not None and (preco_antigo is None or v > preco_antigo):
                    preco_antigo = v
            if preco_antigo is not None:
                break

        promo_css = (
            ".ui-pdp-price__second-line "
            "span.andes-money-amount.ui-pdp-price__part"
            ".andes-money-amount--cents-superscript.andes-money-amount--compact"
        )
        elems = soup.select(promo_css)
        
        if elems:
            el = elems[1] if len(elems) > 1 else elems[0]
            v = _parse_valor(el.get_text(" ", strip=True))
            if v is not None:
                preco_atual = v

        if preco_atual is None:
            for sel in self.CARD_PRECO_BLOCOS:
                for bloco in soup.select(sel):
                    if self._soup_el_is_old(bloco):
                        continue
                    v = self._soup_bloco_money(bloco)
                    if v is not None:
                        preco_atual = v
                        break
                if preco_atual is not None:
                    break

        if preco_atual is None:
            inteiro_txt = self._first_text(soup, self.PRECO_INTEIRO_SELETORES)
            cents_txt   = self._first_text(soup, self.PRECO_CENTS_SELETORES)
            if inteiro_txt:
                i = re.sub(r"\D", "", inteiro_txt)
                c = re.sub(r"\D", "", cents_txt) if cents_txt else "00"
                try:
                    preco_atual = float(Decimal(f"{int(i)}.{int(c):02d}"))
                except Exception:
                    pass

        if preco_atual is None:
            raw = soup.get_text(" ", strip=True)
            m = self._PRECO_REGEX.search(raw)
            if m:
                preco_atual = _parse_valor(m.group(1))

        if preco_antigo is not None and preco_atual is not None and preco_atual >= preco_antigo:
            preco_antigo = None

        return preco_antigo, preco_atual

    def _soup_el_is_old(self, el) -> bool:
        classes = " ".join(el.get("class", [])).lower()
        
        for c in self.PRECO_CLASSES_ANTIGOS:
            if c in classes:
                return True
        
        if el.name == "s":
            return True
        
        if el.find_parent("s"):
            return True
        
        for c in self.PRECO_CLASSES_ANTIGOS:
            if el.find_parent(class_=lambda x: x and c in x):
                return True
        return False

    def _soup_bloco_money(self, bloco) -> float | None:
        frac = bloco.select_one(".andes-money-amount__fraction, .price-tag-fraction")
        cents = bloco.select_one(".andes-money-amount__cents, .price-tag-cents")
        
        if frac:
            ftxt = re.sub(r"\D", "", frac.get_text(strip=True))
            ctxt = re.sub(r"\D", "", cents.get_text(strip=True)) if cents else "00"
            if ftxt:
                try:
                    return float(Decimal(f"{int(ftxt)}.{int(ctxt or 0):02d}"))
                except Exception:
                    pass
        txt = bloco.get_text(" ", strip=True)
        return _parse_valor(txt)

    def _parse_card_html(self, card_html: str) -> Optional[Product]:
        try:
            soup = BeautifulSoup(card_html, "lxml")
            link_el = soup.select_one("a.ui-search-link, a.ui-search-item__group__element, a[href*='produto'], a[href*='item']")
            if not link_el:
                link_el = soup.select_one("a[href]")
            
            if not link_el:
                self.logger.debug("card sem anchor page=%s", self._pagina_atual)
                return None
            link = (link_el.get("href") or "").split("#")[0]
            
            if not link or not ("mercadolivre" in link or link.startswith("/")):
                self.logger.debug("card anchor sem href válido page=%s link=%s", self._pagina_atual, link)
                return None
            
            if link.startswith("/"):
                link = f"https://www.mercadolivre.com.br{link}"
            titulo = self._first_text(soup, self.TITULO_SELETORES)
            
            if not titulo:
                titulo = link_el.get_text(strip=True) if link_el else ""
            
            if not titulo:
                titulo = soup.get_text(" ", strip=True)[:100]
            
            speed_idx = _extract_speed_index(titulo)

            preco_orig, preco_atual = self._extrair_precos_multi(soup)
            preco = preco_atual
            free_ship, frete = self._extrair_frete_soup(soup)

            brand_exp = self._query_meta.get("brand", "")
            model_exp = self._query_meta.get("line_model", "")
            size_exp  = self._query_meta.get("size_norm", "")

            brand = _brand_from_title(titulo, expected=brand_exp)
            model = _model_from_title(titulo, brand=brand, expected=model_exp)
            size  = _size_canonical(size_exp or titulo)

            prod = Product(
                titulo=titulo,
                link=link,
                preco=preco,
                free_ship=free_ship,
                frete=frete,
                marketplace=self.marketplace,
                data_coleta=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                speed_index=speed_idx,
                preco_original=preco_orig,
                preco_desconto=preco_atual,
                desconto_pct=self._calc_desconto_pct(preco_orig, preco_atual),
                query_strict=self._query_meta.get("query_strict",""),
                size_norm=size_exp or extrair_medida(titulo) or "",
                brand_expected=_canon_brand(brand_exp),
                line_expected=_canon_model(model_exp),
                brand=brand,
                model=model,
                size=size,
                marca=brand,
            )
            try:
                if self._dim_pattern:
                    prod.size_ok = bool(self._dim_pattern.search(titulo or ""))
            except Exception:
                prod.size_ok = True

            self.logger.debug("card_price titulo=%s orig=%s atual=%s brand=%s model=%s size=%s",
                              titulo[:60], preco_orig, preco_atual, brand, model, size)
            return prod
        except Exception as e:
            self.logger.debug("parse_card_html_fail page=%s err=%s", self._pagina_atual, e)
            return None

    def _extrair_frete_soup(self, soup) -> tuple[bool, float | None]:
        try:
            txt = soup.get_text(" ", strip=True).lower()
            if "frete grátis" in txt or "frete gratis" in txt or "envio grátis" in txt or "envio gratis" in txt:
                return True, 0.0
            m = re.search(r"(?:frete|envio).*?([\d\.,]+)", txt)
            if m:
                v = _parse_valor(m.group(1))
                if v is not None:
                    return False, v
        except Exception:
            pass
        return False, None

    def _calc_desconto_pct(self, orig: float | None, desc: float | None) -> float | None:
        if orig is None or desc is None or orig <= 0:
            return None
        return round((orig - desc) / orig * 100, 2)

    def _filtrar_produto(self, prod: Product) -> tuple[Optional[Product], str]:
        if not prod:
            return None, "parse_fail"

        titulo_original = prod.titulo or ""
        titulo_norm = _norm_text(titulo_original) 

        UNWANTED_RE = re.compile(
            r"\b("
            r"remold(e|ado|agem)?|"          
            r"recapad[oa]|"                  
            r"recauchutad[oa]|"              
            r"reformad[oa]|"                 
            r"semi[-\s]?novo|seminovo|"      
            r"usad[oa]|"                     
            r"recondicionad[oa]|"            
            r"ecologic[oa]"                  
            r")\b"
        )
        if UNWANTED_RE.search(titulo_norm):
            self.logger.warning("REJEIÇÃO DE TERMO INDESEJADO: Título='%s'", titulo_original[:80])
            return None, "unwanted_keyword"

        query_strict_setting = self._query_meta.get("query_strict")
        ativar_modo_rigoroso = False
        
        if not query_strict_setting:
            ativar_modo_rigoroso = True
        elif isinstance(query_strict_setting, str) and len(query_strict_setting.split()) > 2:
            ativar_modo_rigoroso = True

        if self._dim_pattern and not self._dim_pattern.search(titulo_original):
            self.logger.warning("REJEIÇÃO DE DIMENSÃO: Título='%s', Padrão='%s'", titulo_original[:80], self._dim_pattern.pattern)
            return None, "sem_dim"

        if eh_kit_ou_multiplos_pneus(titulo_original):
            self.logger.warning("REJEIÇÃO DE KIT/MULTIPLOS: Título='%s'", titulo_original[:80])
            return None, "kit"

        def _token_present(text_norm: str, token_norm: str) -> bool:
            if not token_norm:
                return False
            return re.search(rf"(?<![a-z0-9]){re.escape(token_norm)}(?![a-z0-9])", text_norm) is not None

        def _token_present_any(text_norm: str, raw: str) -> bool:
            base = _norm_text(raw)
            if not base:
                return False
            cands = {base, base.replace(" ", ""), base.replace("-", ""), base.replace("/", "")}
            return any(_token_present(text_norm, c) for c in cands)

        marca_desejada = self._brand_norm
        marca_prod = _brand_from_title(titulo_original, expected=marca_desejada)

        if marca_desejada:
            if not marca_prod:
                if not _token_present(titulo_norm, marca_desejada):
                    self.logger.warning("REJEIÇÃO DE MARCA (fallback): Esperado='%s', Título='%s...'", marca_desejada, titulo_original[:80])
                    return None, "marca_diff"
                marca_prod = marca_desejada
            elif _norm_text(marca_prod) != marca_desejada:
                self.logger.warning("REJEIÇÃO DE MARCA: Esperado='%s', Encontrado='%s', Título='%s...'", marca_desejada, marca_prod, titulo_original[:80])
                return None, "marca_diff"

        prod.marca = marca_prod or marca_desejada or ""

        modelo_desejado = getattr(self, "_model_norm", _norm_text(self._query_meta.get("line_model","")))

        if ativar_modo_rigoroso and modelo_desejado:
            if not _token_present_any(titulo_norm, modelo_desejado):
                self.logger.warning("REJEIÇÃO DE MODELO: Esperado='%s', Título='%s...'", modelo_desejado, titulo_original[:80])
                return None, "model_diff"

        if ativar_modo_rigoroso:
            if not _strict_match(
                title=titulo_original,
                brand_expected=self._query_meta.get("brand", ""),
                model_expected=self._query_meta.get("line_model", ""),
                size_norm=self._query_meta.get("size_norm", "") or prod.size_norm
            ):
                self.logger.warning("REJEIÇÃO DE STRICT MATCH: Título='%s'", titulo_original[:80])
                return None, "not_strict"

        prod.frete_gratis = prod.free_ship and (prod.frete in (None, 0.0))
        return prod, "ok"


    def _delay_after_page(self):
        d = random.uniform(self.page_delay_min, self.page_delay_max)
        self.logger.info(f"Delaying {d:.2f}s after finishing page")
        time.sleep(d)
        self.pages_scraped += 1
        
        if self.pages_scraped % self.pages_before_cooldown == 0:
            self.logger.warning(f"Cooldown de {self.cooldown_delay}s após {self.pages_before_cooldown} páginas")
            time.sleep(self.cooldown_delay)

    def _ir_proxima_pagina(self) -> bool:
        for sel in self.NEXT_BTN_SELS:
            try:
                btns = self.driver.find_elements(By.CSS_SELECTOR, sel)
                if not btns:
                    continue
                btn = btns[0]
                if not (btn.is_displayed() and btn.is_enabled()):
                    continue
                self.driver.execute_script("arguments[0].scrollIntoView(true);", btn)
                time.sleep(self._delay_aleatorio(0.3, 0.6))
                url = btn.get_attribute("href")
                try:
                    head = requests.head(url, allow_redirects=True, timeout=5)
                    if head.status_code != 200:
                        self.logger.warning("Próxima página retornou %s, pulando", head.status_code)
                        continue
                except Exception:
                    pass
                btn.click()
                self.logger.info("next_click page=%s sel=%s", self._pagina_atual, sel)
                WebDriverWait(self.driver, 10).until(EC.staleness_of(btn))
                self._delay_after_page()
                return True
            except (ElementClickInterceptedException, StaleElementReferenceException, TimeoutException):
                continue
        backoff = random.uniform(10, 20)
        self.logger.error("Falha ao mudar de página, backoff %.2f s", backoff)
        time.sleep(backoff)
        return False

    def buscar_produtos(self, termo: str, max_resultados: int = 100, ordenacao: str | None = None, max_paginas: int | None = None,
                        filtros: dict | None = None, page_size: int = 48, ceps: Optional[List[str]] = None,
                        enriquecer: bool = False, size_regex_override: Optional[str] = None, query_meta: Optional[dict] = None) -> List[Product]:
        vistos: Set[str] = set()
        resultados: List[Product] = []
        pagina = 1
        self._pagina_atual = 1
        self._slug_termo = slugify(termo)

        self.set_query_meta(query_meta)

        if size_regex_override:
            try:
                self._dim_pattern = re.compile(size_regex_override, flags=re.I)
            except re.error:
                self._dim_pattern = construir_dim_pattern(termo)
        else:
            self._dim_pattern = construir_dim_pattern(termo)

        usar_offset = (self.modo == "offset")
        url = self._construir_url_busca(termo, pagina=1, ordenacao=ordenacao, usar_offset=usar_offset, page_size=page_size, filtros=filtros)
        self.logger.info("URL inicial p1: %s", url)
        self.driver.get(url)
        self._aceitar_cookies()

        while True:
            try:
                n_cards, sel_usado = self._esperar_cards()
                if n_cards == 0:
                    self.logger.warning("Nenhum card encontrado na página %s", pagina)
                    break
                time.sleep(self._delay_aleatorio(1, 2))
                self._rolar_pagina()

                html_cards, anchors, _ = self._snap_cards_html()
                cont_parse_fail = cont_sem_dim = cont_kit = cont_marca = cont_dup = 0
                mantidos = 0
                for card_html in html_cards:
                    try:
                        p = self._parse_card_html(card_html)
                        p, motivo = self._filtrar_produto(p)
                        if not p:
                            if   motivo == "sem_dim": cont_sem_dim += 1
                            elif motivo == "kit":     cont_kit += 1
                            elif motivo == "marca_diff": cont_marca += 1
                            else: cont_parse_fail += 1
                            continue
                        if p.link in vistos:
                            cont_dup += 1
                            continue
                        vistos.add(p.link)
                        resultados.append(p)
                        _delay_between_cards(self.min_delay, self.max_delay, logger=None)
                        mantidos += 1
                    except Exception:
                        cont_parse_fail += 1

                self.logger.info("Página %s novos=%s acumulado=%s parse=%s sem_dim=%s kit=%s marca=%s dup=%s",
                                pagina, mantidos, len(resultados), cont_parse_fail, cont_sem_dim, cont_kit, cont_marca, cont_dup)

                if len(resultados) >= max_resultados:
                    break
                if max_paginas and pagina >= max_paginas:
                    break
                pagina += 1
                self._pagina_atual = pagina

                if usar_offset:
                    next_url = self._construir_url_busca(termo, pagina=pagina, ordenacao=ordenacao, usar_offset=True, page_size=page_size, filtros=filtros)
                    self.logger.info("URL p%s: %s", pagina, next_url)
                    self.driver.get(next_url)
                else:
                    if not self._ir_proxima_pagina():
                        break
                time.sleep(self._delay_aleatorio(0.5, 1.5))
            except KeyboardInterrupt:
                print("\n\n[AVISO] Interrupção detectada!")
                resposta = input("Deseja realmente parar o scraper? (s/n): ").lower().strip()
                if resposta == 's':
                    self.logger.warning("Execução interrompida pelo usuário.")
                    print("Parando o scraper...")
                    break  
                else:
                    print("Continuando a raspagem...")
                    continue 

        if ceps:
            enriquecer = True
        if enriquecer and resultados:
            self._enriquecer_produtos(resultados, [re.sub(r"\D","",c) for c in (ceps or []) if c.strip()])
        return resultados[:max_resultados]

    def _enriquecer_produtos(self, produtos: List[Product], ceps: List[str]):
        self.logger.info("iniciando enriquecimento %s produtos ceps=%s", len(produtos), ceps)
        ceps_norm = [re.sub(r"\D", "", c) for c in ceps if c.strip()]
        for i, p in enumerate(produtos, 1):
            try:
                self.logger.info("detalhes %s/%s abrindo %s", i, len(produtos), p.link)
                self.driver.get(p.link)
                WebDriverWait(self.driver, 15).until(lambda d: d.execute_script("return document.readyState") == "complete")
                time.sleep(self._delay_aleatorio(1,2))
                html = self.driver.page_source
                soup = BeautifulSoup(html, "lxml")
                preco_atual = self._extrair_preco_pdp(soup)
                preco_antigo = self._extrair_preco_pdp_antigo(soup)
                if preco_atual is not None:
                    p.preco_desconto = preco_atual
                    if p.preco is None or p.preco_desconto < (p.preco or 1e18):
                        p.preco = p.preco_desconto
                if preco_antigo is not None:
                    p.preco_original = preco_antigo
                p.desconto_pct = self._calc_desconto_pct(p.preco_original, p.preco_desconto)
                if ceps_norm:
                    ship_map = {}
                    for cep in ceps_norm:
                        v = self._calcular_frete_cep(cep)
                        ship_map[cep] = v
                        time.sleep(self._delay_aleatorio(0.5,1.2))
                    p.shipping = ship_map
            except Exception as e:
                self.logger.warning("detalhes_fail %s err=%s", p.link, e)

    def _extrair_preco_pdp(self, soup) -> float | None:
        for sel in self.PDP_PRECO_ATUAL_SELS:
            el = soup.select_one(sel)
            if not el:
                continue
            v = _parse_valor(el.get("content") or el.get_text(" ", strip=True))
            if v is not None:
                return v
        raw = soup.get_text(" ", strip=True)
        m = self._PRECO_REGEX.search(raw)
        if m:
            return _parse_valor(m.group(1))
        return None

    def _extrair_preco_pdp_antigo(self, soup) -> float | None:
        for sel in self.PDP_PRECO_ANTIGO_SELS:
            el = soup.select_one(sel)
            if not el:
                continue
            v = _parse_valor(el.get("content") or el.get_text(" ", strip=True))
            if v is not None:
                return v
        return None

    def _calcular_frete_cep(self, cep_digits: str) -> float | None:
        # (pode ser implementado no futuro: abrir modal, etc.)
        return None  

def _parse_lista_ceps(arg: Optional[str]) -> List[str]:
    if not arg:
        return []
    return [p.strip() for p in arg.split(",") if p.strip()]

def montar_query_flex(item):
    return f"pneu {item['width']} {item['aspect']} r{item['rim']} {item['brand']} {item['line_model']}"

def imprimir_produtos(produtos: List[Product]):
    print(f"\n{'='*100}")
    print(f"PRODUTOS ENCONTRADOS: {len(produtos)}")
    print(f"{'='*100}")
    for i, produto in enumerate(produtos, 1):
        preco_show = produto.preco or 0.0
        frete = "✓ Frete Grátis" if produto.frete_gratis else "✗ Frete Pago"
        t = produto.titulo if len(produto.titulo) <= 65 else produto.titulo[:65] + "..."
        print(f"{i:3}. {t:<68} | R$ {preco_show:>8.2f} | {frete}")
    print(f"{'='*100}")

def salvar_resultados(produtos: List[Product], termo: str, em_csv: bool, ceps: List[str]):
    base_dir = Path(__file__).parent / "data"
    medida = extrair_medida(termo)  
    out_dir = base_dir / "raw" / medida
    out_dir.mkdir(parents=True, exist_ok=True)
    slug = slugify(termo)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = out_dir / f"{slug}_{timestamp}.json"

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump([p.to_dict() for p in produtos], f, ensure_ascii=False, indent=2)
    print(f"JSON: {json_path}")

    if em_csv and produtos:
        cep_cols = [re.sub(r'\D','',c) for c in ceps]
        header = [
                "titulo","link","preco","free_ship","frete","frete_gratis","marketplace",
                "brand","model","size","speed_index","marca","data_coleta",
                "preco_original","preco_desconto","desconto_pct"
        ]
        for c in cep_cols:
            header.append(f"shipping_{c}")
        csv_path = out_dir / f"{slug}_{timestamp}.csv"
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(header)
            for p in produtos:
                row = [
                    p.titulo, p.link, p.preco, p.free_ship, p.frete, p.frete_gratis, p.marketplace,
                    p.brand, p.model, p.size, p.speed_index, p.marca, p.data_coleta,
                    p.preco_original, p.preco_desconto, p.desconto_pct
                ]

                for c in cep_cols:
                    row.append(p.shipping.get(c))
                w.writerow(row)
        print(f"CSV:  {csv_path}")


def criar_parser():
    parser = argparse.ArgumentParser(description="Scraper MercadoLivre v2.0 (com normalização brand/model/size)")

    parser.add_argument("--modo", choices=["click", "offset"], default="click", help="Modo de paginação")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--termo", help="Termo de busca")
    group.add_argument("--lote-json", help="Caminho do JSON de queries (ex: query_products.json)")

    parser.add_argument("--max", type=int, default=100, help="Máximo de itens buscados")
    parser.add_argument("--ordenacao", default=None, help="price_asc, price_desc, relevance")

    parser.add_argument("--csv", action="store_true", help="Salvar CSV de saída")

    parser.add_argument("--headless", dest="headless", action="store_true", help="Executar sem janela")
    parser.add_argument("--window", dest="headless", action="store_false", help="Executar com janela visível")
    parser.set_defaults(headless=True)

    parser.add_argument("--dump-html", action="store_true", help="Salvar HTML bruto de cada página")
    parser.add_argument("--debug", action="store_true", help="Habilitar logging DEBUG")
    parser.add_argument("--detalhes", action="store_true", help="Abrir PDPs para coletar detalhes")
    parser.add_argument("--ceps", default=None, help="Lista de CEPs separados por vírgula")
    parser.add_argument("--run-id", required=False, default=None)
    parser.add_argument("--out-jsonl", required=False, default=None)

    parser.add_argument("--min-delay", type=float, default=1.0, help="Delay mínimo (s) entre cada card")
    parser.add_argument("--max-delay", type=float, default=2.0, help="Delay máximo (s) entre cada card")
    parser.add_argument("--page-delay-min", type=float, default=5.0, help="Delay mínimo (s) ao trocar de página")
    parser.add_argument("--page-delay-max", type=float, default=10.0, help="Delay máximo (s) ao trocar de página")
    parser.add_argument("--pages-before-cooldown", type=int, default=5, help="Páginas antes de cooldown maior")
    parser.add_argument("--cooldown-delay", type=float, default=60.0, help="Delay extra (s) após cooldown")
    parser.add_argument("--delay-scroll", type=float, default=1.0, help="Delay (s) entre scrolls individuais")

    parser.add_argument("--idx-from", type=int, default=0, help="Índice inicial (inclusive) dentro do lote")
    parser.add_argument("--idx-to", type=int, default=None, help="Índice final (exclusivo) dentro do lote")
    parser.add_argument("--config", help="JSON com known_brands/brand_aliases/known_model_phrases/model_aliases")

    return parser

def main():
    parser = criar_parser()
    args = parser.parse_args()

    logger = _setup_logger(args.debug)
    _load_config_norm(args.config)
    ceps = _parse_lista_ceps(args.ceps)

    try:
        if DEFAULT_LOTE_PATH.exists():
            with open(DEFAULT_LOTE_PATH, "r", encoding="utf-8") as f:
                lote_default = json.load(f)
            _bootstrap_norm_from_lote(lote_default)
            logger.info("norm_bootstrap: brands=%s modelos=%s", len(CONFIG_NORM["known_brands"]), len(CONFIG_NORM["known_model_phrases"]))
    except Exception:
        pass

    try:
        if args.lote_json:
            run_id = args.run_id or datetime.utcnow().strftime("%Y%m%d_%H%M%S") + "_mercadolivre"
            out_jsonl = args.out_jsonl or os.path.join(Path(__file__).parent, "data", "jsonl", f"{run_id}.jsonl")
            batch_docs = []

            with open(args.lote_json, "r", encoding="utf-8") as f:
                lote = json.load(f)

            _bootstrap_norm_from_lote(lote)
            logger.info("norm_bootstrap: brands=%s modelos=%s", len(CONFIG_NORM["known_brands"]), len(CONFIG_NORM["known_model_phrases"]))

            i0 = max(0, int(args.idx_from or 0))
            i1 = int(args.idx_to) if args.idx_to is not None else len(lote)
            subset = lote[i0:i1]

            logger.info("Executando lote %s itens (slice %s:%s) do arquivo %s", len(subset), i0, i1, args.lote_json)
            total_itens = 0

            for k, item in enumerate(subset, start=i0):
                termo = montar_query_flex(item)
                logger.info("(%s/%s) Iniciando busca para: %s", k, len(lote) - 1, termo)

                scraper = ScraperMercadoLivre(
                    modo=args.modo, headless=args.headless, dump_html=args.dump_html,
                    logger=logger, min_delay=args.min_delay, max_delay=args.max_delay,
                    page_delay_min=args.page_delay_min, page_delay_max=args.page_delay_max,
                    pages_before_cooldown=args.pages_before_cooldown,
                    cooldown_delay=args.cooldown_delay, delay_scroll=args.delay_scroll,
                )
                try:
                    meta = {
                        "brand": item.get("brand", ""), "line_model": item.get("line_model", ""),
                        "size_norm": f"{item.get('width','')}/{item.get('aspect','')}r{item.get('rim','')}",
                        "query_strict": item.get("query_strict", ""),
                    }

                    produtos = scraper.buscar_produtos(
                        termo=termo, max_resultados=args.max, ordenacao=args.ordenacao,
                        ceps=ceps, enriquecer=args.detalhes, query_meta=meta,
                    )

                    imprimir_produtos(produtos)
                    tops = sorted([p for p in produtos if p.preco is not None], key=lambda p: p.preco)[:10]
                    if tops:
                        media = sum(p.preco for p in tops) / len(tops)
                        print(f"Média dos {len(tops)} mais baratos: R$ {media:.2f}")

                    salvar_resultados(produtos=produtos, termo=termo, em_csv=args.csv, ceps=ceps)

                    for p in produtos:
                        raw = _product_to_raw(p)
                        doc = to_canonical(raw, "mercadolivre", item.get("cod_prod", "") or "", run_id)
                        ok, msg = validate_or_warn(doc)
                        if not ok and args.debug:
                            print("[WARN]", msg, doc.get("url"))
                        batch_docs.append(doc)

                    total_itens += len(produtos)

                except Exception as e:
                    logger.error("Erro CRÍTICO no item %s (%s): %s", k, termo, e, exc_info=True)
                finally:
                    scraper.fechar()
                    delay_aleatorio = random.uniform(10, 30)
                    logger.info(f"Delay de {delay_aleatorio:.2f}s antes da próxima busca do lote.")
                    time.sleep(delay_aleatorio)

            print(f"\nTotal coletado no lote: {total_itens} itens")

            if batch_docs:
                write_jsonl(out_jsonl, batch_docs)
                print("[OUT_JSONL]", out_jsonl, "itens:", len(batch_docs))

        else: 
            scraper = ScraperMercadoLivre(
                modo=args.modo, headless=args.headless, dump_html=args.dump_html,
                logger=logger, min_delay=args.min_delay, max_delay=args.max_delay,
                page_delay_min=args.page_delay_min, page_delay_max=args.page_delay_max,
                pages_before_cooldown=args.pages_before_cooldown,
                cooldown_delay=args.cooldown_delay, delay_scroll=args.delay_scroll,
            )
            try:
                logger.info("Executando busca por termo único: %s", args.termo)
                
                meta = {
                    "brand": _brand_from_title(args.termo), "line_model": "",
                    "size_norm": _size_canonical(args.termo), "query_strict": "",
                }
                
                if meta["brand"]:
                    meta["line_model"] = _model_from_title(args.termo, brand=meta["brand"])

                logger.info("Meta-dados extraídos do termo: %s", meta)
                produtos = scraper.buscar_produtos(
                    termo=args.termo, max_resultados=args.max, ordenacao=args.ordenacao,
                    ceps=ceps, enriquecer=args.detalhes, query_meta=meta,
                )

                imprimir_produtos(produtos)
                tops = sorted([p for p in produtos if p.preco is not None], key=lambda p: p.preco)[:10]
                if tops:
                    media = sum(p.preco for p in tops) / len(tops)
                    print(f"Média dos {len(tops)} mais baratos: R$ {media:.2f}")
                salvar_resultados(produtos=produtos, termo=args.termo, em_csv=args.csv, ceps=ceps)
            finally:
                scraper.fechar()

    except KeyboardInterrupt:
        print("Interrompido pelo usuário.")
    except Exception as e:
        logger.error("Erro geral ao executar scraper: %s", e, exc_info=True)
        print(f"Erro: {e}")


if __name__ == "__main__":
    main()

#Retirar o modo rigoroso para teste em lote, tentar pegar o cod_prod 30451(pneu 175 70 r13 goodyear assurance)