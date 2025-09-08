import os
import re
import time
import json
import csv
import random
import logging
import unicodedata
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Any
import argparse
import sqlite3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from Scraper_em_geral._common.canon import to_canonical
from Scraper_em_geral._common.io_utils import write_jsonl
from Scraper_em_geral._common.validate import validate_or_warn

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
    NoSuchElementException
)


# =========================
# Configs e listas
# =========================

MARCAS = [
    'aderenza', 'anlas', 'anteo', 'aplus', 'aptany', 'atlander', 'austone', 'barum', 'bf goodrich', 'bfgoodrich', 
    'blacklion', 'bkt', 'borilli', 'bridgestone', 'carlisle', 'ceat', 'chengshan', 'chituma', 'comforser', 'compasal', 
    'continental', 'cooper', 'davanti', 'dayton', 'delmax', 'dewostone', 'double king', 'doubleking', 'doublestar', 'dunlop', 
    'durable', 'dyna', 'dynamo', 'ecovision', 'falken', 'farroad', 'fate', 'federal', 'firemax', 'firestone', 'forceland', 
    'formula', 'general', 'goform', 'goodride', 'goodyear', 'gt radial', 'habilead', 'hankook', 'hifly', 'hilfy', 'horizon', 
    'infinity', 'invovic', 'ironman', 'itaro', 'jk tyre', 'jktyre', 'kenda', 'kingtyre', 'kpatos', 'kumho', 'kumho tire', 
    'landspider', 'lanvigator', 'lavigator', 'levorin', 'linglong', 'luistone', 'magnum', 'maxxis', 'mazzini', 'michelin', 
    'milever', 'minerva', 'nankang', 'nexen', 'nitto', 'nokian', 'onyx', 'otani', 'petlas', 'pirelli', 'power trac', 'primewell', 
    'radar', 'roadcruza', 'roadstone', 'routeway', 'royal black', 'sailun', 'sava', 'semperit', 'speedmax', 'sumitomo', 
    'sumitomo tire', 'sumitomo tires', 'sunfull', 'sunny', 'sunset', 'sunset tires', 'sunwide', 'tbb tires', 'toyo', 'towin', 
    'tracmax', 'trazano', 'triangle', 'valeo', 'vee rubber', 'ventus', 'versatyre', 'viemar', 'viking', 'vitour', 'wanli', 
    'westlake', 'windforce', 'winrun', 'xbri', 'yokohama', 'zmax', 'zptire'
]

MODELOS = [
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

MARCAS_REGEX = re.compile(r'\b(' + '|'.join(map(re.escape, sorted(MARCAS, key=len, reverse=True))) + r')\b', re.IGNORECASE)
MODELOS_REGEX = re.compile(r'\b(' + '|'.join(map(re.escape, sorted(MODELOS, key=len, reverse=True))) + r')\b', re.IGNORECASE)

VENDEDORES_PALAVRAS_INVALIDAS = [
    "imperador", "imperatriz", "carli", "imperiodospneuspecas"
]

CONFIG = {
    'USER_AGENTS': [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    ],
    'RETRY_ATTEMPTS': 3,
    'TIMEOUT': 30,
    'SCROLL_PAUSE_TIME': 2,
    'MAX_PRODUCTS_PER_PAGE': 50,
    'OUTPUT_FORMATS': ['json', 'csv', 'sqlite']
}

# =========================
# Utilidades
# =========================

def _magalu_to_raw(prod) -> dict:
    if hasattr(prod, "__dict__"):  
        d = prod.__dict__
    else:
        d = prod or {}
    return {
        "url": d.get("link"),
        "title": d.get("titulo"),
        "price": d.get("preco"),
        "promo_price": d.get("preco_original") if d.get("promocao") else None,
        "currency": "BRL",
        "brand": d.get("marca"),
        "model": d.get("modelo"),
        "availability": "in_stock" if d.get("disponivel", True) else "out_of_stock",
        "seller": d.get("vendedor"),
        "extra_text": d.get("titulo"),
    }


def delay_humano(min_delay=2.5, max_delay=5.5):
    delay = random.uniform(min_delay, max_delay)
    time.sleep(delay)
    return delay

def slugify(text: str) -> str:
    if not text:
        return "sem-nome"
    text = str(text).lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s/]+", "-", text)
    text = text.strip("-")
    return text[:100] if text else "produto"

def normalizar_termo(termo: str) -> str:
    termo = termo.replace("/", " ")
    termo = termo.replace("-", " ")
    termo = termo.replace("  ", " ")
    termo = termo.replace(" r ", " r")
    return termo.strip()

def extrair_medida_path(termo_ou_titulo: str) -> str:
    termo = re.sub(r"[-_/]", " ", (termo_ou_titulo or "").lower())
    m = re.search(r'(\d{3})\s*(\d{2,3})\s*r?\s*(\d{2})', termo)
    if m:
        return f"{m.group(1)}-{m.group(2)}-r{m.group(3)}"
    return slugify(termo[:30])

def normalizar_medida_valor(s: str) -> str:
    if not s: return ""
    s2 = re.sub(r"[-_/]", " ", s.lower())
    m = re.search(r'(\d{3})\s*(\d{2,3})\s*r?\s*(\d{2})', s2)
    if m:
        return f"{m.group(1)}/{m.group(2)}R{m.group(3)}".upper()
    return ""

def normalizar_str(s):
    s = unicodedata.normalize('NFD', s or "")
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return s.lower()

def _norm_soft(s: Optional[str]) -> str:
    s = normalizar_str(s or "")
    return re.sub(r"[\W_]+", "", s)  

def _contains_token(hay: str, needle: str) -> bool:
    hay_n = normalizar_str(hay or "")
    needle_n = normalizar_str(needle or "")
    return re.search(rf"\b{re.escape(needle_n)}\b", hay_n) is not None


def _extrair_marca_titulo(titulo: str) -> str:
    if not titulo: 
        return ""
    t = normalizar_str(titulo)
    match = MARCAS_REGEX.search(t)
    return match.group(1) if match else ""


def extrair_modelo_titulo(titulo: str) -> str:
    if not titulo: 
        return ""
    t = normalizar_str(titulo)
    match = MODELOS_REGEX.search(t)
    return match.group(1) if match else ""

def extrair_filtros_busca(termo: str):
    termo_low = normalizar_str(termo or "")
    medida = normalizar_medida_valor(termo_low)

    marca = None
    for m in MARCAS:
        m_norm = normalizar_str(m)
        if re.search(rf"\b{re.escape(m_norm)}\b", termo_low):
            marca = m  
            break

    modelo = None
    for mod in MODELOS:
        if normalizar_str(mod) in termo_low:
            modelo = mod
            break

    return medida, marca, modelo

def eh_kit_ou_multiplos_pneus(texto: str) -> bool:
    if not texto:
        return False
    texto_normalizado = unicodedata.normalize("NFD", texto)
    texto_limpo = "".join(c for c in texto_normalizado if unicodedata.category(c) != "Mn").lower()
    texto_sem_pontuacao = ''.join(ch if ch.isalnum() or ch.isspace() else ' ' for ch in texto_limpo)
    palavras = texto_sem_pontuacao.split()
    PALAVRAS_KIT = [
        "kit", "kits", "conjunto", "conjuntos", "par", "pares",
        "04", "duas", "dois", "quatro", "dupla", "duplas", "combo", "combos",
        "pack", "packs", "promoção", "promocao", "jogo", "oferta", "pacote", "pacotes", "lote", "lotes", "casal",
        "pneus", "unidades", "k2", "k4", "k6", "kit2"
    ]
    if any(p in palavras for p in PALAVRAS_KIT):
        return True
    padroes_kit = [
        r'\b(kit|conjunto|par|pack|combo|lote|jogo|casal)\b',
        r'\b(kit|conjunto|par|pack|combo|lote|jogo)\s*(de|com)?\s*(\d+)\s*(pneu|pneus|unidade|unidades)\b',
        r'\b(dois|duas|quatro)\s*(pneu|pneus)\b',
        r'\b(dupla|duplas)\s*(de\s*)?(pneu|pneus)\b',
        r'\b(promoção|promocao|oferta)\s*(kit|conjunto|par|kit2)\b',
        r'\b(kit|conjunto)\s*(com|de)\s*(\d+)\b',
        r'\bk\d{1,2}\b',
        r'\bkit\s*\d{1,2}\b',
        r'\bpar\s*\d{1,2}\b'
    ]
    return any(re.search(p, texto_limpo) for p in padroes_kit)

def parse_preco(preco_str: str) -> Optional[float]:
    if not preco_str:
        return None
    try:
        preco_clean = re.sub(r'[^\d,.]', '', preco_str)
        if ',' in preco_clean and '.' in preco_clean:
            preco_clean = preco_clean.replace('.', '').replace(',', '.')
        elif ',' in preco_clean:
            preco_clean = preco_clean.replace(',', '.')
        return float(preco_clean)
    except (ValueError, AttributeError):
        return None

def parse_avaliacoes(avaliacoes_str: str) -> int:
    if not avaliacoes_str:
        return 0
    numeros = re.findall(r'\d+', avaliacoes_str.replace('.', '').replace(',', ''))
    return int(numeros[0]) if numeros else 0

def parse_nota(nota_str: str) -> float:
    if not nota_str:
        return 0.0
    try:
        nota = re.search(r'(\d+[,.]?\d*)', nota_str)
        if nota:
            return float(nota.group(1).replace(',', '.'))
    except (ValueError, AttributeError):
        pass
    return 0.0

# =========================
# Modelo de dados
# =========================

@dataclass
class ProdutoMagalu:
    titulo: str
    preco: float
    link: str
    data_coleta: str

    # novos campos
    medida: str = ""
    marca: str = ""
    modelo: str = ""

    preco_original: Optional[float] = None
    promocao: bool = False
    imagem: str = ""
    marketplace: str = "magazineluiza"
    categoria: str = ""
    disponivel: bool = True
    avaliacoes: int = 0
    nota_media: float = 0.0
    vendedor: str = ""
    frete_gratis: bool = False
    parcelamento: str = ""
    descricao: str = ""
    frete: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def is_valid(self) -> bool:
        return bool(self.titulo and self.preco and self.link)

# =========================
# Banco de dados
# =========================

class DatabaseManager:
    def __init__(self, db_path: str = "data/magalu_products.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.init_database()

    def init_database(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS produtos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    titulo TEXT NOT NULL,
                    preco REAL NOT NULL,
                    link TEXT UNIQUE NOT NULL,
                    data_coleta TEXT NOT NULL,
                    preco_original REAL,
                    promocao BOOLEAN,
                    imagem TEXT,
                    marketplace TEXT,
                    categoria TEXT,
                    disponivel BOOLEAN,
                    avaliacoes INTEGER,
                    nota_media REAL,
                    vendedor TEXT,
                    frete_gratis BOOLEAN,
                    parcelamento TEXT,
                    descricao TEXT,
                    medida TEXT,
                    marca TEXT,
                    modelo TEXT
                )
            ''')
            conn.commit()

    def salvar_produtos(self, produtos: List[ProdutoMagalu]):
        with sqlite3.connect(self.db_path) as conn:
            for produto in produtos:
                conn.execute('''
                    INSERT OR REPLACE INTO produtos
                    VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    produto.titulo, produto.preco, produto.link,
                    produto.data_coleta, produto.preco_original, produto.promocao,
                    produto.imagem, produto.marketplace, produto.categoria,
                    produto.disponivel, produto.avaliacoes, produto.nota_media,
                    produto.vendedor, produto.frete_gratis, produto.parcelamento,
                    produto.descricao, produto.medida, produto.marca, produto.modelo
                ))
            conn.commit()

# =========================
# Scraper
# =========================

class ScraperMagalu:
    base_url = "https://www.magazineluiza.com.br"
    marketplace = "magazineluiza"

    def __init__(self, headless: bool = True, delay_scroll: float = 1.0,
                 termo_busca: str = None, max_workers: int = 1, output_dir: str = "data"):
        self.headless = headless
        self.delay_scroll = delay_scroll
        self.max_workers = max_workers
        self.output_dir = Path(output_dir)
        self.termo_busca = termo_busca
        self.filtro_medida, self.filtro_marca, self.filtro_modelo = extrair_filtros_busca(termo_busca) if termo_busca else (None, None, None)

        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.driver = None
        self.logger = self._setup_logger()
        self.db_manager = DatabaseManager(db_path=str(self.output_dir / "magalu_products.db"))

    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger(self.marketplace)
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            log_dir = Path("logs")
            log_dir.mkdir(parents=True, exist_ok=True)
            log_file = log_dir / "scraper.log"
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            formatter = logging.Formatter(
                '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
        return logger

    def _init_driver(self) -> webdriver.Chrome:
        options = Options()
        if self.headless:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-plugins")
        options.add_argument("--disable-images")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--start-maximized")
        user_agent = random.choice(CONFIG['USER_AGENTS'])
        options.add_argument(f"--user-agent={user_agent}")
        options.add_argument("--lang=pt-BR")
        options.add_experimental_option('prefs', {
            'intl.accept_languages': 'pt-BR,pt,en-US,en',
            'profile.managed_default_content_settings.images': 2
        })
        try:
            driver = webdriver.Chrome(options=options)
            driver.set_page_load_timeout(CONFIG['TIMEOUT'])
            return driver
        except Exception as e:
            self.logger.error(f"Erro ao inicializar driver: {e}")
            raise

    def construir_url_busca(self, termo: str, pagina: int = 1, filtros: Optional[Dict] = None) -> str:
        termo = normalizar_termo(termo)
        termo_url = termo.strip().replace(" ", "+")
        url = f"{self.base_url}/busca/{termo_url}/?page={pagina}"
        if filtros:
            params = []
            for key, value in filtros.items():
                if value:
                    params.append(f"{key}={value}")
            if params:
                url += "&" + "&".join(params)
        return url

    def scroll_page(self, max_scrolls: int = 5):
        for i in range(max_scrolls):
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(self.delay_scroll)
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if i > 0 and new_height == getattr(self, '_last_height', 0):
                break
            self._last_height = new_height
    
    def _find_in(el, css):
        return el.find_element(By.CSS_SELECTOR, css)


    def extrair_produto_detalhado(self, card) -> Optional[ProdutoMagalu]:
        try:
            try:
                titulo_el = WebDriverWait(self.driver, 6).until(
                    lambda d: card.find_element(
                        By.CSS_SELECTOR,
                        "h1[data-testid='product-title'], "
                        "h2[data-testid='product-title'], "
                        "h3[data-testid='product-title'], "
                        "[aria-label]"
                    )
                )
                titulo = (titulo_el.get_attribute("innerText")
                        or titulo_el.get_attribute("textContent")
                        or "").strip()
                if not titulo and card.tag_name.lower() == "a":
                    titulo = (card.get_attribute("aria-label") or "").strip()
            except TimeoutException:
                self.logger.error("Timeout no TÍTULO do card; ignorando produto.")
                return None

            if not titulo:
                self.logger.warning("Título vazio; ignorando produto.")
                return None

            if eh_kit_ou_multiplos_pneus(titulo):
                self.logger.info(f"Produto ignorado (kit/múltiplos): {titulo}")
                return None

            q_norm = normalizar_str(self.termo_busca or "")
            titulo_normalizado = normalizar_str(titulo)

            if self.filtro_medida:
                q_norm = q_norm.replace(normalizar_str(self.filtro_medida), " ")

            q_tokens = re.findall(r"[a-z0-9]+(?:\+[a-z0-9]+)?", q_norm)
            palavras_chave_obrigatorias = [
                tok for tok in q_tokens
                if tok not in {"pneu", "r"} and not tok.isdigit() and len(tok) > 1
            ]

            if not all(tok in titulo_normalizado for tok in palavras_chave_obrigatorias):
                self.logger.info(f"Produto ignorado (não corresponde à busca '{self.termo_busca}'): {titulo}")
                return None

            try:
                preco_el = WebDriverWait(self.driver, 6).until(
                    lambda d: card.find_element(
                        By.CSS_SELECTOR,
                        "p[data-testid='price-value'], "
                        "span[data-testid='price-value'], "
                        "[data-testid='price-value']"
                    )
                )
                preco = parse_preco(preco_el.text) if preco_el else None
            except TimeoutException:
                self.logger.error(f"Timeout no PREÇO do card: {titulo}")
                return None

            if not preco or preco < 100:
                self.logger.warning(f"Preço inválido ou baixo demais (R$ {preco}) para o produto: {titulo}")
                return None

            link_temporario = card.get_attribute('href')
            if not link_temporario:
                try:
                    link_temporario = card.find_element(By.CSS_SELECTOR, "a").get_attribute("href")
                except Exception:
                    pass
            if not link_temporario:
                self.logger.warning(f"Sem link no card; ignorando: {titulo}")
                return None

            vendedor = self.marketplace
            link_corrigido = link_temporario
            aba_atual = self.driver.current_window_handle

            self.driver.execute_script("window.open(arguments[0], '_blank');", link_temporario)
            self.driver.switch_to.window(self.driver.window_handles[-1])

            try:
                try:
                    canonical_el = WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "link[rel='canonical']"))
                    )
                    link_corrigido = canonical_el.get_attribute("href") or self.driver.current_url
                except TimeoutException:
                    self.logger.info("Canonical não encontrado rápido; usando URL atual como fallback.")
                    link_corrigido = self.driver.current_url

                try:
                    vendedor_el = WebDriverWait(self.driver, 6).until(
                        EC.presence_of_element_located((
                            By.CSS_SELECTOR,
                            "[data-testid='seller-name'], "
                            "a[data-testid='seller-name'], "
                            "p[data-testid='label'] [data-testid='link'], "
                            "a[href*='lojista'], "
                            "[data-testid='seller']"
                        ))
                    )
                    vendedor_txt = (vendedor_el.text or vendedor_el.get_attribute("innerText")
                                    or vendedor_el.get_attribute("textContent") or "").strip()
                    if vendedor_txt:
                        vendedor = vendedor_txt
                except TimeoutException:
                    self.logger.info(f"Vendedor não visível rapidamente; mantendo '{vendedor}'")

                try:
                    vend_low = normalizar_str(vendedor)
                    if any(bad in vend_low for bad in VENDEDORES_PALAVRAS_INVALIDAS):
                        self.logger.info(f"Produto ignorado (vendedor banido: {vendedor}) - {titulo}")
                        return None
                except Exception:
                    pass

            finally:
                try:
                    self.driver.close()
                finally:
                    self.driver.switch_to.window(aba_atual)

            medida_final = normalizar_medida_valor(titulo)
            marca_final = _extrair_marca_titulo(titulo)
            modelo_final = extrair_modelo_titulo(titulo)

            if self.filtro_medida and medida_final != self.filtro_medida:
                self.logger.info(f"Produto ignorado (medida incorreta): {titulo}")
                return None

            if self.filtro_marca and _norm_soft(marca_final) != _norm_soft(self.filtro_marca):
                self.logger.info(f"Produto ignorado (marca incorreta): {titulo}")
                return None

            if self.filtro_modelo:
                if not (_contains_token(titulo, self.filtro_modelo) or
                        _norm_soft(modelo_final).startswith(_norm_soft(self.filtro_modelo)) or
                        _norm_soft(self.filtro_modelo).startswith(_norm_soft(modelo_final))):
                    self.logger.info(f"Produto ignorado (modelo não bate): {titulo}")
                    return None

            produto = ProdutoMagalu(
                titulo=titulo,
                preco=preco,
                link=link_corrigido,
                data_coleta=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                vendedor=vendedor,
                medida=medida_final,
                marca=marca_final,
                modelo=modelo_final
            )

            if produto.is_valid():
                self.logger.info(f"Produto VÁLIDO encontrado: {titulo}")
                return produto

            self.logger.info(f"Produto inválido após validação do dataclass: {titulo}")
            return None

        except TimeoutException:
            self.logger.error("Timeout ao extrair detalhes do card; ignorando produto.")
            return None
        except Exception as e:
            self.logger.error(f"Erro inesperado ao processar card: {e}")
            return None



    def buscar_produtos(self, termo: str, pagina: int = 1, max_resultados: int = 20, filtros: Optional[Dict] = None,
                        scroll_pages: bool = True) -> List[ProdutoMagalu]:
        produtos = []
        for tentativa in range(CONFIG['RETRY_ATTEMPTS']):
            try:
                if not self.driver:
                    self.driver = self._init_driver()

                url = self.construir_url_busca(termo, pagina, filtros)
                self.logger.info(f"Tentativa {tentativa + 1}: Carregando {url}")
                self.driver.get(url)

                WebDriverWait(self.driver, CONFIG['TIMEOUT']).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[data-testid='product-card-container']"))
                )

                delay_humano(3, 6)
                if scroll_pages:
                    self.scroll_page()
                delay_humano(2, 3)

                cards = self.driver.find_elements(By.CSS_SELECTOR, "a[data-testid='product-card-container']")
                self.logger.info(f"Encontrados {len(cards)} produtos na página")

                for i, card in enumerate(cards):
                    if len(produtos) >= max_resultados:
                        break
                    
                    try:
                        prod = self.extrair_produto_detalhado(card)
                        if prod:
                            produtos.append(prod)
                        if i % 5 == 0:
                            delay_humano(1, 2)
                    except StaleElementReferenceException:
                        self.logger.warning("Elemento obsoleto; seguindo...")
                        continue
                    except Exception as e:
                        self.logger.warning(f"Erro ao processar card: {e}")
                        continue

                self.logger.info(f"Coletados {len(produtos)} produtos válidos nesta página")
                break

            except TimeoutException:
                self.logger.error(f"Timeout na tentativa {tentativa + 1}")
                if tentativa < CONFIG['RETRY_ATTEMPTS'] - 1:
                    delay_humano(5, 10)
            except Exception as e:
                self.logger.error(f"Erro na tentativa {tentativa + 1}: {e}")
                if tentativa < CONFIG['RETRY_ATTEMPTS'] - 1:
                    delay_humano(5, 10)
                    if self.driver:
                        self.driver.quit()
                        self.driver = None

        return produtos

    def buscar_varias_paginas(self, termo: str, max_paginas: int = 5, max_resultados: int = 100, filtros: Optional[dict] = None):
        todos = []
        pagina = 1
        while len(todos) < max_resultados and pagina <= max_paginas:
            try:
                self.logger.info(f"--- Buscando página {pagina} ---")
                prods = self.buscar_produtos(
                    termo=termo,
                    pagina=pagina,
                    max_resultados=max_resultados - len(todos),
                    filtros=filtros,
                    scroll_pages=True
                )
                if not prods:
                    self.logger.info(f"Nenhum produto na página {pagina}. Parando.")
                    break
                todos.extend(prods)
                self.logger.info(f"Acumulado: {len(todos)}")
                pagina += 1
                delay_humano(2, 4)
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
        return todos

    def salvar_resultados(self, produtos: List[ProdutoMagalu], termo: str, formatos: List[str] = None) -> Dict[str, str]:
        if not formatos:
            formatos = ['json']

        arquivos_salvos = {}
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        medida_dir = extrair_medida_path(termo)
        output_dir = self.output_dir / "raw" / medida_dir
        output_dir.mkdir(parents=True, exist_ok=True)
        slug = slugify(termo)

        if 'json' in formatos:
            arquivo_json = output_dir / f"{slug}_{timestamp}.json"
            with open(arquivo_json, "w", encoding="utf-8") as f:
                json.dump([p.to_dict() for p in produtos], f, ensure_ascii=False, indent=2)
            arquivos_salvos['json'] = str(arquivo_json)
            self.logger.info(f"JSON salvo: {arquivo_json}")

        if 'csv' in formatos and produtos:
            arquivo_csv = output_dir / f"{slug}_{timestamp}.csv"
            with open(arquivo_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=produtos[0].to_dict().keys())
                writer.writeheader()
                for p in produtos:
                    writer.writerow(p.to_dict())
            arquivos_salvos['csv'] = str(arquivo_csv)
            self.logger.info(f"CSV salvo: {arquivo_csv}")

        if 'sqlite' in formatos and produtos:
            self.db_manager.salvar_produtos(produtos)
            arquivos_salvos['sqlite'] = str(self.db_manager.db_path)
            self.logger.info(f"Dados salvos no banco: {self.db_manager.db_path}")

        return arquivos_salvos

    def executar_busca_completa(self, termo: str, max_paginas: int = 5,
                               max_resultados: int = 100,
                               formatos: List[str] = None) -> Dict[str, Any]:
        inicio = time.time()
        self.logger.info(f"Iniciando busca completa para: '{termo}'")
        self.logger.info(f"Parâmetros: {max_paginas} páginas, {max_resultados} produtos máx")
        try:
            produtos = self.buscar_varias_paginas(
                termo=termo,
                max_paginas=max_paginas,
                max_resultados=max_resultados
            )

            if not produtos:
                self.logger.warning("Nenhum produto encontrado!")
                return {
                    'termo': termo,
                    'produtos_encontrados': 0,
                    'promocoes': 0,
                    'preco_medio': 0.0,
                    'tempo_execucao': round(time.time() - inicio, 2),
                    'arquivos': {},
                    'produtos': []
                }

            promocoes = sum(1 for p in produtos if getattr(p, "promocao", False))
            preco_medio = sum(p.preco for p in produtos) / len(produtos)

            self.logger.info(f"Busca concluída: {len(produtos)} produtos (promoções: {promocoes}) | preço médio: {preco_medio:.2f}")
            arquivos = self.salvar_resultados(produtos, termo, formatos)

            tempo_total = time.time() - inicio
            relatorio = {
                'termo': termo,
                'produtos_encontrados': len(produtos),
                'promocoes': promocoes,
                'preco_medio': round(preco_medio, 2),
                'tempo_execucao': round(tempo_total, 2),
                'arquivos': {
                    'json_produtos': arquivos.get('json'),
                    'csv_produtos': arquivos.get('csv'),
                    'sqlite_db': arquivos.get('sqlite'),
                },
                'produtos': [p.to_dict() for p in produtos]
            }

            self.logger.info(f"Execução concluída em {tempo_total:.2f}s")
            return relatorio

        except Exception as e:
            self.logger.error(f"Erro na execução completa: {e}")
            raise
        finally:
            self.fechar()

    def fechar(self):
        if self.driver:
            try:
                self.driver.quit()
                self.logger.info("Driver fechado com sucesso")
            except Exception as e:
                self.logger.warning(f"Erro ao fechar driver: {e}")
            finally:
                self.driver = None

# =========================
# CLI
# =========================
def main():
    parser = argparse.ArgumentParser(
        description="Scraper Completo do Magazine Luiza",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument("--run-id", required=False, default=None)
    parser.add_argument("--out-jsonl", required=False, default=None)
    parser.add_argument("--termo", type=str, help="Termo de busca")
    parser.add_argument("--paginas", type=int, default=3, help="Número máximo de páginas (padrão: 3)")
    parser.add_argument("--max", type=int, default=50, help="Número máximo de produtos (padrão: 50)")
    parser.add_argument("--formatos", nargs='+', choices=['json', 'csv', 'sqlite'], default=['json'], help="Formatos de saída")
    parser.add_argument("--headless", type=str, choices=['true', 'false'], default='true', help="Executar em modo headless")
    parser.add_argument("--output", default="data", help="Diretório de saída (padrão: data)")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay de scroll em segundos (padrão: 1.0)")
    parser.add_argument("--verbose", action='store_true', help="Modo verbose (mais logs)")
    parser.add_argument("--lote-json", type=str, default="None", help="Arquivo JSON com termos de busca em lote (opcional)")
    parser.add_argument("--idx-from", type=int, default=0, help="Índice inicial no lote (padrão: 0)")
    parser.add_argument("--idx-to", type=int, help="Índice final no lote (padrão: até o fim)")

    args = parser.parse_args()

    if args.lote_json is not None:
        if not os.path.isfile(args.lote_json):
            root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            alt_path = os.path.join(root_path, args.lote_json)
            if os.path.isfile(alt_path):
                args.lote_json = alt_path
        
        with open(args.lote_json, "r", encoding="utf-8") as f:
            queries = json.load(f)

        run_id = args.run_id or datetime.utcnow().strftime("%Y%m%d_%H%M%S") + "_magalu"
        out_jsonl = args.out_jsonl or os.path.join(args.output, "jsonl", "magalu", f"{run_id}.jsonl")
        batch_docs = []

        idx_to = args.idx_to if args.idx_to is not None else len(queries)
        subset = queries[args.idx_from:idx_to]

        print(f"--- Iniciando busca em lote para {len(subset)} itens ---")

        for idx, item in enumerate(subset, start=args.idx_from):
            termo = item.get("query_flex") or item.get("query_strict")
            
            print(f"\n[{idx+1}/{len(queries)}] Buscando: {termo}")
            
            if not termo:
                print(f"Termo de busca não encontrado no item {idx}. PULANDO.")
                continue

            scraper = ScraperMagalu(
                headless=args.headless.lower() == 'true',
                delay_scroll=args.delay,
                output_dir=args.output,
                termo_busca=termo
            )
            
            try:
                relatorio = scraper.executar_busca_completa(
                    termo=termo,
                    max_paginas=args.paginas,
                    max_resultados=args.max,
                    formatos=args.formatos
                )

                produtos_encontrados = relatorio.get("produtos", [])
                if not produtos_encontrados:
                    print(f"Nenhum produto encontrado para o termo: {termo}")
                    continue

                for prod_dict in produtos_encontrados:
                    raw = _magalu_to_raw(prod_dict)
                    doc = to_canonical(raw, "magalu", item.get("cod_prod", "") or "", run_id)
                    ok, msg = validate_or_warn(doc)
                    if not ok and args.verbose:
                        print(f"[WARN] Validação falhou: {msg} para a URL {doc.get('url')}")
                    batch_docs.append(doc)
                
                print(f"Sucesso! {len(produtos_encontrados)} produtos encontrados para o termo '{termo}'.")

            except Exception as e:
                print(f"!!!!!!!! ERRO AO PROCESSAR O ITEM {idx} ({termo}): {e} !!!!!!!!")
                logging.exception(f"Erro detalhado para o item {idx}:")
            
            finally:
                scraper.fechar()
                delay = delay_humano(10, 20)
                print(f"--- Delay de {delay:.2f}s antes do próximo item ---")

        if batch_docs:
            write_jsonl(out_jsonl, batch_docs)
            print(f"\n[FINALIZADO] Arquivo JSONL salvo em: {out_jsonl} com {len(batch_docs)} itens.")
        return

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    scraper = ScraperMagalu(
        headless=args.headless.lower() == 'true',
        delay_scroll=args.delay,
        output_dir=args.output,
        termo_busca=args.termo
    )

    try:
        relatorio = scraper.executar_busca_completa(
            termo=args.termo,
            max_paginas=args.paginas,
            max_resultados=args.max,
            formatos=args.formatos
        )
        print("\n" + "="*60)
        print("RELATÓRIO FINAL")
        print("="*60)
        print(f"Termo buscado: {relatorio['termo']}")
        print(f"Produtos encontrados: {relatorio['produtos_encontrados']}")
        print(f"Produtos em promoção: {relatorio['promocoes']}")
        print(f"Preço médio: R$ {relatorio['preco_medio']:.2f}")
        print(f"Tempo de execução: {relatorio['tempo_execucao']:.2f}s")
        print("\nArquivos gerados:")
        for formato, arquivo in relatorio['arquivos'].items():
            if arquivo:
                print(f"  {formato.upper()}: {arquivo}")
        print("="*60)

    except KeyboardInterrupt:
        print("\nOperação cancelada pelo usuário")
    except Exception as e:
        print(f"Erro na execução: {e}")
        logging.exception("Erro detalhado:")
    finally:
        scraper.fechar()


if __name__ == "__main__":
    main()
