import random
import re
import time
import unicodedata
import string
from typing import Optional, List
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

MARCAS_CONHECIDAS = {
    "pirelli","continental","michelin","goodyear","bridgestone","firestone","dunlop",
    "cooper","hankook","yokohama","kumho","falken","maxxis","bf goodrich","sailun",
    "linglong","avanzi","blacklion","silverstone","aptany","roadstone","sunfull",
    "triangle","zeetex","nexen","aeolus","sumitomo","gislaved","barum","kelly","kenda",
    "goodride","cargo","tigar","westlake","lanvigator","techsun","remold","remoldado",
}

PADROES_KIT = [
    r"\bkit\b",
    r"\bconjunto\b",
    r"\bjogo\b",
    r"\bpar\b",
    r"\bpares?\b",
    r"\b2\s*pneus?\b",
    r"\b4\s*pneus?\b",
    r"\b(2|4)x?\s*pneus?\b",
    r"\bcombo\b",
    r"\bfechamento\b",
    r"\blote\b",
    r"\bunidades\b",
    r"\b4uni\b",
    r"\bk\d\b",
    r"\bk\d{1,2}\b",
    r"\bduas?\s*pneus?\b",
    r"\b4|2\s*unid?\b",
]
PADROES_KIT = [re.compile(p, re.IGNORECASE) for p in PADROES_KIT]

PONTUACAO = str.maketrans({c: " " for c in string.punctuation})

def extrair_medida(termo: str) -> str:
    termo = termo.lower().replace("/", " ").replace("-", " ")
    padrao = r'(\d{3})\s*(\d{2})\s*r?\s*(\d{2})'
    m = re.search(padrao, termo)
    if m:
        return f"{m.group(1)}-{m.group(2)}-r{m.group(3)}"
    return "medida_desconhecida"


def _parse_valor(texto: str) -> float | None:
    if not texto:
        return None
    s = texto.strip()
    s = re.sub(r"[^\d,\.]", "", s)
    if not s:
        return None
    tp = "." in s
    tv = "," in s
    if tp and tv:
        s = s.replace(".", "").replace(",", ".")
    elif tv and not tp:
        s = s.replace(",", ".")
    elif tp and not tv:
        partes = s.split(".")
        if len(partes[-1]) in (1, 2):
            s = "".join(partes[:-1]) + "." + partes[-1]
        else:
            s = "".join(partes)
    try:
        return float(s)
    except ValueError:
        return None

def detectar_marca(texto: str) -> Optional[str]:
    if not texto:
        return None
    t = texto.lower()
    for m in MARCAS_CONHECIDAS:
        if m in t:
            return m
    return None

def eh_kit_ou_multiplos_pneus(texto: str) -> bool:
    if not texto:
        return False
    t = texto.lower()
    for rgx in PADROES_KIT:
        if rgx.search(t):
            return True
    return False

"""def limpar_preco(texto: str) -> float:
    v = _parse_valor(texto)
    return v if v is not None else 0.0"""

"""def extrair_texto_seguro(elemento, seletores: List[str], atributo: str = None) -> str:
    for sel in seletores:
        try:
            el = elemento.find_element(By.CSS_SELECTOR, sel)
            if atributo:
                val = el.get_attribute(atributo)
            else:
                val = el.text
            if val and val.strip():
                return val.strip()
        except NoSuchElementException:
            continue
        except Exception:
            continue
    return """

def slugify(texto: str) -> str:
    s = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode()
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s)
    return s.strip("-")

def construir_dim_pattern(termo: str) -> Optional[re.Pattern]:
    if not termo:
        return None
    nums = re.findall(r"\d+", termo)
    if len(nums) < 3:
        return None
    larg, perf, aro = nums[0], nums[1], nums[2]

    pat = (
        rf"\b{re.escape(larg)}[\s\-/]*{re.escape(perf)}[\s\-/]*[Rr]?[\s\-/]*{re.escape(aro)}\b"
        rf"|\b{re.escape(larg)}{re.escape(perf)}[Rr]?{re.escape(aro)}\b"
    )
    return re.compile(pat, re.IGNORECASE)


def _delay_between_cards(min_delay, max_delay, logger=None):
    d = random.uniform(min_delay, max_delay)
    if logger:
        logger.debug(f"Delaying {d:.2f}s before next card")

    time.sleep(d)

