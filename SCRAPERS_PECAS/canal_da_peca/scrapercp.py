from __future__ import annotations
import argparse, csv, json, os, re, sys, time, uuid, logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote_plus, urlparse
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

LOGGER = logging.getLogger("cdp")

def setup_logging(base_dir: str, verbose: bool = False):
    os.makedirs(base_dir, exist_ok=True)
    log_path = os.path.join(base_dir, "run.log")
    LOGGER.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)
    ch.setFormatter(fmt)
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    if not LOGGER.handlers:
        LOGGER.addHandler(ch)
        LOGGER.addHandler(fh)
    LOGGER.info("Logging inicializado em %s", log_path)

NORMALIZE_RE = re.compile(r"[^A-Za-z0-9]+", re.UNICODE)
PART_KEY_RE = re.compile("|".join([
    r"n[úu]mero\s*de\s*pe[cç]a",
    r"n[ºo]\s*da\s*pe[cç]a",
    r"c[óo]digo\s*do\s*fabricante",
    r"c[óo]d\.?\s*do\s*produto",
    r"ref\.\s*do\s*produto",
    r"ref(er[eê]ncia)?",
    r"sku",
    r"part\s*number",
    r"pn",
    r"mpn",
]), re.IGNORECASE)
BRAND_KEY_RE = re.compile("|".join([r"marca", r"fabricante", r"brand", r"manufacturer", r"montadora"]), re.IGNORECASE)

def normalize(s: Optional[str]) -> str:
    if not s:
        return ""
    try:
        import unicodedata
        s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    except Exception:
        pass
    return NORMALIZE_RE.sub("", s).upper()

def parse_price_to_float(price_str: Optional[str]) -> Optional[float]:
    if not price_str:
        return None
    s = price_str.replace(".", "").replace(" ", "")
    s = s.replace("R$", "").replace("US$", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        try:
            return float(re.sub(r"[^\d.]", "", s))
        except Exception:
            return None

def infer_units_from_text(text: str) -> Tuple[Optional[str], Optional[int]]:
    if not text:
        return None, None
    t = text.lower()
    m = re.search(r"\bpar(es)?\b", t)
    if m: return "PAR", 2
    m = re.search(r"\bkit(?:\s*(?:com|c\/|de))?\s*(\d+)\b", t)
    if m: return "KIT", int(m.group(1))
    m = re.search(r"\b(\d+)\s*(?:pe[çc]as|pcs|unidades|unds|und)\b", t)
    if m: return "LOTE", int(m.group(1))
    m = re.search(r"\b(?:x|×)\s*(\d+)\b", t)
    if m: return "LOTE", int(m.group(1))
    m = re.search(r"\bc\/\s*(\d+)\b", t)
    if m: return "LOTE", int(m.group(1))
    if "kit" in t or "jogo" in t or "conjunto" in t: return "KIT", None
    return "UNIDADE", 1

def infer_units(specs: Dict[str,str], title: str, desc: str) -> Tuple[Optional[str], Optional[int]]:
    for k, v in specs.items():
        nk = normalize(k)
        if any(t in nk for t in ("UNIDADES", "UND", "PECAS", "PECA", "CONTEUD", "QUANT")):
            m = re.search(r"\d+", v.replace(".", " "))
            if m:
                try: return "LOTE", int(m.group(0))
                except: pass
            st, un = infer_units_from_text(v)
            if st or un: return st, un
    return infer_units_from_text((title or "") + " " + (desc or ""))

def extract_mpn_brand_from_jsonld(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    mpn, brand = None, None
    for s in soup.select("script[type='application/ld+json']"):
        try:
            data = json.loads(s.get_text(strip=True))
        except Exception:
            continue
        stack = data if isinstance(data, list) else [data]
        for node in stack:
            if isinstance(node, dict):
                if not brand:
                    b = node.get("brand")
                    if isinstance(b, dict) and b.get("name"): brand = str(b["name"])
                    elif isinstance(b, str): brand = b
                if not mpn:
                    for key in ("mpn", "sku", "gtin13", "gtin"):
                        if node.get(key):
                            mpn = str(node[key]); break
    return mpn, brand

def extract_code_from_text(txt: str) -> Optional[str]:
    tokens = re.findall(r"[A-Z0-9][A-Z0-9.\-]*\d[A-Z0-9.\-]*", txt.upper())
    if not tokens:
        digits = re.findall(r"\b\d{3,}\b", txt)
        return max(digits, key=len) if digits else None
    def score(t: str): return (len(re.findall(r"\d", t)), len(t))
    return max(tokens, key=score)

def extract_partner_code(soup: BeautifulSoup) -> Optional[str]:
    node = soup.select_one("span#product_partcode.product_partcode") or soup.select_one("div[id^='partnerPartCode-'].produto-mfrPartCode")
    if not node:
        return None
    txt = node.get_text(" ", strip=True)
    return extract_code_from_text(txt) if txt else None

def extract_title(driver, soup) -> Tuple[Optional[str], str]:
    try:
        el = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "h1.product_name, h1[id^='productName-'], h2.produto-nome, h2[id^='produto-nome-']"))
        )
    except Exception:
        el = None
    if el:
        return el.text.strip(), "h1/h2.product_name"
    sels = ["h1.product_name", "h1[id^='productName-']", "h2.produto-nome", "h2[id^='produto-nome-']", "meta[property='og:title']", "h1", "title"]
    for sel in sels:
        node = soup.select_one(sel)
        if node:
            if node.name == "meta":
                return (node.get("content") or "").strip(), f"meta:{sel}"
            elif node.name == "title":
                return node.get_text(strip=True), "<title>"
            else:
                return node.get_text(strip=True), sel
    return None, "not-found"

def extract_price(driver, soup) -> Tuple[Optional[str], Optional[str], str]:
    cands = []
    for n in soup.select("div[itemprop='offers'] meta[itemprop='price'], meta[itemprop='price']"):
        v = (n.get("content") or "").strip()
        if v and re.search(r"\d", v):
            cands.append(v)
    if cands:
        nums = [(v, parse_price_to_float(v) or 0.0) for v in cands]
        nums = [x for x in nums if x[1] > 0]
        if nums:
            best = max(nums, key=lambda x: x[1])[0]
            cur = None
            cur_node = soup.select_one("div[itemprop='offers'] meta[itemprop='priceCurrency'], meta[itemprop='priceCurrency']")
            if cur_node:
                cur = (cur_node.get("content") or "").strip()
            return best, cur, "meta:offers"
    for sel in ["div.product-price span.price-title", "[itemprop='offers'] [data-price]", "span[data-price]", "span.price", "strong.price", "div.price", "span[class*='price']"]:
        node = soup.select_one(sel)
        if node:
            txt = node.get("data-price") or node.get_text(" ", strip=True)
            if txt and re.search(r"\d", txt):
                return txt, None, sel
    return None, None, "not-found"

def slug_from_url(url: str) -> str:
    path = urlparse(url).path.rstrip("/")
    slug = path.split("/")[-1] or str(uuid.uuid4())
    return re.sub(r"[^a-zA-Z0-9._-]", "_", slug)

@dataclass
class Product:
    query: str
    searched_part: Optional[str]
    url: str
    title: Optional[str] = None
    price: Optional[str] = None
    currency: Optional[str] = None
    price_num: Optional[float] = None
    matched_part: Optional[str] = None
    matched_brand: Optional[str] = None
    part_match: bool = False
    sale_type: Optional[str] = None
    units: Optional[int] = None
    price_per_unit: Optional[float] = None
    specs: Dict[str, str] = field(default_factory=dict)
    desc_excerpt: Optional[str] = None
    source: str = "canaldapeca"
    accepted: bool = False
    decision_reason: str = ""

    def row(self) -> Dict[str, str]:
        base = {
            "query": self.query,
            "searched_part": self.searched_part or "",
            "url": self.url,
            "title": self.title or "",
            "price": self.price or "",
            "currency": self.currency or "",
            "price_num": f"{self.price_num:.2f}" if self.price_num is not None else "",
            "matched_part": self.matched_part or "",
            "matched_brand": self.matched_brand or "",
            "part_match": "1" if self.part_match else "0",
            "sale_type": self.sale_type or "",
            "units": str(self.units) if self.units is not None else "",
            "price_per_unit": f"{self.price_per_unit:.2f}" if self.price_per_unit is not None else "",
            "desc_excerpt": (self.desc_excerpt or "")[:500],
            "source": self.source,
            "accepted": "1" if self.accepted else "0",
            "decision_reason": self.decision_reason,
        }
        for k, v in self.specs.items():
            base.setdefault(f"spec:{k}", v)
        return base

def make_driver(headful: bool=False) -> webdriver.Chrome:
    chrome_opts = Options()
    if not headful:
        chrome_opts.add_argument("--headless=new")
    chrome_opts.add_argument("--disable-gpu")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--window-size=1366,768")
    chrome_opts.add_argument("--disable-blink-features=AutomationControlled")
    chrome_opts.add_argument("--lang=pt-BR")
    chrome_opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_opts.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(options=chrome_opts)
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {"behavior": "deny"})
    LOGGER.debug("Driver Chrome criado (headful=%s)", headful)
    return driver

def wait(driver, timeout=20):
    return WebDriverWait(driver, timeout)

def maybe_accept_cookies(driver):
    selectors = [
        "button#onetrust-accept-btn-handler",
        "button[aria-label*='aceitar']",
        "button:contains('Aceitar')"
    ]
    for sel in selectors:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            el.click(); time.sleep(0.5)
            LOGGER.debug("Banner de cookies fechado (%s)", sel)
            return
        except Exception:
            pass

BASE = "https://www.canaldapeca.com.br"

def open_search(driver, query: str):
    LOGGER.info("Abrindo busca: %s", query)
    driver.get(BASE)
    maybe_accept_cookies(driver)
    for sel in ["input[name='q']", "input[type='search']", "input#search", "input[placeholder*='busque' i]"]:
        try:
            box = wait(driver).until(EC.presence_of_element_located((By.CSS_SELECTOR, sel)))
            box.clear(); box.send_keys(query); box.send_keys(Keys.ENTER)
            LOGGER.debug("Busca enviada via campo %s", sel)
            return
        except Exception:
            continue
    u = f"{BASE}/busca/?q={quote_plus(query)}"
    driver.get(u)
    LOGGER.debug("Busca via URL fallback: %s", u)

def collect_result_urls(driver, limit: int=24) -> List[str]:
    time.sleep(1.5)
    candidates = ["a[href*='/produto/']", "a[href*='/p/']", "div.product-card a[href]"]
    hrefs = []
    for sel in candidates:
        anchors = driver.find_elements(By.CSS_SELECTOR, sel)
        for a in anchors:
            href = (a.get_attribute("href") or "").split("?")[0]
            if href.startswith("http"):
                hrefs.append(href)
        if hrefs:
            LOGGER.debug("Coletados %d URLs com seletor %s", len(hrefs), sel)
    seen, out = set(), []
    for h in hrefs:
        if h not in seen:
            seen.add(h); out.append(h)
        if len(out) >= limit:
            break
    LOGGER.info("Total de URLs coletadas para visitar: %d", len(out))
    return out

def extract_first_match_from_specs(specs: Dict[str,str], rx: re.Pattern) -> Tuple[Optional[str], Optional[str]]:
    for k, v in specs.items():
        if rx.search(k):
            return v.strip(), k
    return None, None

def parse_product_page(driver, url: str, query: str, searched_part: Optional[str],
                       save_dir: Optional[str], save_html: bool) -> Tuple[Product, str]:
    LOGGER.info("Visitando produto para VERIFICAÇÃO: %s", url)
    driver.get(url)
    maybe_accept_cookies(driver)
    wait(driver, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")

    title, title_src = extract_title(driver, soup)
    LOGGER.debug("Título(%s): %s", title_src, title)

    price_txt, currency, price_src = extract_price(driver, soup)
    LOGGER.debug("Preço(%s): %s %s", price_src, currency or "", price_txt or "")

    specs: Dict[str, str] = {}
    for table in soup.select("table.table-specifications, table.specifications, table"):
        for tr in table.select("tr"):
            th = tr.find("th"); td = tr.find("td")
            if th and td:
                k = th.get_text(" ", strip=True)
                v = td.get_text(" ", strip=True)
                if k and v:
                    specs[k] = v
        if specs: break
    LOGGER.debug("Specs capturadas (%d chaves)", len(specs))

    desc = ""
    for sel in ["#description", ".product-description", "section.description", "div#descricao", "div.description"]:
        el = soup.select_one(sel)
        if el:
            desc = el.get_text("\n", strip=True); break
    
    accepted = False
    reason = f"REJECT_PN_NAO_ENCONTRADO_NA_PAGINA({searched_part})" 
    
    if not searched_part:
        reason = "REJECT_ALVO_SEM_CODIGO"
        n_searched_part = ""
    else:
        n_searched_part = normalize(searched_part)

    if n_searched_part and n_searched_part in normalize(title):
        accepted = True
        reason = f"OK_PN_VERIFICADO_NO_TITULO"

    if not accepted and n_searched_part:
        for spec_key, spec_value in specs.items():
            if n_searched_part in normalize(spec_value) or n_searched_part in normalize(spec_key):
                accepted = True
                reason = f"OK_PN_VERIFICADO_NAS_ESPECS (em '{spec_key}')"
                break 

    LOGGER.info("Decisão: %s — %s", "ACEITO" if accepted else "REJEITADO", reason)

    matched_brand, _ = extract_first_match_from_specs(specs, BRAND_KEY_RE)
    sale_type, units = infer_units(specs, title or "", desc or "")
    price_num = parse_price_to_float(price_txt)
    price_per_unit = (price_num / units) if (price_num is not None and units and units > 0) else None

    prod = Product(
        query=query,
        searched_part=searched_part,
        url=url,
        title=title or None,
        price=price_txt or None,
        currency=currency or None,
        price_num=price_num,
        matched_part=searched_part if accepted else "", 
        matched_brand=matched_brand,
        part_match=accepted, 
        sale_type=sale_type,
        units=units,
        price_per_unit=price_per_unit,
        specs=specs,
        desc_excerpt=(desc or "")[:1000] or None,
        accepted=accepted,
        decision_reason=reason,
    )

    if save_dir:
        os.makedirs(save_dir, exist_ok=True)
        slug = slug_from_url(url)
        if save_html:
            with open(os.path.join(save_dir, f"{slug}.html"), "w", encoding="utf-8") as f:
                f.write(html)
        with open(os.path.join(save_dir, f"{slug}.json"), "w", encoding="utf-8") as f:
            json.dump(prod.row(), f, ensure_ascii=False, indent=2)

    return prod, html

def write_csv(rows: List[Product], out_path: str) -> None:
    cols: List[str] = []
    for p in rows:
        for k in p.row().keys():
            if k not in cols:
                cols.append(k)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for p in rows:
            w.writerow(p.row())
    LOGGER.info("Arquivo CSV salvo: %s (%d linhas)", out_path, len(rows))

def append_to_master_csv(rows: List[Product], master_path: str):
    accepted_rows = [p for p in rows if p.accepted]
    if not accepted_rows:
        LOGGER.info("Nenhum produto aceito neste lote para adicionar ao arquivo mestre.")
        return

    os.makedirs(os.path.dirname(master_path), exist_ok=True)

    all_cols = []
    for p in accepted_rows:
        for k in p.row().keys():
            if k not in all_cols:
                all_cols.append(k)
    
    file_exists = os.path.exists(master_path)

    try:
        with open(master_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_cols, restval='', extrasaction='ignore')
            
            if not file_exists:
                writer.writeheader()
            
            for p in accepted_rows:
                writer.writerow(p.row())
        
        LOGGER.info(f"✅ {len(accepted_rows)} produtos aceitos foram adicionados com sucesso ao arquivo mestre: {master_path}")

    except Exception as e:
        LOGGER.error(f"Falha ao escrever no arquivo mestre CSV: {e}")

def save_consolidated(lote_dir: str, base_out_dir: str, rows: List[Product]):
    all_csv = os.path.join(lote_dir, "all_products.csv")
    ok_csv  = os.path.join(lote_dir, "accepted_only.csv")
    nok_csv = os.path.join(lote_dir, "rejected.csv")
    write_csv(rows, all_csv)
    write_csv([r for r in rows if r.accepted], ok_csv)
    write_csv([r for r in rows if not r.accepted], nok_csv)
    resumo = {
        "total": len(rows),
        "aceitos": sum(1 for r in rows if r.accepted),
        "rejeitados": sum(1 for r in rows if not r.accepted),
        "por_query": {},
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
    for r in rows:
        d = resumo["por_query"].setdefault(r.query, {"total":0,"aceitos":0,"rejeitados":0})
        d["total"] += 1
        d["aceitos"] += 1 if r.accepted else 0
        d["rejeitados"] += 0 if r.accepted else 1
    with open(os.path.join(lote_dir, "resumo_lote.json"), "w", encoding="utf-8") as f:
        json.dump(resumo, f, ensure_ascii=False, indent=2)
    LOGGER.info("Resumo do lote salvo.")

    master_csv_path = os.path.join(base_out_dir, "master_products.csv")
    append_to_master_csv(rows, master_csv_path)

def make_lote_dir(base_out: str, lote_id: Optional[str]) -> str:
    if not lote_id:
        lote_id = datetime.now().strftime("lote_%Y%m%d_%H%M%S")
    path = os.path.join(base_out, lote_id)
    os.makedirs(path, exist_ok=True)
    return path

def make_driver_and_log(headful: bool) -> webdriver.Chrome:
    try:
        return make_driver(headful=headful)
    except Exception as e:
        LOGGER.error("Falha criando WebDriver: %s", e)
        raise

def open_search_safe(driver, q: str):
    try:
        open_search(driver, q)
    except Exception as e:
        LOGGER.error("Erro ao abrir busca '%s': %s", q, e)
        raise

def infer_part_from_query(q: str) -> Optional[str]:
    toks = re.findall(r"[A-Za-z0-9\-\.]+", q or "")
    cand = None
    best_score = (-1, -1)
    for t in toks:
        digits = len(re.findall(r"\d", t))
        if digits == 0:
            continue
        score = (digits, len(t))
        if score > best_score:
            best_score = score
            cand = t
    return cand

def login_canaldapeca(driver):
    load_dotenv()
    username = os.getenv("CDP_USER")
    password = os.getenv("CDP_PASS")

    if not username or not password:
        LOGGER.warning("Credenciais não fornecidas. Pulando login.")
        return
    
    try:
        LOGGER.info("Realizando login no Canal da Peça...")
        driver.get("https://www.canaldapeca.com.br/login")

        wait(driver).until(EC.presence_of_element_located((By.ID, "username"))).send_keys(username)
        
        driver.find_element(By.ID, "password").send_keys(password)

        driver.find_element(By.ID, "submit").click()

        wait(driver).until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/minha-conta']")))
        LOGGER.info("✅ Login realizado com sucesso.")

    except TimeoutException:
        LOGGER.error("❌ Erro de Timeout: A página de login demorou muito ou um dos elementos não foi encontrado. Verifique se os seletores ainda são válidos.")
        return
    except Exception as e:
        LOGGER.error("❌ Erro inesperado ao preencher formulário de login: %s", e)
        return

def run(query: Optional[str], parts_file: Optional[str], parts: List[str],
        catalog_csv: Optional[str], ref_col: str, limit: int, headful: bool,
        base_out: str, lote_id: Optional[str], save_html: bool, verbose: bool) -> None:
    lote_dir = make_lote_dir(base_out, lote_id)
    setup_logging(lote_dir, verbose=verbose)
    LOGGER.info("Diretório do lote: %s", lote_dir)

    driver = make_driver_and_log(headful=headful)
    login_canaldapeca(driver)
    rows: List[Product] = []

    try:
        queries: List[Tuple[str, Optional[str]]] = []
        if query:
            queries.append((query, None))
        if parts_file:
            with open(parts_file, "r", encoding="utf-8") as f:
                plist = [line.strip() for line in f if line.strip()]
                for p in plist: queries.append((p, p))
        if parts:
            for p in parts: queries.append((p, p))
        if catalog_csv:
            with open(catalog_csv, "r", encoding="utf-8-sig") as f:
                rd = csv.DictReader(f)
                header = rd.fieldnames or []
                def autodetect(wanted: List[str]) -> Optional[str]:
                    def n(s): return normalize(s or "")
                    for h in header:
                        if any(n(w) in n(h) for w in wanted):
                            return h
                    return None
                rc = ref_col or autodetect(["referencia","ref","pn","partnumber","codigo","codigo_fabricante","mpn"])
                for row in rd:
                    part = (row.get(rc) or "").strip() if rc else ""
                    q = part or ""
                    if q:
                        queries.append((q, part or None))

        LOGGER.info("Total de consultas no lote: %d", len(queries))

        for q, searched_part in queries:
            if not searched_part:
                ipn = infer_part_from_query(q)
                searched_part = ipn if ipn else None
            LOGGER.info("Alvo desta consulta → PN=%s", searched_part)

            prod_dir = os.path.join(lote_dir, normalize(searched_part)) if searched_part else os.path.join(lote_dir, "sem_codigo")
            accepted_dir = os.path.join(prod_dir, "accepted")
            rejected_dir = os.path.join(prod_dir, "rejected")

            open_search_safe(driver, q)
            urls = collect_result_urls(driver, limit=limit)
            if not urls:
                LOGGER.warning("Nenhum resultado para query: %s", q)

            for u in urls:
                try:
                    p, html = parse_product_page(driver, u, q, searched_part, save_dir=None, save_html=False)
                    save_dir = accepted_dir if p.accepted else rejected_dir
                    os.makedirs(save_dir, exist_ok=True)
                    slug = slug_from_url(u)
                    if save_html:
                        with open(os.path.join(save_dir, f"{slug}.html"), "w", encoding="utf-8") as f:
                            f.write(html)
                    with open(os.path.join(save_dir, f"{slug}.json"), "w", encoding="utf-8") as f:
                        json.dump(p.row(), f, ensure_ascii=False, indent=2)
                    rows.append(p)
                except Exception as e:
                    LOGGER.error("Falha ao processar URL %s: %s", u, e)

    finally:
        driver.quit()
        LOGGER.debug("Driver finalizado.")

    save_consolidated(lote_dir, base_out_dir=base_out, rows=rows)
    LOGGER.info("✅ Lote concluído. Saída em: %s", lote_dir)

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Scraper Canal da Peça (PN estrito)")
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--query")
    group.add_argument("--parts-file")
    group.add_argument("--parts", nargs="+")
    group.add_argument("--catalog-csv")
    ap.add_argument("--ref-col", default="")
    ap.add_argument("--limit", type=int, default=24)
    ap.add_argument("--headful", action="store_true")
    ap.add_argument("--outdir", default="data")
    ap.add_argument("--lote-id", default="")
    ap.add_argument("--save-html", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    run(args.query, args.parts_file, args.parts or [], args.catalog_csv,
        args.ref_col, args.limit, args.headful, base_out=args.outdir,
        lote_id=(args.lote_id or None), save_html=args.save_html, verbose=args.verbose)
