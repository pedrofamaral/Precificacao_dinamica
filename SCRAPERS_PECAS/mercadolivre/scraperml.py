from __future__ import annotations
import asyncio
import csv
import dataclasses as dc
import json
import re
import sys
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote_plus

import argparse
try:
    from playwright.async_api import async_playwright, Browser, Page
except Exception:
    async_playwright = None

NORMALIZE_RE = re.compile(r"[^A-Za-z0-9]+", re.UNICODE)
def normalize(s: Optional[str]) -> str:
    if not s:
        return ""
    try:
        import unicodedata
        s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    except Exception:
        pass
    return NORMALIZE_RE.sub("", s).upper()

PART_KEYS = [
    r"n[úu]mero\s*de\s*pe[cç]a",
    r"n[ºo]\s*da\s*pe[cç]a",
    r"n[úu]mero\s*da\s*pe[cç]a",
    r"c[óo]digo\s*do\s*fabricante",
    r"c[óo]d\.?\s*do\s*produto",
    r"ref\.\s*do\s*produto",
    r"ref(er[eê]ncia)?",
    r"sku",
    r"part\s*number",
    r"pn",
    r"mpn",
]
BRAND_KEYS = [
    r"marca",
    r"fabricante",
    r"brand",
    r"manufacturer",
    r"montadora",
]

PADROES_KIT = [
    r"\bkit\b",
    r"\bconjunto\b",
    r"\bpar\b",
    r"\bpares\b",
    r"\bjogo\b",
    r"\bpe[çc]as\b",
    r"\bunidades\b",
    r"\bund\b",
    r"\bc\/\s*\d+\b",
    r"\b\d+\s*x\b",
    r"\bx\s*\d+\b",
    r"\b\d+\s*pe[çc]as\b",
]

PART_KEY_RE = re.compile("|".join(PART_KEYS), re.IGNORECASE)
BRAND_KEY_RE = re.compile("|".join(BRAND_KEYS), re.IGNORECASE)
KIT_RE = re.compile("|".join(PADROES_KIT), re.IGNORECASE)

@dc.dataclass
class Product:
    query: str
    searched_part: Optional[str]
    searched_brand: Optional[str]
    url: str
    title: Optional[str] = None
    price: Optional[str] = None
    currency: Optional[str] = None
    price_num: Optional[float] = None
    seller: Optional[str] = None
    matched_part: Optional[str] = None
    matched_brand: Optional[str] = None
    part_match: bool = False
    brand_match: bool = False
    sale_type: Optional[str] = None
    units: Optional[int] = None
    price_per_unit: Optional[float] = None
    specs: Dict[str, str] = dc.field(default_factory=dict)
    desc_excerpt: Optional[str] = None
    source: str = "mercadolivre"

    def row(self) -> Dict[str, str]:
        base = {
            "query": self.query,
            "searched_part": self.searched_part or "",
            "searched_brand": self.searched_brand or "",
            "url": self.url,
            "title": self.title or "",
            "price": self.price or "",
            "currency": self.currency or "",
            "price_num": f"{self.price_num:.2f}" if self.price_num is not None else "",
            "seller": self.seller or "",
            "matched_part": self.matched_part or "",
            "matched_brand": self.matched_brand or "",
            "part_match": "1" if self.part_match else "0",
            "brand_match": "1" if self.brand_match else "0",
            "sale_type": self.sale_type or "",
            "units": str(self.units) if self.units is not None else "",
            "price_per_unit": f"{self.price_per_unit:.2f}" if self.price_per_unit is not None else "",
            "desc_excerpt": (self.desc_excerpt or "")[:500],
            "source": self.source,
        }
        for k, v in self.specs.items():
            base.setdefault(f"spec:{k}", v)
        return base

SEARCH_BASE = "https://lista.mercadolivre.com.br/{}#D[A:{}]"

async def collect_search_result_urls(page: Page, query: str, limit: int = 24) -> List[str]:
    url = SEARCH_BASE.format(quote_plus(query), quote_plus(query))
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    for _ in range(3):
        await page.mouse.wheel(0, 2000)
        await page.wait_for_timeout(500)
    selectors = [
        "li.ui-search-layout__item a.ui-search-link",
        "a.ui-search-item__group__element.ui-search-link__title-card",
        "a.ui-search-result__content-wrapper.ui-search-link",
        "a[href*='/MLB-']",
    ]
    hrefs: List[str] = []
    for sel in selectors:
        try:
            anchors = await page.query_selector_all(sel)
            for a in anchors:
                href = await a.get_attribute("href")
                if href and "/MLB-" in href:
                    hrefs.append(href.split("?")[0])
        except Exception:
            continue
        if hrefs:
            break
    seen, out = set(), []
    for h in hrefs:
        if h not in seen:
            seen.add(h)
            out.append(h)
        if len(out) >= limit:
            break
    return out

def _parse_price_to_float(price_str: Optional[str]) -> Optional[float]:
    if not price_str:
        return None
    s = price_str.strip()
    s = s.replace(".", "").replace(" ", "")
    s = s.replace("R$", "").replace("US$", "")
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        try:
            return float(re.sub(r"[^\d.]", "", s))
        except Exception:
            return None

async def parse_price_and_currency(page: Page) -> Tuple[Optional[str], Optional[str]]:
    try:
        price = await page.get_attribute("meta[itemprop='price']", "content")
        currency = await page.get_attribute("meta[itemprop='priceCurrency']", "content")
        if price:
            return price, currency
    except Exception:
        pass
    candidates = [
        ("span.andes-money-amount__fraction", "span.andes-money-amount__currency-symbol"),
        ("span.price-tag-fraction", "span.price-tag-symbol"),
        ("div.ui-pdp-price__second-line span.andes-money-amount__fraction",
         "div.ui-pdp-price__second-line span.andes-money-amount__currency-symbol"),
    ]
    for p_sel, c_sel in candidates:
        try:
            p = await page.text_content(p_sel)
            c = await page.text_content(c_sel)
            if p:
                return p.replace(".", ""), (c or "").strip()
        except Exception:
            continue
    return None, None

async def parse_specs_table(page: Page) -> Dict[str, str]:
    specs: Dict[str, str] = {}
    table_selectors = [
        "div.ui-vpp-striped-specs__table table.andes-table",
        "div.ui-pdp-specs__table table.andes-table",
        "section.ui-pdp-specs__section--highlighted table.andes-table",
        "table.andes-table",
    ]
    for tsel in table_selectors:
        tables = await page.query_selector_all(tsel)
        for t in tables:
            rows = await t.query_selector_all("tr")
            for r in rows:
                th = await r.query_selector("th")
                td = await r.query_selector("td")
                key = (await th.inner_text()).strip() if th else None
                val = (await td.inner_text()).strip() if td else None
                if key and val:
                    specs[key] = val
        if specs:
            break
    if not specs:
        items = await page.query_selector_all("li.ui-pdp-list__item")
        for it in items:
            txt = (await it.inner_text() or "").strip()
            if ":" in txt:
                k, v = txt.split(":", 1)
                specs[k.strip()] = v.strip()
    return specs

async def parse_description_text(page: Page) -> str:
    parts: List[str] = []
    selectors = [
        "div.ui-pdp-description__content",
        "div.ui-pdp-description",
        "div[data-testid='description-content']",
        "section#description",
    ]
    for sel in selectors:
        try:
            el = await page.query_selector(sel)
            if el:
                txt = await el.inner_text()
                if txt:
                    parts.append(txt.strip())
        except Exception:
            continue
    if not parts:
        paras = await page.query_selector_all("p, li")
        for p in paras[:50]:
            try:
                t = await p.inner_text()
                if t and len(t) > 30:
                    parts.append(t.strip())
            except Exception:
                continue
    return "\n".join(parts)

def extract_first_match_from_specs(specs: Dict[str,str], pattern_re: re.Pattern) -> Optional[str]:
    for k, v in specs.items():
        if pattern_re.search(k):
            return v.strip()
    return None

def extract_mpn_from_jsonld(blobs: List[str]) -> Tuple[Optional[str], Optional[str]]:
    mpn, brand = None, None
    for raw in blobs:
        try:
            data = json.loads(raw)
        except Exception:
            continue
        stack = data if isinstance(data, list) else [data]
        for node in stack:
            try:
                if isinstance(node, dict):
                    if not brand and isinstance(node.get("brand"), dict) and node["brand"].get("name"):
                        brand = str(node["brand"]["name"])
                    if not mpn and (node.get("mpn") or node.get("sku") or node.get("gtin13") or node.get("gtin")):
                        mpn = str(node.get("mpn") or node.get("sku") or node.get("gtin13") or node.get("gtin"))
            except Exception:
                continue
    return mpn, brand

def find_ref_in_text(text: str, searched_part: str) -> Optional[str]:
    if not text or not searched_part:
        return None
    p = re.escape(searched_part)
    variants = [
        rf"\b{p}\b",
        rf"\b{p[:2]}[ .-]?{p[2:]}\b" if len(searched_part) > 3 else None,
    ]
    variants = [v for v in variants if v]
    rx = re.compile("|".join(variants), re.IGNORECASE)
    m = rx.search(text)
    return m.group(0) if m else None

def infer_units_from_text(text: str) -> Tuple[Optional[str], Optional[int]]:
    if not text:
        return None, None
    t = text.lower()
    m = re.search(r"\bpar(es)?\b", t)
    if m:
        return "PAR", 2
    m = re.search(r"\bkit(?:\s*(?:com|c\/|de))?\s*(\d+)\b", t)
    if m:
        try:
            return "KIT", int(m.group(1))
        except Exception:
            return "KIT", None
    m = re.search(r"\b(\d+)\s*(?:pe[çc]as|pcs|unidades|unds|und)\b", t)
    if m:
        try:
            return "LOTE", int(m.group(1))
        except Exception:
            return "LOTE", None
    m = re.search(r"\b(?:x|×)\s*(\d+)\b", t)
    if m:
        try:
            return "LOTE", int(m.group(1))
        except Exception:
            return "LOTE", None
    m = re.search(r"\bc\/\s*(\d+)\b", t)
    if m:
        try:
            return "LOTE", int(m.group(1))
        except Exception:
            return "LOTE", None
    if "kit" in t or "jogo" in t or "conjunto" in t:
        return "KIT", None
    return "UNIDADE", 1

def infer_units(specs: Dict[str,str], title: str, desc: str) -> Tuple[Optional[str], Optional[int]]:
    for k, v in specs.items():
        nk = normalize(k)
        if "UNIDADES" in nk or "UND" in nk or "PECAS" in nk or "PECA" in nk or "CONTEUD" in nk or "QUANT" in nk:
            m = re.search(r"\d+", v.replace(".", " "))
            if m:
                try:
                    return "LOTE", int(m.group(0))
                except Exception:
                    pass
            st, un = infer_units_from_text(v)
            if st or un:
                return st, un
    st, un = infer_units_from_text((title or "") + " " + (desc or ""))
    return st, un

async def parse_product_page(page: Page, url: str, query: str, searched_part: Optional[str], searched_brand: Optional[str]) -> Product:
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    try:
        title = await page.text_content("h1.ui-pdp-title") or await page.get_attribute("meta[property='og:title']", "content")
    except Exception:
        title = None
    price, currency = await parse_price_and_currency(page)
    price_num = _parse_price_to_float(price)
    seller = None
    for sel in ("a#seller-profile-link", "a.ui-pdp-seller__link-trigger", "a[data-testid='store-link']"):
        try:
            seller = await page.text_content(sel)
            if seller:
                seller = seller.strip()
                break
        except Exception:
            continue
    specs = await parse_specs_table(page)
    desc_text = await parse_description_text(page)
    try:
        scripts = await page.query_selector_all("script[type='application/ld+json']")
        blobs = []
        for s in scripts:
            try:
                blobs.append(await s.inner_text())
            except Exception:
                continue
    except Exception:
        blobs = []
    mpn_jsonld, brand_jsonld = extract_mpn_from_jsonld(blobs)
    matched_part = extract_first_match_from_specs(specs, PART_KEY_RE) or mpn_jsonld
    if not matched_part and searched_part:
        snippet = find_ref_in_text((title or "") + "\n" + desc_text, searched_part)
        if snippet:
            matched_part = snippet
    matched_brand = extract_first_match_from_specs(specs, BRAND_KEY_RE) or brand_jsonld
    part_match = False
    brand_match = False
    if searched_part and matched_part:
        part_match = normalize(searched_part) == normalize(matched_part)
    if searched_brand and matched_brand:
        brand_match = normalize(searched_brand) == normalize(matched_brand)
    sale_type, units = infer_units(specs, title or "", desc_text or "")
    price_per_unit = None
    if price_num is not None and units and units > 0:
        price_per_unit = price_num / units
    return Product(
        query=query,
        searched_part=searched_part,
        searched_brand=searched_brand,
        url=url,
        title=(title or "").strip() or None,
        price=(price or "").strip() or None,
        currency=(currency or "").strip() or None,
        price_num=price_num,
        seller=seller,
        matched_part=matched_part,
        matched_brand=matched_brand,
        part_match=part_match,
        brand_match=brand_match,
        sale_type=sale_type,
        units=units,
        price_per_unit=price_per_unit,
        specs=specs,
        desc_excerpt=(desc_text or "").strip()[:1000] or None,
    )

async def scrape_query(browser: Browser, query: str, searched_part: Optional[str], searched_brand: Optional[str], limit: int, concurrency: int) -> List[Product]:
    page = await browser.new_page()
    try:
        urls = await collect_search_result_urls(page, query, limit=limit)
    finally:
        await page.close()
    sem = asyncio.Semaphore(concurrency)
    results: List[Product] = []
    async def worker(u: str):
        async with sem:
            p = await browser.new_page()
            try:
                prod = await parse_product_page(p, u, query, searched_part, searched_brand)
                results.append(prod)
            except Exception as e:
                sys.stderr.write(f"[warn] Falha ao processar {u}: {e}\n")
            finally:
                await p.close()
    await asyncio.gather(*(worker(u) for u in urls))
    return results

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

def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Scraper de peças no Mercado Livre (Brasil).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--query", help="Consulta livre (ex.: 'amortecedor capo cofap 16570').")
    group.add_argument("--parts-file", help="Arquivo .txt com um Número de peça por linha.")
    group.add_argument("--parts", nargs="+", help="Lista de números de peça no CLI.")
    group.add_argument("--catalog-csv", help="CSV com colunas referencia/ref/pn e marca/brand/fabricante (opcionalmente query).")
    ap.add_argument("--ref-col", default="", help="Nome da coluna de referência no CSV (se vazio, autodetect).")
    ap.add_argument("--brand-col", default="", help="Nome da coluna de marca no CSV (se vazio, autodetect).")
    ap.add_argument("--query-col", default="", help="Nome da coluna de query no CSV (se vazio, monta a partir de ref+marca).")
    ap.add_argument("--limit", type=int, default=24, help="Máximo de cards por consulta.")
    ap.add_argument("--concurrency", type=int, default=4, help="Abas em paralelo.")
    ap.add_argument("--out", default="resultados_ml.csv", help="Arquivo CSV de saída.")
    ap.add_argument("--strict-part-match", action="store_true", help="Quando buscar por peça, manter só anúncios com PN igual ao procurado.")
    ap.add_argument("--strict-brand-match", action="store_true", help="Manter só anúncios com marca igual à procurada.")
    ap.add_argument("--headful", action="store_true", help="Abrir navegador visível (debug).")
    return ap

def _autodetect_cols(header: List[str], wanted: List[str]) -> Optional[str]:
    hnorm = [normalize(h) for h in header]
    wanted_norm = [normalize(w) for w in wanted]
    for i, h in enumerate(hnorm):
        for w in wanted_norm:
            if w and w in h:
                return header[i]
    return None

def _read_catalog_csv(path: str, ref_col: str, brand_col: str, query_col: str) -> List[Tuple[str, Optional[str], Optional[str]]]:
    out: List[Tuple[str, Optional[str], Optional[str]]] = []
    with open(path, "r", encoding="utf-8-sig") as f:
        rd = csv.DictReader(f)
        header = rd.fieldnames or []
        rc = ref_col or _autodetect_cols(header, ["referencia", "ref", "pn", "partnumber", "codigo", "codigo_fabricante", "mpn"])
        bc = brand_col or _autodetect_cols(header, ["marca", "brand", "fabricante"])
        qc = query_col or _autodetect_cols(header, ["query", "consulta", "busca"])
        for row in rd:
            part = (row.get(rc) or "").strip() if rc else ""
            brand = (row.get(bc) or "").strip() if bc else ""
            if qc and (row.get(qc) or "").strip():
                q = (row.get(qc) or "").strip()
            else:
                tokens = [t for t in [part, brand] if t]
                q = " ".join(tokens)
            if q:
                out.append((q, part or None, brand or None))
    return out

def _generate_queries_from_parts(parts: List[str]) -> List[Tuple[str, Optional[str], Optional[str]]]:
    out: List[Tuple[str, Optional[str], Optional[str]]] = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        out.append((p, p, None))
    return out

async def main_async(args) -> int:
    if async_playwright is None:
        print("Instale as dependências: pip install playwright && playwright install chromium", file=sys.stderr)
        return 2
    queries: List[Tuple[str, Optional[str], Optional[str]]] = []
    if args.query:
        queries.append((args.query, None, None))
    elif args.parts_file:
        with open(args.parts_file, "r", encoding="utf-8") as f:
            parts = [line.strip() for line in f if line.strip()]
            queries.extend(_generate_queries_from_parts(parts))
    elif args.parts:
        parts = [p for p in args.parts if p.strip()]
        queries.extend(_generate_queries_from_parts(parts))
    elif args.catalog_csv:
        queries.extend(_read_catalog_csv(args.catalog_csv, args.ref_col, args.brand_col, args.query_col))
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=not args.headful)
        try:
            all_rows: List[Product] = []
            for q, searched_part, searched_brand in queries:
                rows = await scrape_query(browser, q, searched_part, searched_brand, limit=args.limit, concurrency=args.concurrency)
                if args.strict_part_match and searched_part:
                    rows = [r for r in rows if r.part_match]
                if args.strict_brand_match and searched_brand:
                    rows = [r for r in rows if r.brand_match]
                all_rows.extend(rows)
            write_csv(all_rows, args.out)
            print(f"✅ Salvo: {args.out} ({len(all_rows)} linhas)")
        finally:
            await browser.close()
    return 0

def main(argv=None) -> int:
    ap = build_parser()
    args = ap.parse_args(argv)
    return asyncio.run(main_async(args))

if __name__ == "__main__":
    raise SystemExit(main())
