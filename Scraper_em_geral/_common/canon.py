import hashlib, re, urllib.parse, datetime

def now_utc_iso():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def make_listing_id(url: str) -> str:
    u = urllib.parse.urlparse(url)
    base = f"{u.netloc}|{u.path}|{u.params}|{u.query}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]

SIZE_RE = re.compile(r"\b(?P<w>\d{3})[\/\s-]?(?P<a>\d{2})\s*R?\s*(?P<r>\d{2})\b", re.I)

def extract_size(text: str):
    m = SIZE_RE.search(text or "")
    if not m: return None
    w, a, r = int(m.group("w")), int(m.group("a")), int(m.group("r"))
    return {"width": w, "aspect": a, "rim": r, "size_norm": f"{w} {a} r{r}"}

def to_canonical(raw: dict, marketplace: str, cod_prod: str, run_id: str) -> dict:
    """raw precisa ter pelo menos: url, title, price (opcional promo_price, currency, brand, model, availability, seller, seller_id)"""
    url = raw["url"]
    lid = raw.get("listing_id") or make_listing_id(url)
    title = (raw.get("title") or "").strip()
    sz = extract_size(title + " " + (raw.get("extra_text") or ""))
    price = raw.get("promo_price") or raw.get("price")
    doc = {
        "cod_prod": cod_prod,
        "marketplace": marketplace,
        "listing_id": lid,
        "url": url,
        "title": title,
        "price": float(price) if price is not None else None,
        "promo_price": float(raw.get("promo_price")) if raw.get("promo_price") else None,
        "currency": raw.get("currency", "BRL"),
        "seller": raw.get("seller"),
        "seller_id": raw.get("seller_id"),
        "availability": raw.get("availability", "unknown"),
        "brand": (raw.get("brand") or "").upper().strip() or None,
        "model": (raw.get("model") or "").upper().strip() or None,
        "observed_at": raw.get("observed_at") or now_utc_iso(),
        "run_id": run_id,
    }
    if sz:
        doc.update(sz)
        doc["size_regex_hit"] = True
    return doc
