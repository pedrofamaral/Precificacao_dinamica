"""
unifier.py — agrupa ofertas equivalentes em um produto unificado
Lê:
  - unifier_input (preferencial) OU market_items_clean (fallback) no SQLite
Escreve:
  - unified_products (replace)
  - unified_offers (replace)
  - unify_stats (replace)
Chave:
  - Assinatura estável por (brand, size, model_norm), com fallback se faltar info.
Execução:
  python -m pricing_mvp.unifier
"""
from __future__ import annotations

# bootstrap p/ rodar como módulo (-m) ou script
if __package__ is None or __package__ == "":
    import os as _os, sys as _sys
    _sys.path.append(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))

import re
import hashlib
import sqlite3
from dataclasses import dataclass
from typing import Optional, Tuple, List
from urllib.parse import urlparse, unquote

import numpy as np
import pandas as pd

try:
    from .common import SETTINGS, logger, read_sql, to_sql, ensure_dirs
except ImportError:
    from pricing_mvp.common import SETTINGS, logger, read_sql, to_sql, ensure_dirs

# --------------------- helpers ---------------------

BRANDS = {
    "dunlop","goodyear","pirelli","firestone","michelin","continental","bridgestone",
    "falken","sumitomo","yokohama","kumho","hankook","maxxis","achilles","linglong",
    "triangle","sailun","goodride","cooper","bf goodrich","bfgoodrich","general",
    "bf-goodrich","marshal","nexen","aptany","roadone","giti","zeetex","aurora"
}
GENERIC = {"pneu","aro","radial","tubeless","runflat","xl","tl","t","r","pro","sport","touring","city","street"}

SIZE_RE = re.compile(r"(?P<w>\d{3})\s*[\/ ]\s*(?P<a>\d{2})\s*[Rr]\s*(?P<r>\d{2})")
LOAD_SPEED_RE = re.compile(r"\b(\d{2,3})\s*[A-Z]{1,2}\b")  # ex: 88T, 79H, 102V
MODEL_CLEAN_RE = re.compile(r"[^0-9a-zA-Z]+")

def normalize_text(s: Optional[str]) -> str:
    if not isinstance(s, str):
        return ""
    s = s.strip()
    return s

def extract_size(text: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Retorna (width, aspect, rim, size_str ex.: '185/60R14') se encontrado.
    """
    if not text:
        return None, None, None, None
    m = SIZE_RE.search(text.replace("-", " ").replace("_", " ").replace(".", " "))
    if not m:
        return None, None, None, None
    w, a, r = m.group("w"), m.group("a"), m.group("r")
    return w, a, r, f"{w}/{a}R{r}"

def detect_brand(text: str) -> Optional[str]:
    if not text:
        return None
    low = text.lower()
    for b in sorted(BRANDS, key=len, reverse=True):
        if b in low:
            return b.replace("-", " ")
    return None

def normalize_model(text: str, brand: Optional[str], size_str: Optional[str]) -> str:
    """
    Remove marca e tamanho do texto, limpa tokens muito genéricos, mantém
    códigos de modelo (ex.: fm800, dz102, p7, primacy 4, wrangler territory).
    """
    if not text:
        return ""
    t = text.lower()
    if size_str:
        t = t.replace(size_str.lower(), " ")
        t = t.replace(size_str.lower().replace("/", " "), " ")
    # tira variantes tipo "185 60 r14"
    t = SIZE_RE.sub(" ", t)
    # tira índice de carga/velocidade
    t = LOAD_SPEED_RE.sub(" ", t)
    if brand:
        t = t.replace(brand.lower(), " ")
    # limpeza de separadores
    t = MODEL_CLEAN_RE.sub(" ", t)
    tokens = [tok for tok in t.split() if tok and tok not in GENERIC]
    # mantém tokens alfanuméricos pequenos relevantes (p7, fm800, dz102)
    model = " ".join(tokens).strip()
    return model

def product_signature(brand: Optional[str], size_str: Optional[str], model: str, fallback_key: str) -> str:
    base = f"{(brand or 'unk').strip()}|{(size_str or 'unk').strip()}|{model.strip() or 'unk'}"
    # se ficou muito fraco, usa fallback (sku_norm/title)
    if base.count("unk") >= 2 and fallback_key:
        base = f"fb|{fallback_key.lower()}"
    return re.sub(r"\s+", " ", base)

def stable_id(sig: str) -> str:
    return hashlib.sha1(sig.encode("utf-8")).hexdigest()[:16]

# --------------------- core ------------------------

def load_source_df() -> pd.DataFrame:
    # preferir unifier_input; senão, market_items_clean
    try:
        df = read_sql("SELECT * FROM unifier_input")
        if not df.empty:
            logger.info("Lendo de unifier_input (%d linhas).", len(df))
            return df
    except Exception:
        pass
    df = read_sql("SELECT * FROM market_items_clean")
    logger.info("Lendo de market_items_clean (%d linhas).", len(df))
    return df

def unify():
    df = load_source_df()
    if df.empty:
        logger.warning("Nada para unificar.")
        return

    # campos mínimos
    for c in ("marketplace","title","price","url","sku_norm","captured_at"):
        if c not in df.columns:
            df[c] = np.nan

    # extrair features
    df["title"] = df["title"].astype(str)
    df["title_norm"] = df["title"].str.strip()

    # tenta extrair do title; se não achar, tenta do sku_norm
    w, a, r, s = zip(*df["title_norm"].map(extract_size))
    df["w"], df["a"], df["r"], df["size_str"] = list(w), list(a), list(r), list(s)

    missing_size = df["size_str"].isna()
    if missing_size.any():
        w2, a2, r2, s2 = zip(*df.loc[missing_size, "sku_norm"].fillna("").map(extract_size))
        df.loc[missing_size, "w"] = w2
        df.loc[missing_size, "a"] = a2
        df.loc[missing_size, "r"] = r2
        df.loc[missing_size, "size_str"] = s2

    df["brand"] = df["title_norm"].map(detect_brand)
    miss_brand = df["brand"].isna() & df["sku_norm"].notna()
    df.loc[miss_brand, "brand"] = df.loc[miss_brand, "sku_norm"].map(detect_brand)

    df["model_norm"] = [
        normalize_model(t, b, s) for t, b, s in zip(df["title_norm"], df["brand"], df["size_str"])
    ]

    fb_key = df["sku_norm"].fillna(df["title_norm"]).str.replace(r"[^0-9a-zA-Z]+", " ", regex=True).str.strip()
    df["product_signature"] = [
        product_signature(b, s, m, f) for b, s, m, f in zip(df["brand"], df["size_str"], df["model_norm"], fb_key)
    ]
    df["product_id"] = df["product_signature"].map(stable_id)

    prod = df.groupby("product_id").agg(
        product_signature=("product_signature","first"),
        brand=("brand", lambda s: s.dropna().iloc[0] if s.dropna().size else None),
        size_str=("size_str", lambda s: s.dropna().iloc[0] if s.dropna().size else None),
        model_norm=("model_norm","first"),
        example_title=("title","first"),
        n_offers=("url","nunique"),
        first_seen=("captured_at","min"),
        last_seen=("captured_at","max"),
    ).reset_index()

    keep_cols = ["product_id","marketplace","title","price","seller","url","sku_norm","captured_at","currency","condition","shipping_price","stock","location"]
    for c in keep_cols:
        if c not in df.columns:
            df[c] = np.nan
    offers = df[keep_cols].copy()

    to_sql(prod, "unified_products", if_exists="replace", index=False)
    to_sql(offers, "unified_offers", if_exists="replace", index=False)

    stats = pd.DataFrame([{
        "rows_in": int(len(df)),
        "products_out": int(len(prod)),
        "offers_out": int(len(offers)),
        "avg_offers_per_product": float(len(offers) / max(1, len(prod))),
    }])
    to_sql(stats, "unify_stats", if_exists="replace", index=False)

    ensure_dirs()
    try:
        prod.to_parquet(SETTINGS.processed_dir / "unified_products.parquet", index=False)
        offers.to_parquet(SETTINGS.processed_dir / "unified_offers.parquet", index=False)
    except Exception:
        prod.to_csv(SETTINGS.processed_dir / "unified_products.csv", index=False, encoding="utf-8")
        offers.to_csv(SETTINGS.processed_dir / "unified_offers.csv", index=False, encoding="utf-8")

    logger.info("Unifier: %d ofertas → %d produtos (≈ %.2f ofertas/produto)",
                len(offers), len(prod), len(offers)/max(1,len(prod)))

def main():
    unify()

if __name__ == "__main__":
    main()
