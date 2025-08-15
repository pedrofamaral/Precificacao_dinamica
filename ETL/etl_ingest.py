"""
ETL ÚNICO (3 em 1) + LIMPEZA:
- Lê JSON, CSV (em --raw_dir) e SQLite (em --sqlite_dir).
- Concatena e normaliza os registros (heterogeneidade de scrapers).
- Grava tabela BRUTA: market_items (SQLite, append, idempotente).
- Limpa/Padroniza (marketplace, title, captured_at, sku_norm, currency).
- Deduplica (exato -> (marketplace,url) -> (marketplace,title,price) mantendo o mais recente).
- Cria NOME CANÔNICO por (marketplace, sku_norm).
- Gera tabelas finais no mesmo SQLite:
    * market_items_clean (replace)
    * unifier_input (replace)
    * products_dim (replace)
- Salva snapshots em arquivo (Parquet; fallback para CSV).

Execução típica:
    python -m pricing_mvp.etl_ingest --raw_dir ".\\PriceMonitor\\MercadoLivre\\data\\raw"
    python -m pricing_mvp.etl_ingest --raw_dir ".\\PriceMonitor\\MagazineLuiza\\data\\raw"
    python -m pricing_mvp.etl_ingest --raw_dir ".\\PriceMonitor\\pneustore\\dados\\raw"
    # depois disso, o banco já terá market_items_clean e unifier_input prontos
"""
from __future__ import annotations

if __package__ is None or __package__ == "":
    import os as _os, sys as _sys
    _sys.path.append(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))

import argparse
import os
import re
import sqlite3
from pathlib import Path
from typing import Dict, Any, Iterable, List, Optional
from urllib.parse import urlparse, unquote, parse_qs

import numpy as np
import pandas as pd

try:
    from .common import (
        SETTINGS, logger, ensure_dirs, iter_files, load_json_lines, norm_sku,
        to_sql, read_sql, exec_sql, seen, mark_seen, file_fingerprint
    )
except ImportError:
    from ETL.common import (
        SETTINGS, logger, ensure_dirs, iter_files, load_json_lines, norm_sku,
        to_sql, read_sql, exec_sql, seen, mark_seen, file_fingerprint
    )

# ============================================================
# Helpers gerais
# ============================================================

GENERIC_TOKENS = {"p","produto","products","product","click","clicks","count","item"}

def to_float(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    s = s.replace("R$", "").replace("$", "").replace("€", "").strip()
    s = s.replace(".", "")
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None

def _save_parquet_with_fallback(df: pd.DataFrame, out_path: Path, csv_name: str):
    try:
        df.to_parquet(out_path, index=False)
        logger.info("Parquet salvo em %s", out_path)
    except Exception as e:
        backup = out_path.with_name(csv_name)
        df.to_csv(backup, index=False, encoding="utf-8")
        logger.warning("Sem engine parquet (pyarrow/fastparquet). Salvei CSV em %s. Erro: %s", backup, e)

def unwrap_ml_click(url: str | None) -> str | None:
    if not url or not isinstance(url, str):
        return url
    try:
        p = urlparse(url)
        host = p.netloc.lower()
        if host.startswith("click") and "mercadolivre.com" in host:
            q = parse_qs(p.query)
            for key in ("url","u","redirect","redirectUrl","dest","go","to"):
                if key in q and q[key]:
                    return unquote(q[key][0])
        return url
    except Exception:
        return url

def name_from_url(url: str | None) -> str | None:
    if not url or not isinstance(url, str):
        return None
    try:
        u = unwrap_ml_click(url)
        p = urlparse(u)
        seg = [s for s in unquote(p.path).split("/") if s]
        if not seg:
            return None
        last = seg[-1]
        if re.fullmatch(r"(ML[BALMCUV]-?\d+)", last, flags=re.I) and len(seg) >= 2:
            last = seg[-2]
        if last.lower() in ("p","produto","products","product") and len(seg) >= 2:
            last = seg[-2]
        last = re.sub(r"\.(html?|php|aspx|jsp|json|xml|jm)$", "", last, flags=re.I).replace("_JM", "")
        last = re.sub(r"^(ML[BALMCUV]-?\d+)", "", last, flags=re.I).lstrip("-_")
        s = re.sub(r"[-_]+", " ", last)
        s = re.sub(r"\b(oficial|original|novo|usado|frete|gratis|grátis)\b", "", s, flags=re.I)
        s = re.sub(r"\s+", " ", s).strip().lower()
        if not s or s in GENERIC_TOKENS or len(s) < 4:
            return None
        return s.title()
    except Exception:
        return None

def name_from_query(query: str | None) -> str | None:
    """Extrai nome legível do nome do arquivo de busca (ex.: pneu-185-60-r14-dunlop_20250808_101010.json)."""
    if not query or not isinstance(query, str):
        return None
    m = re.match(r"(.+?)(?:_\d{8}_\d{6})?\.(json|csv|txt)$", query, flags=re.I)
    base = m.group(1) if m else query
    s = re.sub(r"[-_]+", " ", base)
    s = re.sub(r"\s+", " ", s).strip().lower()
    if not s or s in GENERIC_TOKENS or len(s) < 4:
        return None
    return s.title()

def infer_marketplace_from_url(url_unwrapped: str | None) -> str | None:
    if not url_unwrapped or not isinstance(url_unwrapped, str):
        return None
    try:
        host = urlparse(url_unwrapped).netloc.lower()
    except Exception:
        return None
    if "mercadolivre.com" in host:
        return "mercadolivre"
    if "magazineluiza.com.br" in host or "magalu.com" in host:
        return "magalu"
    if "pneustore.com.br" in host:
        return "pneustore"
    return host.split(":")[0] if host else None

def parse_captured_from_query(q: str | None):
    if not q or not isinstance(q, str):
        return pd.NaT
    m = re.search(r"_(\d{8})_(\d{6})", q)
    if not m:
        return pd.NaT
    d, t = m.group(1), m.group(2)
    try:
        return pd.to_datetime(f"{d} {t}", format="%Y%m%d %H%M%S")
    except Exception:
        return pd.NaT

# ============================================================
# Ingestão heterogênea (JSON/CSV/SQLite)
# ============================================================

def normalize_record(raw: Dict[str, Any], meta: Dict[str, Any]) -> Dict[str, Any]:
    def pick(*keys):
        for k in keys:
            if k in raw and raw[k] not in (None, "", [], {}):
                return raw[k]
        return None

    title = pick("title","product_title","productTitle","name","nome","titulo","Title")
    price = pick("price","preco","product_price","valor","salePrice","sellingPrice","bestPrice","final_price","Price")
    seller = pick("seller","sellerName","seller_name","loja","store","merchant","vendor")
    url = pick("url","link","product_url","productUrl","urlProduto","url_produto","href","Url")
    sku = pick("sku","product_id","productId","code","identifier","id","Id")
    stock = pick("stock","estoque","quantity","availableQuantity","available_quantity")
    location = pick("location","cidade","city")
    currency = pick("currency","moeda","currency_id","Currency") or "BRL"
    condition = pick("condition","condicao","Condition")
    shipping = pick("shipping_price","frete","shipping","frete_price")

    captured_at = pick("captured_at","scraped_at") or meta.get("captured_at")

    return {
        "source": meta.get("source", "unknown"),
        "marketplace": meta.get("marketplace", meta.get("source", "unknown")),
        "query": meta.get("query"),
        "title": title,
        "sku_text": sku or title,
        "sku_norm": norm_sku(sku or title or ""),
        "price": to_float(price),
        "currency": currency,
        "condition": condition,
        "seller": seller,
        "url": url,
        "shipping_price": to_float(shipping),
        "stock": stock if isinstance(stock, (int, float)) else None,
        "location": location,
        "captured_at": captured_at,
    }


def meta_from_path(p: Path) -> Dict[str, str]:
    parts = p.parts
    marketplace = "unknown"
    if "raw" in parts:
        idx = parts.index("raw")
        if idx + 1 < len(parts):
            marketplace = parts[idx + 1]
    query = p.name
    ts = parse_captured_from_query(query)
    captured_at = ts.isoformat() if pd.notna(ts) else None

    m_low = str(marketplace).lower()
    if "magazineluiza" in m_low or "magalu" in m_low or "magazine" in m_low:
        marketplace = "magalu"
    elif "mercado" in m_low and "livre" in m_low:
        marketplace = "mercadolivre"
    elif "pneustore" in m_low:
        marketplace = "pneustore"

    return {"source": marketplace, "marketplace": marketplace, "query": query, "captured_at": captured_at}

def ingest_json() -> List[Dict[str, Any]]:
    rows = []
    for p in iter_files(SETTINGS.raw_dir, (".json",)):
        fid = "json:" + file_fingerprint(p)
        if seen("market_items", fid):
            logger.debug("PULANDO JSON já visto: %s", p)
            continue
        meta = meta_from_path(p)
        try:
            items = load_json_lines(p)
        except Exception as e:
            logger.exception("Falha lendo JSON %s: %s", p, e)
            continue
        for it in items:
            rows.append(normalize_record(it, meta))
        mark_seen("market_items", fid)
    return rows

def ingest_csv() -> List[Dict[str, Any]]:
    rows = []
    for p in iter_files(SETTINGS.raw_dir, (".csv",)):
        fid = "csv:" + file_fingerprint(p)
        if seen("market_items", fid):
            logger.debug("PULANDO CSV já visto: %s", p)
            continue
        meta = meta_from_path(p)
        try:
            df = pd.read_csv(p)
        except Exception as e:
            logger.exception("Falha lendo CSV %s: %s", p, e)
            continue
        for rec in df.to_dict(orient="records"):
            rows.append(normalize_record(rec, meta))
        mark_seen("market_items", fid)
    return rows

def ingest_sqlite() -> List[Dict[str, Any]]:
    rows = []
    sdir = SETTINGS.sqlite_dir
    if not sdir.exists():
        return rows
    for db in sdir.glob("*.db"):
        fid = f"sqlite:{db}:{db.stat().st_size}:{int(db.stat().st_mtime)}"
        if seen("market_items", fid):
            logger.debug("PULANDO SQLite já visto: %s", db)
            continue
        import sqlite3 as _sq
        try:
            conn = _sq.connect(db)
            cur = conn.cursor()
            tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
            preferred = [t for t in tables if t.lower() in ("items","products","market_prices","scraped")]
            if preferred:
                pick = preferred[0]
            else:
                pick = None
                for t in tables:
                    cols = [r[1] for r in cur.execute(f"PRAGMA table_info('{t}')").fetchall()]
                    if any(c.lower() in ("price","preco") for c in cols) and any(c.lower() in ("title","name","nome") for c in cols):
                        pick = t; break
                if not pick and tables:
                    pick = tables[0]
            df = pd.read_sql_query(f"SELECT * FROM {pick}", conn)
        except Exception as e:
            logger.exception("Falha lendo %s: %s", db, e)
            try: conn.close()
            except Exception: pass
            continue
        finally:
            try: conn.close()
            except Exception: pass

        marketplace = db.stem
        meta = {"source": marketplace, "marketplace": marketplace, "query": None, "captured_at": None}
        for rec in df.to_dict(orient="records"):
            rows.append(normalize_record(rec, meta))
        mark_seen("market_items", fid)
    return rows

# ============================================================
# Limpeza / Padronização / Dedup / Canonical Name
# ============================================================

def clean_and_snapshot(all_rows_df: pd.DataFrame):
    if not all_rows_df.empty:
        to_sql(all_rows_df, "market_items", if_exists="append", index=False)

    full = read_sql("SELECT * FROM market_items")
    if full.empty:
        logger.info("market_items está vazio após ingestão.")
        return

    full["url_unwrapped"] = full["url"].map(unwrap_ml_click)

    mp = full["url_unwrapped"].map(infer_marketplace_from_url)
    full["marketplace"] = np.where(
        full["marketplace"].isna() | (full["marketplace"] == full.get("source")),
        mp,
        full["marketplace"]
    )

    t_url = full["url_unwrapped"].map(name_from_url)
    t_query = full["query"].map(name_from_query)
    full["title"] = full["title"].fillna(t_url)
    full["title"] = full["title"].fillna(t_query)

    full["price"] = pd.to_numeric(full["price"], errors="coerce")

    full["sku_norm"] = np.where(
        (full["sku_norm"].isna()) | (full["sku_norm"].astype(str).str.len() == 0),
        full["title"].map(lambda x: re.sub(r"[^0-9A-Za-z]+", "-", str(x).upper()).strip("-") if pd.notna(x) else None),
        full["sku_norm"]
    )

    full["currency"] = full["currency"].fillna("BRL")

    parsed = full["query"].map(parse_captured_from_query)
    full["captured_at"] = pd.to_datetime(full.get("captured_at"), errors="coerce")
    full.loc[full["captured_at"].isna(), "captured_at"] = parsed
    full["captured_date"] = full["captured_at"].dt.date.astype("string")

    mask_ess = (
        (~full["price"].isna()) & (full["price"] > 0) &
        (~full["url_unwrapped"].isna()) &
        (~full["title"].isna()) &
        (~full["marketplace"].isna())
    )
    clean = full.loc[mask_ess].copy()

    clean["url"] = clean["url_unwrapped"]
    clean.drop(columns=["url_unwrapped"], inplace=True)

    clean.sort_values(["marketplace", "url", "captured_at"], inplace=True)
    clean = clean[~clean.duplicated()]  # exato
    clean = clean[~clean.duplicated(subset=["marketplace", "url"], keep="last")]  # por URL
    clean = clean[~clean.duplicated(subset=["marketplace", "title", "price"], keep="last")]  # por título+preço

    canon = clean.groupby(["marketplace", "sku_norm"], dropna=False)["title"] \
                 .agg(lambda s: s.value_counts().index[0] if len(s) > 0 else None) \
                 .rename("product_name").reset_index()
    clean = clean.merge(canon, on=["marketplace", "sku_norm"], how="left")

    to_sql(clean, "market_items_clean", if_exists="replace", index=False)
    snap = clean[
        clean.groupby(["marketplace", "sku_norm"])["captured_at"].transform("max") == clean["captured_at"]
    ].drop_duplicates(subset=["marketplace", "sku_norm"])
    to_sql(snap, "unifier_input", if_exists="replace", index=False)
    to_sql(canon, "products_dim", if_exists="replace", index=False)

    ensure_dirs()
    _save_parquet_with_fallback(clean, SETTINGS.processed_dir / "market_items_clean.parquet", "market_items_clean.csv")
    _save_parquet_with_fallback(snap,  SETTINGS.processed_dir / "unifier_input.parquet",     "unifier_input.csv")

    try:
        miss = full.loc[~mask_ess].copy()
        miss["miss_reason"] = np.select(
            [
                miss["url_unwrapped"].isna(),
                miss["title"].isna(),
                miss["price"].isna() | (miss["price"] <= 0),
                miss["marketplace"].isna(),
            ],
            ["missing_url","missing_title","bad_price","missing_marketplace"],
            default="other"
        )
        diag = miss.assign(marketplace=miss["marketplace"].fillna("unknown"))
        tops = diag.groupby("marketplace")["miss_reason"].value_counts().sort_values(ascending=False).head(12)
        logger.info("Top motivos de descarte (amostra):\n%s", tops.to_string())
        logger.info("market_items_clean por marketplace:\n%s", clean["marketplace"].value_counts().to_string())
    except Exception as _e:
        logger.debug("diagnóstico de descarte falhou: %s", _e)

    logger.info("Limpeza ok → market_items_clean=%d, unifier_input=%d", len(clean), len(snap))


# ============================================================
# Pipeline principal
# ============================================================

def main():
    ensure_dirs()
    all_rows: List[Dict[str, Any]] = []
    all_rows += ingest_json()
    all_rows += ingest_csv()
    all_rows += ingest_sqlite()

    if not all_rows:
        logger.info("Nenhum dado novo para ingerir (idempotência). Ainda assim vou reconstruir o snapshot limpo a partir do banco.")
        dummy = pd.DataFrame()
        clean_and_snapshot(dummy)
        return

    df_new = pd.DataFrame(all_rows)

    if "captured_at" in df_new.columns:
        pass

    _save_parquet_with_fallback(df_new, SETTINGS.processed_dir / "market_items_new.parquet", "market_items_new.csv")

    clean_and_snapshot(df_new)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw_dir", help="Pasta de JSON/CSV (default: pricing_mvp/data/raw)")
    ap.add_argument("--sqlite_dir", help="Pasta com .db (default: pricing_mvp/data/sqlite)")
    args = ap.parse_args()
    if args.raw_dir:
        from .common import SETTINGS as S
        S.raw_dir = Path(args.raw_dir)
    if args.sqlite_dir:
        from .common import SETTINGS as S
        S.sqlite_dir = Path(args.sqlite_dir)
    main()
