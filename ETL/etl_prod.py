# -*- coding: utf-8 -*-
"""
ETL ÚNICO (fusão S1+S2) para marketplaces de pneus/produtos
-----------------------------------------------------------
- Ingestão heterogênea: JSON, CSV, SQLite e pastas dos scrapers.
- Idempotência: evita reprocessar arquivos já vistos (seen/mark_seen).
- Normalização forte:
    * Usa `cod_prod` como chave confiável quando disponível (S1).
    * `sku_norm` para fallback/chave híbrida entre fontes (S2).
    * `size_norm` e colunas derivadas vindas de parsing externo `tire_size.py`.
- Deduplicação em 3 camadas (mais recente): exato → (marketplace,url) →
  (marketplace,title,price).
- Canonical name por (marketplace, sku_norm) e/ou por `cod_prod` quando existir.
- Exporta:
    * market_items_clean (replace)
    * unifier_input (replace)
    * products_dim (replace)
  em SQLite + snapshots Parquet (fallback CSV).

Execução típica:
    python -m ETL.etl_ingest --raw_dir "path/para/dados" --sqlite_dir "path/para/dbs" --out_db "data/processed/warehouse.db"
"""
from __future__ import annotations

if __package__ is None or __package__ == "":
    import os as _os, sys as _sys
    _sys.path.append(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))

import sqlite3 as _sq
import json
import argparse
import re
import sqlite3
import unicodedata
from pathlib import Path
from typing import Dict, Any, List, Optional
from urllib.parse import urlparse, unquote, parse_qs
from ETL.tire_size import extract_tire_size_from_title, extract_brand, extract_model, enrich_with_parsed_fields
import numpy as np
import pandas as pd

try:
    from ETL.common import (
        SETTINGS, logger, ensure_dirs, iter_files, load_json_lines, norm_sku,
        to_sql, read_sql, seen, mark_seen, file_fingerprint
    )
except ImportError:
    from .common import (
        SETTINGS, logger, ensure_dirs, iter_files, load_json_lines, norm_sku,
        to_sql, read_sql, seen, mark_seen, file_fingerprint
    )



GENERIC_TOKENS = {"p","produto","products","product","click","clicks","count","item"}

SCRAPER_ROOT = Path(__file__).resolve().parents[2] / "Scraper_em_geral"
MARKET_PATHS = {
    "mercadolivre": [SCRAPER_ROOT / "mercadolivre"    / "data"  / "raw"],
    "magalu":       [SCRAPER_ROOT / "MagazineLuiza"   / "data"  / "raw"],
    "pneustore":    [SCRAPER_ROOT / "pneustore"       / "dados" / "raw"],
}

SPEED_RE = re.compile(r"\b\d{2,3}[A-Z]{1,2}\b", re.I)

# ============================================================
# Funções utilitárias
# ============================================================
def to_float(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    s = s.replace("R$", "").replace("$", "").replace("€", "").strip()
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None

def _nfkd(s: str) -> str:
    return unicodedata.normalize("NFKD", s or "").encode("ascii","ignore").decode("ascii")

def _norm(s: str) -> str:
    s = _nfkd(str(s or ""))
    s = re.sub(r"[^A-Za-z0-9 /_-]+", " ", s).lower()
    return re.sub(r"\s+", " ", s).strip()

def _save_parquet_with_fallback(df: pd.DataFrame, out_path: Path, csv_name: str):
    try:
        df.to_parquet(out_path, index=False)
        logger.info("💾 Parquet salvo em %s", out_path)
    except Exception as e:
        backup = out_path.with_name(csv_name)
        df.to_csv(backup, index=False, encoding="utf-8")
        logger.warning("⚠️ Sem engine parquet. Salvei CSV em %s. Erro: %s", backup, e)

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
# Ingestão: normalização de registros
# ============================================================
def normalize_record(raw: Dict[str, Any], meta: Dict[str, Any]) -> Dict[str, Any]:
    def pick(*keys):
        for k in keys:
            if k in raw and raw[k] not in (None, "", [], {}):
                return raw[k]
        return None

    cod_prod = pick("cod_prod", "internal_code", "product_id_internal", "codigo_produto")
    title = pick("title","product_title","productTitle","name","nome","titulo","Title")
    price = pick("price","preco","product_price","valor","salePrice","sellingPrice","bestPrice","final_price","Price")
    seller = pick("seller","sellerName","seller_name","loja","store","merchant","vendor")
    url = pick("url","link","product_url","productUrl","urlProduto","url_produto","href","Url")
    sku = pick("sku","listing_id","product_id","productId","code","identifier","id","Id")
    stock = pick("stock","estoque","quantity","availableQuantity","available_quantity")
    location = pick("location","cidade","city")
    currency = pick("currency","moeda","currency_id","Currency", "dinheiro") or "BRL"
    condition = pick("condition","condicao","Condition")
    shipping = pick("shipping_price","frete","shipping","frete_price")
    size_norm = pick("size_norm","size","medida","tamanho", "width")
    observed_at = pick("observed_at","scraped_at","captured_at","data_coleta","data_captura")

    captured_at = pick("captured_at","scraped_at") or meta.get("captured_at")

    return {
        "cod_prod": int(cod_prod) if (cod_prod is not None and str(cod_prod).isdigit()) else None,
        "source": meta.get("source", "unknown"),
        "marketplace": meta.get("marketplace", meta.get("source", "unknown")),
        "query": meta.get("query"),
        "title": title,
        "title_raw": title,
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
        "size_norm": size_norm,  
        "captured_at": captured_at,
        "observed_at": observed_at
    }

# ============================================================
# Ingestão: múltiplas fontes + idempotência
# ============================================================
def meta_from_path(p: Path) -> Dict[str, str]:
    parts_low = [s.lower() for s in p.parts]
    
    if any("mercadolivre" in s for s in parts_low):
        marketplace = "mercadolivre"
    elif any(("magazineluiza" in s) or ("magalu" in s) for s in parts_low):
        marketplace = "magalu"
    elif any("pneustore" in s for s in parts_low):
        marketplace = "pneustore"
    else:
        marketplace = "unknown"
    
    query = p.name
    ts = parse_captured_from_query(query)
    captured_at = ts.isoformat() if pd.notna(ts) else None
    
    return {"source": marketplace, "marketplace": marketplace, "query": query, "captured_at": captured_at}

def iter_input_files():
    exts = {".jsonl", ".json", ".csv"}
    
    for mp, dirs in MARKET_PATHS.items():
        for d in dirs:
            if not d.exists():
                continue
            for p in d.rglob("*"):
                if p.suffix.lower() in exts and p.is_file():
                    yield mp, p

def ingest_scraper_dirs() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    
    for _mp, path in iter_input_files():
        try:
            if path.suffix.lower() == ".jsonl":
                fid = "jsonl:" + file_fingerprint(path)
                if seen("market_items", fid):
                    logger.debug("PULANDO JSONL já visto: %s", path); continue
                meta = meta_from_path(path)
                with path.open("r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line: continue
                        raw = json.loads(line)
                        rows.append(normalize_record(raw, meta))
                mark_seen("market_items", fid)

            elif path.suffix.lower() == ".json":
                fid = "json:" + file_fingerprint(path)
                if seen("market_items", fid):
                    logger.debug("PULANDO JSON já visto: %s", path); continue
                meta = meta_from_path(path)
                data = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(data, list):
                    for raw in data:
                        rows.append(normalize_record(raw, meta))
                elif isinstance(data, dict):
                    rows.append(normalize_record(data, meta))
                mark_seen("market_items", fid)

            elif path.suffix.lower() == ".csv":
                fid = "csv:" + file_fingerprint(path)
                if seen("market_items", fid):
                    logger.debug("PULANDO CSV já visto: %s", path); continue
                meta = meta_from_path(path)
                df = pd.read_csv(path, encoding="utf-8")
                for rec in df.to_dict(orient="records"):
                    rows.append(normalize_record(rec, meta))
                mark_seen("market_items", fid)
        except Exception as e:
            logger.warning("Falha ao ler %s: %s", path, e)
    return rows

def ingest_json() -> List[Dict[str, Any]]:
    rows = []
    
    for p in iter_files(SETTINGS.raw_dir, (".json", ".jsonl")):
        fid = f"{p.suffix.lower().strip('.')}:" + file_fingerprint(p)
        if seen("market_items", fid):
            logger.debug("PULANDO já visto: %s", p); continue
        meta = meta_from_path(p)
        try:
            items = load_json_lines(p) if p.suffix.lower()==".jsonl" else json.loads(p.read_text(encoding="utf-8"))
            if isinstance(items, dict): items = [items]
        except Exception as e:
            logger.exception("Falha lendo JSON %s: %s", p, e); continue
        for it in items:
            rows.append(normalize_record(it, meta))
        mark_seen("market_items", fid)
    return rows

def ingest_csv() -> List[Dict[str, Any]]:
    rows = []
    
    for p in iter_files(SETTINGS.raw_dir, (".csv",)):
        fid = "csv:" + file_fingerprint(p)
        if seen("market_items", fid):
            logger.debug("PULANDO CSV já visto: %s", p); continue
        meta = meta_from_path(p)
        
        try:
            try:
                df = pd.read_csv(p, sep=';', decimal=',')
            except Exception:
                df = pd.read_csv(p)
        except Exception as e:
            logger.exception("Falha lendo CSV %s: %s", p, e); continue
        
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
            logger.debug("PULANDO SQLite já visto: %s", db); continue
        
        try:
            conn = _sq.connect(db)
            cur = conn.cursor()
            tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
            preferred = [t for t in tables if t.lower() in ("items","products","market_prices","scraped")]
            pick = preferred[0] if preferred else None
            if not pick and tables:
                for t in tables:
                    cols = [r[1] for r in cur.execute(f"PRAGMA table_info('{t}')").fetchall()]
                    if any(c.lower() in ("price","preco") for c in cols) and any(c.lower() in ("title","name","nome") for c in cols):
                        pick = t; break
                if not pick:
                    pick = tables[0]
            df = pd.read_sql_query(f"SELECT * FROM {pick}", conn)
        except Exception as e:
            logger.exception("Falha lendo %s: %s", db, e); 
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
# Enriquecimento: pneus (tire_size) + marca/modelo
# ============================================================
def extract_speed_index(text: str) -> str:
    if not text: 
        return ""
    m = SPEED_RE.search(str(text).upper())
    return m.group(0).upper() if m else ""

def clean_speed_tokens(s: str) -> str:
    t = re.sub(r"\(\s*(?:\d{2,3}[A-Z]{1,2})\s*\)", " ", s or "", flags=re.I)
    t = SPEED_RE.sub(" ", t)
    return re.sub(r"\s{2,}", " ", t).strip()

def load_known_lists_from_lote() -> tuple[set, set]:
    candidates = [
        Path(__file__).resolve().parents[2] / "Extracao_e_Insercao" / "data" / "dados_consolidados_20250825_041845.csv",
        Path(__file__).resolve().parents[1] / "data" / "dados_consolidados_20250825_041845.csv",
        Path.cwd() /"Extracao_e_Insercao"/ "data" / "query_products.csv",
    ]
    
    brands, models = set(), set()
    
    for p in candidates:
        if p.exists():
            try:
                data = pd.read_json(p, dtype=False).to_dict(orient="records")
                for it in data:
                    b = (it.get("brand") or "").strip()
                    m = (it.get("line_model") or "").strip()
                    if b and not SPEED_RE.fullmatch(b.replace(" ","")):
                        brands.add(b.title())
                    if m:
                        m = re.sub(r"\(\s*(?:\d{2,3}[A-Z]{1,2})\s*\)","",m).strip()
                        if m: models.add(m.title())
                break
            except Exception:
                pass
    return brands, models

def split_title(title: str, known_brands: set[str], brand_aliases: dict[str,str] | None = None) -> dict:
    sz = extract_tire_size_from_title(title or "")
    brand = extract_brand(title or "", known_brands, brand_aliases)
    speed = sz.get("speed_symbol") or extract_speed_index(title or "")
    size_norm = sz.get("size_norm") or ""
    model = extract_model(title or "", brand, size_norm)

    base = {
        "brand": brand,
        "model": model,
        "speed_index": speed,
        "size_norm": size_norm or "",
        "width": sz.get("width_mm"),
        "aspect": sz.get("aspect_pct"),
        "rim": sz.get("rim_in"),
        "load_index": sz.get("load_index"),
        "construction": sz.get("construction"),
        "lt_flag": bool(sz.get("lt_flag")),
        "xl_flag": bool(sz.get("xl_flag")),
        "size_regex": sz.get("raw_pattern") or "",
    }
    return base

#fallback == coleta do titulo
def enrich_with_parsed_fields(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: 
        return df
    brands, _models = load_known_lists_from_lote()
    brand_aliases = {"Kelly":"Goodyear"}
    src_title = df["title"].fillna(df.get("product_name")).fillna(df.get("sku_text")).astype(str)

    parsed = src_title.apply(lambda t: split_title(t, brands, brand_aliases))
    parsed_df = pd.DataFrame(parsed.tolist(), index=df.index)

    cols = ["brand","model","speed_index","width","aspect","rim",
            "size_norm","size_regex","load_index","construction","lt_flag","xl_flag"]

    for col in cols:
        if col not in df.columns:
            df[col] = parsed_df[col]
        else:
            df[col] = df[col].fillna(parsed_df[col])

    for col in ("width","aspect","rim","load_index"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    
    for col in ("lt_flag","xl_flag"):
        if col in df.columns:
            df[col] = df[col].astype("boolean")

    return df

# ============================================================
# Limpeza / Dedup / Snapshots / Produtos
# ============================================================
def export_sqlite_outputs(clean: pd.DataFrame, snap: pd.DataFrame, canon: pd.DataFrame, out_db: Path) -> None:
    out_db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(out_db)
    
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")

        clean.to_sql("market_items_clean", conn, if_exists="replace", index=False)
        snap.to_sql("unifier_input",     conn, if_exists="replace", index=False)
        canon.to_sql("products_dim",     conn, if_exists="replace", index=False)

        conn.executescript("""
        CREATE INDEX IF NOT EXISTS ix_clean_marketplace_sku ON market_items_clean(marketplace, sku_norm);
        CREATE INDEX IF NOT EXISTS ix_clean_captured_at ON market_items_clean(captured_at);
        CREATE INDEX IF NOT EXISTS ix_unifier_marketplace_sku ON unifier_input(marketplace, sku_norm);
        -- índices úteis para pneus:
        CREATE INDEX IF NOT EXISTS ix_clean_size_norm ON market_items_clean(size_norm);
        CREATE INDEX IF NOT EXISTS ix_clean_size_triplet ON market_items_clean(width, aspect, rim);
        -- se você usar cod_prod como pivô:
        CREATE INDEX IF NOT EXISTS ix_clean_cod_prod ON market_items_clean(cod_prod);
        """)
        conn.commit()
    finally:
        conn.close()

def clean_and_snapshot(all_rows_df: pd.DataFrame, out_db: Optional[Path] = None):
    if not all_rows_df.empty:
        to_sql(all_rows_df, "market_items", if_exists="append", index=False)

    full = read_sql("SELECT * FROM market_items")
    
    if full.empty:
        logger.info("market_items vazio após ingestão.")
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
    full["title"] = full["title"].fillna(t_url).fillna(t_query)

    full["price"] = pd.to_numeric(full["price"], errors="coerce")
    parsed = full["query"].map(parse_captured_from_query)
    full["captured_at"] = pd.to_datetime(full.get("captured_at"), errors="coerce", utc=True)
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

    clean.sort_values("captured_at", ascending=False, inplace=True)

    has_cod = clean["cod_prod"].notna()
    df_with_cod = clean[has_cod].drop_duplicates(subset=["marketplace", "cod_prod"], keep="first")

    df_without_cod = clean[~has_cod].drop_duplicates(subset=["marketplace", "url"], keep="first")
    
    clean = pd.concat([df_with_cod, df_without_cod], ignore_index=True)
    
    clean = clean.drop_duplicates(subset=["marketplace", "title", "price"], keep="last")

    has_cod = clean["cod_prod"].notna()
    canon_cod = clean.loc[has_cod].groupby("cod_prod", dropna=False)["title"] \
                      .agg(lambda s: s.value_counts().index[0]).rename("product_name_cod").reset_index()
    canon_sku = clean.loc[~has_cod].groupby(["marketplace", "sku_norm"], dropna=False)["title"] \
                      .agg(lambda s: s.value_counts().index[0]).rename("product_name_sku").reset_index()

    clean = clean.merge(canon_cod, on="cod_prod", how="left")
    clean = clean.merge(canon_sku, on=["marketplace","sku_norm"], how="left")
    clean["product_name"] = clean["product_name_cod"].fillna(clean["product_name_sku"])
    clean.drop(columns=["product_name_cod","product_name_sku"], inplace=True)

    clean = enrich_with_parsed_fields(clean)
    
    to_sql(clean, "market_items_clean", if_exists="replace", index=False)

    if clean["cod_prod"].notna().any():
        key_cols = ["cod_prod"]
    else:
        key_cols = ["marketplace", "sku_norm"]

    snap = clean[
        clean.groupby(key_cols)["captured_at"].transform("max") == clean["captured_at"]
    ].drop_duplicates(subset=key_cols)

    to_sql(snap, "unifier_input", if_exists="replace", index=False)

    if "cod_prod" in key_cols:
        canon = clean.groupby("cod_prod", dropna=False)["product_name"] \
                     .agg(lambda s: s.value_counts().index[0]).reset_index()
    else:
        canon = clean.groupby(["marketplace","sku_norm"], dropna=False)["product_name"] \
                     .agg(lambda s: s.value_counts().index[0]).reset_index()

    canon = canon.rename(columns={"product_name":"product_name"})
    to_sql(canon, "products_dim", if_exists="replace", index=False)

    ensure_dirs()
    _save_parquet_with_fallback(clean, SETTINGS.processed_dir / "market_items_clean.parquet", "market_items_clean.csv")
    _save_parquet_with_fallback(snap,  SETTINGS.processed_dir / "unifier_input.parquet",     "unifier_input.csv")

    if out_db is not None:
        try:
            export_sqlite_outputs(clean, snap, canon, out_db)
            logger.info("📦 SQLite de saída atualizado em %s", out_db)
        except Exception as e:
            logger.exception("Falha ao exportar SQLite final (%s): %s", out_db, e)

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
        logger.info("📊 Top motivos de descarte (amostra):\n%s", tops.to_string())
        logger.info("📊 market_items_clean por marketplace:\n%s", clean["marketplace"].value_counts().to_string())
    except Exception as _e:
        logger.debug("diagnóstico de descarte falhou: %s", _e)

    logger.info("✅ Limpeza ok → market_items_clean=%d, unifier_input=%d", len(clean), len(snap))

# ============================================================
# Main
# ============================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw_dir", help="Pasta de JSON/CSV (default: SETTINGS.raw_dir)")
    ap.add_argument("--sqlite_dir", help="Pasta com .db (default: SETTINGS.sqlite_dir)")
    ap.add_argument("--out_db", help="Caminho para salvar o SQLite final (default: data/processed/warehouse.db)")
    args = ap.parse_args()

    ensure_dirs()
    if args.raw_dir:
        SETTINGS.raw_dir = Path(args.raw_dir)
    if args.sqlite_dir:
        SETTINGS.sqlite_dir = Path(args.sqlite_dir)

    default_out = SETTINGS.processed_dir / "warehouse.db"
    out_db = Path(args.out_db) if args.out_db else default_out

    all_rows: List[Dict[str, Any]] = []
    all_rows += ingest_scraper_dirs()           
    all_rows += ingest_json()
    all_rows += ingest_csv()
    all_rows += ingest_sqlite()

    if not all_rows:
        logger.info("Nenhum dado novo para ingerir (idempotência). Vou reconstruir o snapshot a partir do banco.")
        dummy = pd.DataFrame()
        clean_and_snapshot(dummy, out_db)
        return

    df_new = pd.DataFrame(all_rows)
    _save_parquet_with_fallback(df_new, SETTINGS.processed_dir / "market_items_new.parquet", "market_items_new.csv")

    clean_and_snapshot(df_new, out_db)

if __name__ == "__main__":
    main()
