# load_market_JSON.py

import os
import sys
import json
import sqlite3
import glob
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

# Carrega DB_PATH do .env
load_dotenv()
DB = os.environ.get("DB_PATH", "./data/processed/pricing.db")

# -------------------------
# Utilitários
# -------------------------
def ensure_schema(con: sqlite3.Connection):
    con.executescript("""
    CREATE TABLE IF NOT EXISTS competitors (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      sku_key TEXT,
      marketplace TEXT,
      title TEXT,
      link TEXT,
      price REAL,
      raw_price REAL,
      collected_at TEXT,
      available INTEGER,
      seller TEXT,
      freight REAL,
      delivery_time_days INTEGER
    );
    """)

def expand_inputs(args):
    paths = []
    for a in args:
        p = Path(a)
        if p.is_dir():
            paths.extend(str(x) for x in p.rglob("*.json"))
        elif any(ch in a for ch in "*?[]"):
            paths.extend(glob.glob(a, recursive=True))
        elif p.suffix.lower()==".json" and p.exists():
            paths.append(str(p))
    return sorted(set(paths))

def infer_marketplace_from_path(path: str) -> str:
    pl = path.lower()
    if "mercadolivre" in pl: return "mercadolivre"
    if "pneustore"   in pl: return "pneustore"
    return "desconhecido"

def infer_sku_key_from_filename(path: str) -> str:
    fname = Path(path).stem.lower()
    if "assurance" in fname:
        return "pneu-17570r13-goodyear-assurance"
    if "kelly" in fname:
        return "pneu-17570r13-goodyear-kelly"
    if "dunlop" in fname or "sp-touring" in fname:
        return "pneu-17570r13-dunlop-sp-touring"
    return "pneu-17570r13-desconhecido"

def coalesce(d: dict, *keys, default=None):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default

def parse_records(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("items","results","data","anuncios","ads","rows"):
            if key in data and isinstance(data[key], list):
                return data[key]
        return [data]
    return []

def normalize_price_value(x: float) -> float:
    try:
        p = float(x)
    except Exception:
        return None
    while p and p > 5000:
        p = p / 10.0
    return round(p, 2)


def parse_datetime_any(v):
    # aceita ISO string ou epoch ms
    try:
        return pd.to_datetime(v, errors="coerce")
    except Exception:
        return pd.to_datetime(v, unit="ms", utc=True, errors="coerce").dt.tz_localize(None)

# -------------------------
# Normalizadores
# -------------------------
def normalize_generic(records, source_hint: str, sku_key_hint: str):
    rows = []
    for r in records or []:
        title = coalesce(r, "titulo","title","name","search_term","descricao","description") or sku_key_hint
        link  = coalesce(r, "link","url","permalink")
        rawp  = coalesce(r, "preco","price","valor","raw_price")
        price = normalize_price_value(rawp)
        dc    = coalesce(r, "data_coleta","collected_at","timestamp","scraped_at")
        dtv   = parse_datetime_any(dc) or pd.Timestamp.utcnow()
        rows.append({
            "sku_key": sku_key_hint,
            "marketplace": source_hint,
            "title": title,
            "link": link,
            "raw_price": float(rawp) if rawp not in (None, "") else None,
            "price": price,
            "collected_at": dtv,
            "available": int(coalesce(r, "available","disponivel","estoque", default=1)),
            "seller": coalesce(r, "seller","vendedor","store","shop"),
            "freight": coalesce(r, "frete","freight"),
            "delivery_time_days": coalesce(r, "prazo_entrega","delivery_time_days")
        })
    return pd.DataFrame(rows)

def normalize_ml(records, sku_key_hint):
    # Quando houver "search_term" no JSON, uso ML format
    df = pd.DataFrame(records or [])
    if "search_term" not in df.columns:
        return normalize_generic(records, "mercadolivre", sku_key_hint)

    out = pd.DataFrame()
    out["sku_key"]      = df["search_term"].fillna(sku_key_hint)
    out["marketplace"]  = df.get("marketplace", "mercadolivre")
    out["title"]        = df["search_term"].fillna(sku_key_hint)
    out["link"]         = df.get("link")
    # usa apenas o campo 'preco'
    out["raw_price"]    = pd.to_numeric(df.get("preco"), errors="coerce")
    out["price"]        = out["raw_price"].apply(normalize_price_value)
    out["collected_at"] = parse_datetime_any(df.get("data_coleta"))
    out["available"]    = 1
    out["seller"]       = None
    out["freight"]      = None
    out["delivery_time_days"] = None
    return out

def normalize_pneustore(records, sku_key_hint):
    df = pd.DataFrame(records or [])
    if "titulo" not in df.columns:
        return normalize_generic(records, "pneustore", sku_key_hint)

    out = pd.DataFrame()
    out["sku_key"]      = sku_key_hint
    out["marketplace"]  = df.get("marketplace", "pneustore")
    out["title"]        = df.get("titulo")
    out["link"]         = df.get("link")
    out["raw_price"]    = pd.to_numeric(df.get("preco"), errors="coerce")
    out["price"]        = out["raw_price"].round(2)
    out["collected_at"] = parse_datetime_any(df.get("data_coleta"))
    out["available"]    = (~df.get("frete_gratis", pd.Series([None]*len(df))).isna()).astype(int)
    out["seller"]       = df.get("vendedor")
    out["freight"]      = None
    out["delivery_time_days"] = None
    return out

# -------------------------
# Pipeline principal
# -------------------------
def main(args):
    paths = expand_inputs(args)
    if not paths:
        print("Nenhum JSON encontrado nos caminhos informados.")
        sys.exit(1)

    con = sqlite3.connect(DB)
    ensure_schema(con)
    frames = []

    for p in paths:
        try:
            data = json.load(open(p, "r", encoding="utf-8"))
        except Exception as e:
            print(f"[WARN] Falha lendo {p}: {e}")
            continue

        source  = infer_marketplace_from_path(p)
        sku_key = infer_sku_key_from_filename(p)
        records = parse_records(data)

        if source == "mercadolivre":
            df = normalize_ml(records, sku_key)
        elif source == "pneustore":
            df = normalize_pneustore(records, sku_key)
        else:
            df = normalize_generic(records, source, sku_key)

        frames.append(df)

    if not frames:
        print("Nada para inserir.")
        sys.exit(0)

    all_df = pd.concat(frames, ignore_index=True)
    all_df.to_sql("competitors", con, if_exists="append", index=False)
    con.commit()
    con.close()

    print(f"Inseridos {len(all_df)} registros em 'competitors' no DB '{DB}'.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python load_market_JSON.py data/raw")
        sys.exit(1)
    main(sys.argv[1:])
