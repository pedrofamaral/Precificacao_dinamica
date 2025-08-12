# unificar_marketplaces.py
# -*- coding: utf-8 -*-
"""
Unifica anúncios de múltiplos marketplaces (CSV/JSON/SQLite), normaliza colunas,
extrai brand/size/model (com aliases/frases via --config), cria chave canônica (brand|size|model),
deduplica e salva:

- unified_listings  (linha a linha)
- canonical_summary (por produto canônico, com P10/P90/mediana/média aparada 10–90)

Recursos:
- --input aceita UMA OU MAIS pastas (varredura recursiva)
- Filtros: --only-brand / --only-size / --only-model
- --split-by brand,size,model => cria um SQLite por grupo (output deve ser PASTA)
- --append para acrescentar nas tabelas
- --config JSON com:
    {
      "known_brands": ["goodyear","pirelli", ...],
      "brand_aliases": {"kelly": "goodyear", ...},
      "known_model_phrases": ["assurance maxlife","powercontact", ...],
      "model_aliases": {"power contact": "powercontact", "cint p7":"cinturato p7", ...}
    }
"""

import os
import re
import sys
import json
import math
import argparse
import sqlite3
import unicodedata
from pathlib import Path
from typing import List, Dict, Any

import numpy as np
import pandas as pd


# -----------------------------
# Defaults (podem ser sobrescritos via --config)
# -----------------------------
DEFAULT_KNOWN_BRANDS = [
    "goodyear", "pirelli", "michelin", "dunlop", "bridgestone", "continental",
    "hankook", "bfgoodrich", "firestone", "kumho", "atras", "maxxis", "formula",
    "yokohama", "toyo", "nitto", "general", "cooper", "falken", "nexen", "sumitomo"
]
DEFAULT_KNOWN_MODEL_PHRASES = [
    "kelly edge", "formula evo", "sp touring",
    "assurance", "maxlife", "efficientgrip",
    "wrangler", "eagle", "energy", "direction","kelly", "p400 EVO", "bc20", "lm704", "enasave ec300"
    "xl tl primacy", "primacy 4", "f700", "sp sport", "FM800","eagle sport", "p400"
    "energy xm2"
]

CONFIG = {
    "known_brands": DEFAULT_KNOWN_BRANDS.copy(),
    "brand_aliases": {},                      # ex.: {"kelly":"goodyear"}
    "known_model_phrases": DEFAULT_KNOWN_MODEL_PHRASES.copy(),
    "model_aliases": {}                       # ex.: {"power contact":"powercontact"}
}

SIZE_RE = re.compile(r"(\d{3})\s*[/\-]\s*(\d{2,3})\s*[r]?\s*[- ]?\s*(\d{2})", re.IGNORECASE)

CANONICAL_COLS = ["marketplace", "title", "price", "url", "brand_raw", "model_raw",
                  "size_raw", "seller", "collected_at"]

COLUMN_ALIASES = {
    "marketplace": ["marketplace", "site", "loja", "canal"],
    "title": ["titulo", "title", "nome", "product_title", "produto"],
    "price": ["preco", "price", "valor", "valor_preco"],
    "url": ["link", "url", "product_url"],
    "brand_raw": ["marca", "brand"],
    "model_raw": ["marca_filho", "modelo", "model", "marca"],
    "size_raw": ["medida", "tamanho", "size", "medidas"],
    "seller": ["vendedor", "seller", "loja"],
    "collected_at": ["data_coleta", "collected_at", "data", "capturado_em"]
}


# -----------------------------
# Utils
# -----------------------------
def norm_text(s: Any) -> str:
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return ""
    s = str(s).lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-z0-9 /\-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def to_float(val: Any) -> float:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return np.nan
    s = str(val).strip()
    s = re.sub(r"[^\d,.\-]", "", s)
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return np.nan

def parse_datetime_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", utc=True).dt.tz_convert(None)

def safe_part(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("/", "-").replace("\\", "-")
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"[^a-zA-Z0-9_.=\-]+", "-", s)
    return s.strip("-")


# -----------------------------
# Config helpers
# -----------------------------
def apply_config_lowerdedup():
    """normaliza CONFIG para tudo minúsculo e sem duplicatas."""
    CONFIG["known_brands"] = sorted({norm_text(b) for b in CONFIG.get("known_brands", []) if b})
    CONFIG["brand_aliases"] = {norm_text(k): norm_text(v) for k, v in CONFIG.get("brand_aliases", {}).items()}
    CONFIG["known_model_phrases"] = sorted({norm_text(m) for m in CONFIG.get("known_model_phrases", []) if m})
    CONFIG["model_aliases"] = {norm_text(k): norm_text(v) for k, v in CONFIG.get("model_aliases", {}).items()}

def _canon_brand(s: str) -> str:
    s = norm_text(s)
    if not s:
        return ""
    # alias
    if s in CONFIG["brand_aliases"]:
        return CONFIG["brand_aliases"][s]
    # match exato contra lista conhecida
    for kb in CONFIG["known_brands"]:
        if s == kb:
            return kb
    # tentar detectar token da lista conhecida contido no string
    for kb in CONFIG["known_brands"]:
        if f" {kb} " in f" {s} ":
            return kb
    # fallback: primeira palavra
    return s.split()[0]

def _canon_model(s: str) -> str:
    s = norm_text(s)
    if not s:
        return ""
    if s in CONFIG["model_aliases"]:
        return CONFIG["model_aliases"][s]
    return s


# -----------------------------
# Extractors
# -----------------------------
def extract_size(row: Dict[str, Any]) -> str:
    for cand_key in ("size_raw", "title"):
        cand = row.get(cand_key, "")
        m = SIZE_RE.search(norm_text(cand))
        if m:
            return f"{m.group(1)}/{m.group(2)}R{m.group(3)}"
    return ""

def extract_brand(row: Dict[str, Any]) -> str:
    # 1) campo explícito
    b = row.get("brand_raw") or ""
    b = _canon_brand(b)
    if b:
        return b
    # 2) título
    t = norm_text(row.get("title",""))
    # tenta marca conhecida
    for kb in CONFIG["known_brands"]:
        if f" {kb} " in f" {t} ":
            return kb
    # tenta alias no título
    for alias, target in CONFIG["brand_aliases"].items():
        if f" {alias} " in f" {t} ":
            return target
    return ""

def extract_model(row: Dict[str, Any], brand: str) -> str:
    # 1) explícito
    m = _canon_model(row.get("model_raw") or "")
    if m:
        return m
    t = norm_text(row.get("title",""))
    # 2) frases conhecidas (prioridade)
    for phrase in CONFIG["known_model_phrases"]:
        if phrase and phrase in t:
            return _canon_model(phrase)
    # 3) heurística após a marca
    if brand and brand in t:
        after = t.split(brand, 1)[1].strip()
        toks = [w for w in after.split() if w not in {
            "pneu","aro","r13","r14","r15","r16","r17",
            "175/70r13","175/70","175-70","t","82","82t","p","86","88","h","v"
        }]
        if toks:
            guess = " ".join(toks[:2])
            return _canon_model(guess)
    return ""


# -----------------------------
# Readers
# -----------------------------
def load_csv(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(path, encoding="latin-1")
    df["__source_file"] = path.name
    return df

def load_json(path: Path) -> pd.DataFrame:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        df = pd.DataFrame(data)
    elif isinstance(data, dict):
        # tenta listas dentro do dict
        for k, v in data.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return pd.DataFrame(v)
        df = pd.json_normalize(data)
    else:
        df = pd.DataFrame([{"raw": data}])
    df["__source_file"] = path.name
    return df

def read_sqlite_tables(path: Path) -> Dict[str, pd.DataFrame]:
    out = {}
    con = sqlite3.connect(str(path))
    try:
        tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", con)["name"].tolist()
        for t in tables:
            try:
                out[t] = pd.read_sql(f"SELECT * FROM {t};", con)
                out[t]["__source_file"] = path.name
                out[t]["__table"] = t
            except Exception as e:
                out[t] = pd.DataFrame([{"__error": str(e), "__table": t, "__source_file": path.name}])
    finally:
        con.close()
    return out

def discover_files(input_dir: Path) -> Dict[str, List[Path]]:
    csvs = list(input_dir.rglob("*.csv"))
    jsons = list(input_dir.rglob("*.json"))
    sqlites = list(input_dir.rglob("*.sqlite")) + list(input_dir.rglob("*.db"))
    return {"csv": csvs, "json": jsons, "sqlite": sqlites}


# -----------------------------
# Normalização e canônico
# -----------------------------
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=CANONICAL_COLS)
    cols_lower = {c.lower(): c for c in df.columns}
    out = pd.DataFrame()
    def pick(names):
        for n in names:
            if n in cols_lower:
                return df[cols_lower[n]]
        return pd.Series([None]*len(df))
    out["marketplace"]  = pick(COLUMN_ALIASES["marketplace"])
    out["title"]        = pick(COLUMN_ALIASES["title"])
    out["price"]        = pick(COLUMN_ALIASES["price"])
    out["url"]          = pick(COLUMN_ALIASES["url"])
    out["brand_raw"]    = pick(COLUMN_ALIASES["brand_raw"])
    out["model_raw"]    = pick(COLUMN_ALIASES["model_raw"])
    out["size_raw"]     = pick(COLUMN_ALIASES["size_raw"])
    out["seller"]       = pick(COLUMN_ALIASES["seller"])
    out["collected_at"] = pick(COLUMN_ALIASES["collected_at"])
    if out["marketplace"].isna().all() and "url" in out:
        m = out["url"].astype(str).str.extract(r"https?://(?:www\.)?([a-z0-9\-]+)\.", expand=False)
        out["marketplace"] = m
    return out

def build_canonical(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["marketplace","title","price","url","brand","model","size",
                                     "canonical_key","collected_at","seller"])
    df = df.copy()
    df["price"] = df["price"].apply(to_float)
    df["title"] = df["title"].fillna("")
    df["title_norm"] = df["title"].apply(norm_text)

    df["size"]  = df.apply(lambda r: extract_size(r), axis=1)
    df["brand"] = df.apply(lambda r: extract_brand(r), axis=1)
    df["model"] = df.apply(lambda r: extract_model(r, r.get("brand","")), axis=1)

    df["canonical_key"] = (
        df["brand"].fillna("") + "|" + df["size"].fillna("") + "|" + df["model"].fillna("")
    ).str.strip("|")

    df["marketplace"] = df["marketplace"].fillna("")
    if "url" in df.columns:
        from_url = df["url"].astype(str).str.extract(r"https?://(?:www\.)?([a-z0-9\-]+)\.", expand=False)
        df.loc[df["marketplace"]=="", "marketplace"] = from_url

    if "collected_at" in df.columns:
        df["collected_at"] = parse_datetime_series(df["collected_at"])
        df["collected_at"] = df["collected_at"].dt.strftime("%Y-%m-%d %H:%M:%S")

    df = df[~df["price"].isna() & (df["title"].str.len() > 0)].copy()

    if "url" in df.columns:
        df["_dedup_key"] = np.where(
            df["url"].notna() & (df["url"].astype(str)!=""),
            df["marketplace"].fillna("") + "|" + df["url"].astype(str),
            df["marketplace"].fillna("") + "|" + df["title_norm"].fillna("") + "|" + df["price"].astype(str)
        )
    else:
        df["_dedup_key"] = df["marketplace"].fillna("") + "|" + df["title_norm"].fillna("") + "|" + df["price"].astype(str)

    df = df.drop_duplicates(subset=["_dedup_key"]).drop(columns=["_dedup_key","title_norm"], errors="ignore")

    keep = ["marketplace","title","price","url","brand","model","size","canonical_key","collected_at","seller"]
    for k in keep:
        if k not in df.columns:
            df[k] = None
    return df[keep]


# -----------------------------
# Agregados
# -----------------------------
def summarize_canonical(unified: pd.DataFrame) -> pd.DataFrame:
    if unified.empty:
        return pd.DataFrame(columns=[
            "canonical_key","brand","model","size","n_listings","marketplaces",
            "min_price","max_price","mean_price","median_price","p10","p90","média_correta"
        ])

    def _agg_group(g: pd.DataFrame) -> pd.Series:
        prices = g["price"].astype(float).dropna().values
        if len(prices) == 0:
            return pd.Series({
                "n_listings": len(g),
                "marketplaces": [],
                "min_price": np.nan, "max_price": np.nan, "mean_price": np.nan,
                "median_price": np.nan, "p10": np.nan, "p90": np.nan,
                "média_correta": np.nan
            })
        p10 = float(np.quantile(prices, 0.10))
        p90 = float(np.quantile(prices, 0.90))
        trimmed = [x for x in prices if p10 <= x <= p90]
        media_correta = float(np.mean(trimmed)) if len(trimmed) else np.nan
        return pd.Series({
            "n_listings": len(g),
            "marketplaces": sorted(set([m for m in g["marketplace"].dropna().astype(str) if m])),
            "min_price": float(np.min(prices)),
            "max_price": float(np.max(prices)),
            "mean_price": float(np.mean(prices)),
            "median_price": float(np.median(prices)),
            "p10": p10,
            "p90": p90,
            "média_correta": media_correta
        })

    summary = (unified
               .groupby(["canonical_key","brand","model","size"], dropna=False)
               .apply(_agg_group)
               .reset_index())

    summary["marketplaces"] = summary["marketplaces"].apply(lambda lst: ",".join(lst) if isinstance(lst, list) else str(lst))
    summary = summary.sort_values(["brand","model","size","n_listings"], ascending=[True, True, True, False]).reset_index(drop=True)
    return summary


# -----------------------------
# Pipeline
# -----------------------------
def discover_files(input_dir: Path) -> Dict[str, List[Path]]:
    csvs = list(input_dir.rglob("*.csv"))
    jsons = list(input_dir.rglob("*.json"))
    sqlites = list(input_dir.rglob("*.sqlite")) + list(input_dir.rglob("*.db"))
    return {"csv": csvs, "json": jsons, "sqlite": sqlites}

def process_input_folders(input_dirs: List[Path]) -> pd.DataFrame:
    all_norm = []
    for in_dir in input_dirs:
        if not in_dir.exists() or not in_dir.is_dir():
            print(f"[WARN] Pasta ignorada: {in_dir}")
            continue
        files = discover_files(in_dir)
        print(f"[INFO] {in_dir} => {len(files['csv'])} CSV, {len(files['json'])} JSON, {len(files['sqlite'])} SQLite/DB")

        # CSV
        for p in files["csv"]:
            try:
                df = load_csv(p)
                norm = normalize_columns(df)
                built = build_canonical(norm)
                if not built.empty:
                    all_norm.append(built)
                print(f"[OK] CSV: {p.name} -> {len(built)} linhas")
            except Exception as e:
                print(f"[WARN] CSV {p}: {e}")

        # JSON
        for p in files["json"]:
            try:
                df = load_json(p)
                norm = normalize_columns(df)
                built = build_canonical(norm)
                if not built.empty:
                    all_norm.append(built)
                print(f"[OK] JSON: {p.name} -> {len(built)} linhas")
            except Exception as e:
                print(f"[WARN] JSON {p}: {e}")

        # SQLite
        for p in files["sqlite"]:
            try:
                tables = read_sqlite_tables(p)
                count_file = 0
                for tname, df in tables.items():
                    if df is None or df.empty:
                        continue
                    norm = normalize_columns(df)
                    built = build_canonical(norm)
                    if not built.empty:
                        all_norm.append(built)
                        count_file += len(built)
                print(f"[OK] SQLite: {p.name} -> {count_file} linhas (somando tabelas)")
            except Exception as e:
                print(f"[WARN] SQLite {p}: {e}")

    if not all_norm:
        print("[ERRO] Nenhum dado útil foi lido.")
        return pd.DataFrame(columns=["marketplace","title","price","url","brand","model","size","canonical_key","collected_at","seller"])

    unified = pd.concat(all_norm, ignore_index=True, sort=False)
    unified["price"] = unified["price"].astype(float)
    return unified

def apply_filters(unified: pd.DataFrame, only_brand: str, only_size: str, only_model: str) -> pd.DataFrame:
    df = unified
    if only_brand:
        df = df[norm_text(df["brand"]) == norm_text(only_brand)]
    if only_size:
        sz_in = norm_text(only_size).replace("-", "/").replace(" r", "r").upper()
        sz_in = sz_in.replace("//", "/").replace("R/", "R")
        df = df[df["size"].str.upper() == sz_in]
    if only_model:
        df = df[norm_text(df["model"]) == norm_text(only_model)]
    return df

def summarize_and_save(unified: pd.DataFrame, out_path: Path, append: bool):
    mode = "append" if append and out_path.exists() else "replace"
    con = sqlite3.connect(str(out_path))
    try:
        unified.to_sql("unified_listings", con, if_exists=mode, index=False)
        summary = summarize_canonical(unified)
        summary.to_sql("canonical_summary", con, if_exists=mode, index=False)
    finally:
        con.close()
    print(f"[DONE] Salvo em: {out_path}  (unified_listings={len(unified)} linhas, summary={len(summarize_canonical(unified))} linhas)")

def save_partitioned(unified: pd.DataFrame, out_dir: Path, split_by: List[str], append: bool):
    out_dir.mkdir(parents=True, exist_ok=True)
    for _, group in unified.groupby(split_by):
        parts = []
        for col in split_by:
            val = group.iloc[0][col]
            parts.append(f"{col}={safe_part(str(val)) if val is not None else 'NA'}")
        db_name = "__".join(parts) + ".db"
        out_path = out_dir / db_name
        summarize_and_save(group, out_path, append=append)


# -----------------------------
# CLI
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Unifica produtos de múltiplos marketplaces e gera SQLite(s).")
    ap.add_argument("--input", required=True, nargs="+", help="Uma ou mais pastas de entrada (busca recursiva CSV/JSON/SQLite).")
    ap.add_argument("--output", required=True, help="Arquivo SQLite (sem --split-by) OU pasta (com --split-by).")
    ap.add_argument("--only-brand", help="Filtra por marca (ex.: goodyear).")
    ap.add_argument("--only-size", help="Filtra por medida (ex.: 175/70R13).")
    ap.add_argument("--only-model", help='Filtra por modelo (ex.: "assurance maxlife").')
    ap.add_argument("--append", action="store_true", help="Acrescenta (append) nas tabelas existentes em vez de substituir.")
    ap.add_argument("--split-by", help="Particiona e salva um SQLite por grupo. Ex.: 'brand', 'size', 'model', 'brand,size,model'.")
    ap.add_argument("--config", help="JSON com known_brands/brand_aliases/known_model_phrases/model_aliases para normalização.")

    args = ap.parse_args()

    # Carrega config (se houver) e normaliza
    if args.config:
        p = Path(args.config).expanduser().resolve()
        if not p.exists():
            print(f"[WARN] --config não encontrado: {p}. Usando defaults embutidos.")
        else:
            try:
                with open(p, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
                # merge simples (sobrescreve defaults quando presente)
                for k in ("known_brands","brand_aliases","known_model_phrases","model_aliases"):
                    if k in cfg:
                        CONFIG[k] = cfg[k]
            except Exception as e:
                print(f"[WARN] Falha ao ler --config: {e}. Usando defaults.")
    apply_config_lowerdedup()

    input_dirs = [Path(p).expanduser().resolve() for p in args.input]
    out = Path(args.output).expanduser().resolve()

    print(f"[INFO] CWD: {Path.cwd()}")
    for dd in input_dirs:
        print(f"[INFO] Input: {dd}")

    unified = process_input_folders(input_dirs)
    if unified.empty:
        print("[ERRO] Nada para salvar.")
        sys.exit(2)

    # Filtros
    unified = apply_filters(unified, args.only_brand, args.only_size, args.only_model)
    if unified.empty:
        print("[ERRO] Nada após filtros. Ajuste --only-brand/--only-size/--only-model.")
        sys.exit(3)

    # split-by
    if args.split_by:
        split_cols = [c.strip().lower() for c in args.split_by.split(",") if c.strip()]
        for c in split_cols:
            if c not in {"brand","size","model"}:
                print(f"[ERRO] Coluna inválida em --split-by: {c}. Use brand,size,model.")
                sys.exit(4)
        if out.suffix:  # se tiver extensão, parece arquivo
            print("[ERRO] Quando usar --split-by, --output deve ser uma PASTA.")
            sys.exit(5)
        save_partitioned(unified, out, split_cols, append=args.append)
    else:
        if out.exists() and not args.append:
            out.unlink()
        summarize_and_save(unified, out, append=args.append)


if __name__ == "__main__":
    main()
