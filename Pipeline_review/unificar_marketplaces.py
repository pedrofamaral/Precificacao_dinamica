import os
import re
import sys
import json
import math
import argparse
import sqlite3
import unicodedata
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import List, Dict, Any, Optional

import numpy as np
import pandas as pd

# -----------------------------
# Regex de tamanho (abrangentes)
# -----------------------------
SIZE_PATTERNS = [
    r"(?<!\d)(\d{3})\s*[/xX\-]\s*(\d{2})\s*[rRzZ][fF]?\s*-?\s*(\d{2})(?!\d)",  # 195/55R16 | 195x55R16 | 195/55ZR16
    r"(?<!\d)(\d{3})\s*[/\-]\s*(\d{2})\s*(\d{2})(?!\d)",                        # 195/55 16
    r"(?<!\d)(\d{3})(\d{2})[rRzZ][fF]?(\d{2})(?!\d)",                           # 19555R16 | 19555ZR16
    r"(?<!\d)(\d{2})\s*[xX]\s*(\d{2}\.?\d*)\s*[rR]\s*(\d{2})(?!\d)",            # 31x10.5R15 (off-road)
]
SIZE_RES = [re.compile(p) for p in SIZE_PATTERNS]

# Regex simples legado (mantido para compat)
SIZE_RE = re.compile(r"(\d{3})\s*[/\-]\s*(\d{2,3})\s*[r]?\s*[- ]?\s*(\d{2})", re.IGNORECASE)

# -----------------------------
# Config
# -----------------------------
DEFAULT_KNOWN_BRANDS = [
    "goodyear", "pirelli", "michelin", "dunlop", "bridgestone", "continental",
    "hankook", "bfgoodrich", "firestone", "kumho", "atras", "maxxis", "formula",
    "yokohama", "toyo", "nitto", "general", "cooper", "falken", "nexen", "sumitomo"
]
DEFAULT_KNOWN_MODEL_PHRASES = [
    "kelly edge", "formula evo", "sp touring",
    "assurance", "maxlife", "efficientgrip",
    "wrangler", "eagle", "energy", "direction", "kelly", "p400 evo",
    "bc20", "lm704", "enasave ec300", "xl tl primacy", "primacy 4",
    "f700", "sp sport", "fm800", "eagle sport", "p400", "energy xm2"
]

CONFIG = {
    "known_brands": DEFAULT_KNOWN_BRANDS.copy(),
    "brand_aliases": {},
    "known_model_phrases": DEFAULT_KNOWN_MODEL_PHRASES.copy(),
    "model_aliases": {}
}

CANONICAL_COLS = ["marketplace", "title", "price", "url", "brand_raw", "model_raw",
                  "size_raw", "seller", "collected_at", "source_file"]

COLUMN_ALIASES = {
    "title":       ["title","titulo","nome","produto","product_title"],
    "url":         ["url","link","href","page_url","request_url"],
    "price":       ["price","preco","valor","amount"],
    "brand_raw":   ["brand_raw","brand","marca","marca_prod","marca_produto"],
    "model_raw":   ["model_raw","model","modelo","modelo_prod","linha","linha_modelo"],
    "size_raw":    ["size_raw","size","tamanho","medida","medida_norm"],
    "seller":      ["seller","vendedor","loja","store","seller_name","store_name"],
    "collected_at":["collected_at","data_coleta","coletado_em","scrape_date","scraped_at"],
    "marketplace": ["marketplace","site","canal"],
    "source_file": ["source_file","arquivo","fonte","raw_file"],
}

# -----------------------------
# Logging helpers
# -----------------------------
def setup_logging(path: str, level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger("unify")
    logger.setLevel(getattr(logging, level, logging.INFO))

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
    fh = RotatingFileHandler(path, maxBytes=10_000_000, backupCount=5, encoding="utf-8")
    fh.setFormatter(fmt); fh.setLevel(logger.level)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt); ch.setLevel(logger.level)

    if not logger.handlers:
        logger.addHandler(fh); logger.addHandler(ch)
    return logger

def log_missing(df: pd.DataFrame, stage: str):
    logger = logging.getLogger("unify")
    cols = ["title","url","price","brand_raw","model_raw","size_raw","seller","marketplace","collected_at"]
    present = [c for c in cols if c in df.columns]
    miss = {c: float(((df[c].isna()) | (df[c].astype(str)=="")).mean()) for c in present}
    logger.debug(f"[{stage}] missing_pct: {miss}")

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
    CONFIG["known_brands"] = sorted({norm_text(b) for b in CONFIG.get("known_brands", []) if b})
    CONFIG["brand_aliases"] = {norm_text(k): norm_text(v) for k, v in CONFIG.get("brand_aliases", {}).items()}
    CONFIG["known_model_phrases"] = sorted({norm_text(m) for m in CONFIG.get("known_model_phrases", []) if m})
    CONFIG["model_aliases"] = {norm_text(k): norm_text(v) for k, v in CONFIG.get("model_aliases", {}).items()}

SOURCE_TAG_TAIL_SEGMENTS = 3
def make_source_tag(file_path: Path, base_dir: Path, tail_segments: int = SOURCE_TAG_TAIL_SEGMENTS) -> str:
    try:
        base_res = base_dir.resolve()
        file_res = file_path.resolve()
        rel = file_res.relative_to(base_res).as_posix()
        n = min(tail_segments, len(base_res.parts))
        base_tail = "/".join(base_res.parts[-n:])
        return f"{base_tail}/{rel}" if rel else base_tail
    except Exception:
        return file_path.name

def _canon_brand(s: str) -> str:
    s = norm_text(s)
    if not s:
        return ""
    if s in CONFIG["brand_aliases"]:
        return CONFIG["brand_aliases"][s]
    for kb in CONFIG["known_brands"]:
        if s == kb:
            return kb
    for kb in CONFIG["known_brands"]:
        if f" {kb} " in f" {s} ":
            return kb
    return s.split()[0]

def _canon_model(s: str) -> str:
    s = norm_text(s)
    if not s:
        return ""
    if s in CONFIG["model_aliases"]:
        return CONFIG["model_aliases"][s]
    return s

# -----------------------------
# Extratores linha-a-linha (legados)
# -----------------------------
def extract_size(row: Dict[str, Any]) -> str:
    for cand_key in ("size_raw", "title"):
        cand = row.get(cand_key, "")
        m = SIZE_RE.search(norm_text(cand))
        if m:
            return f"{m.group(1)}/{m.group(2)}R{m.group(3)}"
    return ""

def extract_brand(row: Dict[str, Any]) -> str:
    b = row.get("brand_raw") or ""
    b = _canon_brand(b)
    if b:
        return b
    t = norm_text(row.get("title",""))
    for kb in CONFIG["known_brands"]:
        if f" {kb} " in f" {t} ":
            return kb
    for alias, target in CONFIG["brand_aliases"].items():
        if f" {alias} " in f" {t} ":
            return target
    return ""

def extract_model(row: Dict[str, Any], brand: str) -> str:
    m = _canon_model(row.get("model_raw") or "")
    if m:
        if brand and m == brand:
            return ""
        return m

    t = norm_text(row.get("title", ""))

    for phrase in CONFIG.get("known_model_phrases", []):
        phrase = (phrase or "").strip().lower()
        if not phrase:
            continue
        p = re.compile(rf"(?<![a-z0-9]){re.escape(phrase)}(?![a-z0-9])")
        if p.search(t):
            cand = _canon_model(phrase)
            if not brand or cand != brand:
                return cand

    if brand:
        t_spaced = f" {t} "
        brand_spaced = f" {brand} "
        if brand_spaced in t_spaced:
            after = t_spaced.split(brand_spaced, 1)[1].strip()

            msize = SIZE_RE.search(after)
            if msize:
                after = after[:msize.start()].strip()

            after = re.sub(r"[\/\-_,]+", " ", after).strip()

            stop = {
                "pneu", "pneus", "aro", "tl", "tt", "tl/tt",
                "xl", "runflat", "rft", "reforce", "reforzado", "reforçado",
                "radial", "tubeless", "tubetype", "indice", "indicecarga",
                "h","v","t","w","y","z",
                "r10","r12","r13","r14","r15","r16","r17","r18","r19","r20","r21","r22",
                "82","84","86","88","90","91","92","94","95","97","99","100",
            }

            toks = [w for w in after.split() if w and w not in stop]

            if toks:
                guess = " ".join(toks[:3]).strip(" -_/")
                guess = _canon_model(guess)

                if guess and (not brand or guess != brand):
                    return guess

    return ""

# -----------------------------
# Extratores vetorizados (para logs e fallback seguro de size)
# -----------------------------
def _norm_text_simple(s: str) -> str:
    return norm_text(s)

def extract_size_from_title_series(series: pd.Series) -> pd.Series:
    t = series.fillna("").astype(str).map(_norm_text_simple)
    out = pd.Series([""] * len(t), index=t.index, dtype=object)
    for i, rx in enumerate(SIZE_RES):
        m = t.str.extract(rx, expand=True)
        if m.isnull().all().all():
            continue
        if i == len(SIZE_RES) - 1:  # padrão 31x10.5R15
            fill = (m[0] + "x" + m[1] + "R" + m[2]).str.upper()
        else:
            fill = (m[0] + "/" + m[1] + "R" + m[2]).str.upper()
        out = out.mask((out == "") & m.notna().all(axis=1), fill)
    return out

# -----------------------------
# Readers
# -----------------------------
def load_csv(path: Path, base_dir: Path) -> pd.DataFrame:
    logger = logging.getLogger("unify")
    try:
        df = pd.read_csv(path, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(path, encoding="latin-1")
    df["__source_file"] = make_source_tag(path, base_dir)
    logger.info(f"[load_csv] {path.name} -> {len(df)} linhas")
    return df

def load_json(path: Path, base_dir: Path) -> pd.DataFrame:
    logger = logging.getLogger("unify")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        df = pd.DataFrame(data)
    elif isinstance(data, dict):
        for k, v in data.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                df = pd.DataFrame(v)
                break
        else:
            df = pd.json_normalize(data)
    else:
        df = pd.DataFrame([{"raw": data}])
    df["__source_file"] = make_source_tag(path, base_dir)
    logger.info(f"[load_json] {path.name} -> {len(df)} linhas")
    return df

def read_sqlite_tables(path: Path, base_dir: Path) -> Dict[str, pd.DataFrame]:
    logger = logging.getLogger("unify")
    out = {}
    con = sqlite3.connect(str(path))
    try:
        tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", con)["name"].tolist()
        for t in tables:
            try:
                dft = pd.read_sql(f"SELECT * FROM {t};", con)
                dft["__source_file"] = make_source_tag(path, base_dir)
                dft["__table"] = t
                out[t] = dft
                logger.info(f"[read_sqlite] {path.name}:{t} -> {len(dft)} linhas")
            except Exception as e:
                out[t] = pd.DataFrame([{"__error": str(e), "__table": t, "__source_file": make_source_tag(path, base_dir)}])
                logger.warning(f"[read_sqlite] {path.name}:{t} erro: {e}")
    finally:
        con.close()
    return out

def discover_files(input_dir: Path) -> Dict[str, List[Path]]:
    csvs = list(input_dir.rglob("*.csv"))
    jsons = list(input_dir.rglob("*.json"))
    sqlites = list(input_dir.rglob("*.sqlite")) + list(input_dir.rglob("*.db"))
    logger = logging.getLogger("unify")
    logger.info(f"[discover] {input_dir} => CSV={len(csvs)} JSON={len(jsons)} SQLITE/DB={len(sqlites)}")
    return {"csv": csvs, "json": jsons, "sqlite": sqlites}

# -----------------------------
# Normalização e canônico
# -----------------------------
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    logger = logging.getLogger("unify")
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
    out["source_file"]  = df.get("__source_file") if "__source_file" in df.columns else None

    log_missing(out, "normalize_columns:aliases")

    if out["marketplace"].isna().all() and "url" in out:
        m = out["url"].astype(str).str.extract(r"https?://(?:www\.)?([a-z0-9\-]+)\.", expand=False)
        out["marketplace"] = m
        logger.debug("[normalize_columns] marketplace preenchido via domínio do URL (fallback).")

    return out

def build_canonical(df: pd.DataFrame) -> pd.DataFrame:
    logger = logging.getLogger("unify")
    keep = [
        "marketplace", "title", "price", "url",
        "brand", "model", "size",
        "canonical_key", "collected_at", "seller", "source_file"
    ]

    if df is None or df.empty:
        return pd.DataFrame(columns=keep)

    logger.info(f"[canonical] input={len(df)}")
    df = df.copy()

    base_cols = ["marketplace","title","price","url","brand","model","size",
                 "seller","collected_at","source_file"]
    for c in base_cols:
        if c not in df.columns:
            df[c] = pd.NA

    try:
        df["price"] = df["price"].apply(to_float)
    except Exception:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")

    df["title"] = df["title"].fillna("").astype(str).str.strip()
    df["title_norm"] = df["title"].apply(norm_text)

    df["seller"] = df["seller"].fillna("").astype(str).str.strip()
    df["seller_norm"] = df["seller"].apply(norm_text)

    recs = df.to_dict("records")

    if "size_raw" in df.columns:
        df["size"] = [r.get("size_raw") or extract_size(r) for r in recs]
    else:
        df["size"] = [extract_size(r) for r in recs]

    if "brand_raw" in df.columns:
        df["brand"] = [r.get("brand_raw") or extract_brand(r) for r in recs]
    else:
        df["brand"] = [extract_brand(r) for r in recs]

    if "model_raw" in df.columns:
        df["model"] = [r.get("model_raw") or extract_model(r, r.get("brand")) for r in recs]
    else:
        df["model"] = [extract_model(r, r.get("brand")) for r in recs]

    blank_size = (df["size"].fillna("") == "")
    if blank_size.any():
        size_from_title = extract_size_from_title_series(df["title"])
        can_fill = blank_size & (size_from_title != "")
        filled_n = int(can_fill.sum())
        if filled_n:
            df.loc[can_fill, "size"] = size_from_title[can_fill]
        logger.info(f"[fallback:size] blanks={int(blank_size.sum())} | filled_from_title={filled_n}")

    _title_size = extract_size_from_title_series(df["title"])
    diff_mask = ((_title_size != "") & (df["size"].fillna("") != _title_size))
    if int(diff_mask.sum()) > 0:
        logger.debug(f"[debug:size] size_from_title encontrados={int((_title_size!='').sum())} | diffs={int(diff_mask.sum())}")

    df["canonical_key"] = (
        df["brand"].fillna("").astype(str).str.strip() + "|" +
        df["size"].fillna("").astype(str).str.strip() + "|" +
        df["model"].fillna("").astype(str).str.strip()
    ).str.strip("|")

    df["marketplace"] = df["marketplace"].astype(str).fillna("").str.strip()
    if "url" in df.columns:
        mkt_from_url = df["url"].astype(str).str.extract(
            r"https?://(?:www\.)?([a-z0-9\-]+)\.", expand=False
        )
        bad_mkt = (df["marketplace"] == "") | (df["marketplace"].apply(norm_text) == df["seller_norm"])
        if int(bad_mkt.sum()) > 0:
            logger.debug(f"[canonical] marketplace corrigido via URL em {int(bad_mkt.sum())} linhas")
        df.loc[bad_mkt, "marketplace"] = mkt_from_url[bad_mkt].fillna(df.loc[bad_mkt, "marketplace"])

    try:
        dt = parse_datetime_series(df["collected_at"])
        df["collected_at"] = dt.dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        df["collected_at"] = df["collected_at"].astype(str)

    has_title = df["title"].astype(str).str.len() > 0
    has_url = df["url"].astype(str).str.len() > 0
    df = df[has_title | has_url].copy()

    has_url_mask = df["url"].notna() & (df["url"].astype(str) != "")
    df["_collected_str"] = df["collected_at"].fillna("").astype(str)
    df["_source_str"] = df["source_file"].fillna("").astype(str)

    df["_dedup_key"] = np.where(
        has_url_mask,
        df["marketplace"].fillna("").astype(str) + "|" +
        df["url"].astype(str) + "|" +
        df["seller_norm"] + "|" +
        df["_collected_str"] + "|" +
        df["price"].astype(str),
        df["marketplace"].fillna("").astype(str) + "|" +
        df["seller_norm"] + "|" +
        df["title_norm"].fillna("") + "|" +
        df["price"].astype(str) + "|" +
        df["_source_str"]
    )
    before = len(df)
    df = df.drop_duplicates(subset=["_dedup_key"]).copy()
    removed = before - len(df)
    if removed:
        logger.info(f"[dedupe] removidas={removed} | final={len(df)}")

    df.drop(columns=["_dedup_key","title_norm","seller_norm","_collected_str","_source_str"],
            inplace=True, errors="ignore")
    for k in keep:
        if k not in df.columns:
            df[k] = pd.NA

    logger.info(f"[canonical] output={len(df)}")
    return df[keep]

# -----------------------------
# Agregados
# -----------------------------
def summarize_canonical(unified: pd.DataFrame) -> pd.DataFrame:
    if unified.empty:
        return pd.DataFrame(columns=[
            "canonical_key","brand","model","size","n_listings","marketplaces",
            "min_price","max_price","mean_price","median_price","p10","p90","media_correta",
            "evidence_files"
        ])

    def _agg_group(g: pd.DataFrame) -> pd.Series:
        prices = g["price"].astype(float).dropna().values
        sources = sorted(set([s for s in g.get("source_file", pd.Series(dtype=str)).dropna().astype(str) if s]))
        if len(prices) == 0:
            return pd.Series({
                "n_listings": len(g),
                "marketplaces": [],
                "min_price": np.nan, "max_price": np.nan, "mean_price": np.nan,
                "median_price": np.nan, "p10": np.nan, "p90": np.nan,
                "media_correta": np.nan,
                "evidence_files": ",".join(sources)
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
            "media_correta": media_correta,
            "evidence_files": ",".join(sources)
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
def process_input_folders(input_dirs: List[Path]) -> pd.DataFrame:
    logger = logging.getLogger("unify")
    all_norm = []
    for in_dir in input_dirs:
        if not in_dir.exists() or not in_dir.is_dir():
            logger.warning(f"[process] Pasta ignorada: {in_dir}")
            continue
        files = discover_files(in_dir)
        logger.info(f"[process] {in_dir} => CSV={len(files['csv'])} JSON={len(files['json'])} SQLITE={len(files['sqlite'])}")

        for p in files["csv"]:
            try:
                df = load_csv(p, base_dir=in_dir)
                norm = normalize_columns(df)
                built = build_canonical(norm)
                if not built.empty:
                    all_norm.append(built)
                logger.info(f"[OK] CSV: {p.name} -> {len(built)} linhas")
            except Exception as e:
                logger.warning(f"[WARN] CSV {p}: {e}")

        for p in files["json"]:
            try:
                df = load_json(p, base_dir=in_dir)
                norm = normalize_columns(df)
                built = build_canonical(norm)
                if not built.empty:
                    all_norm.append(built)
                logger.info(f"[OK] JSON: {p.name} -> {len(built)} linhas")
            except Exception as e:
                logger.warning(f"[WARN] JSON {p}: {e}")

        for p in files["sqlite"]:
            try:
                tables = read_sqlite_tables(p, base_dir=in_dir)
                count_file = 0
                for tname, df in tables.items():
                    if df is None or df.empty:
                        continue
                    norm = normalize_columns(df)
                    built = build_canonical(norm)
                    if not built.empty:
                        all_norm.append(built)
                        count_file += len(built)
                logger.info(f"[OK] SQLite: {p.name} -> {count_file} linhas (somando tabelas)")
            except Exception as e:
                logger.warning(f"[WARN] SQLite {p}: {e}")

    if not all_norm:
        logger.error("[ERRO] Nenhum dado útil foi lido.")
        return pd.DataFrame(columns=["marketplace","title","price","url","brand","model","size","canonical_key","collected_at","seller"])

    unified = pd.concat(all_norm, ignore_index=True, sort=False)
    u = unified.copy()
    u["collected_at_date"] = pd.to_datetime(u["collected_at"], errors="coerce").dt.strftime("%Y-%m-%d")

    def norm_txt(s):
        import re, unicodedata
        s = "" if s is None else str(s).strip().lower()
        s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
        s = re.sub(r"\s+", " ", s)
        return s

    u["url_norm"]    = u["url"].fillna("").astype(str).str.strip().str.lower().str.replace(r"#.*$","",regex=True)
    u["seller_norm"] = u["seller"].map(norm_txt)
    u["title_norm"]  = u["title"].map(norm_txt)

    with_url = (u["url_norm"]!="")
    key_url = (u["marketplace"].fillna("").astype(str) + "|" +
            u["url_norm"] + "|" +
            u["seller_norm"] + "|" +
            u["collected_at_date"] + "|" +
            u["price"].fillna(-1).astype(str))

    key_no_url = (u["marketplace"].fillna("").astype(str) + "|" +
                u["seller_norm"] + "|" +
                u["title_norm"] + "|" +
                u["price"].fillna(-1).astype(str) + "|" +
                u["source_file"].fillna(""))

    u["_dedup_global"] = np.where(with_url, key_url, key_no_url)
    u = u.drop_duplicates(subset=["_dedup_global"]).copy()
    unified["price"] = unified["price"].astype(float)
    logger.info(f"[process] TOTAL UNIFIED: {len(unified)} linhas")
    return unified

def apply_filters(unified: pd.DataFrame, only_brand: str, only_size: str, only_model: str) -> pd.DataFrame:
    df = unified
    if only_brand:
        brand_in = norm_text(only_brand)
        df = df[df["brand"].fillna("").apply(norm_text) == brand_in]
    if only_size:
        sz_in = norm_text(only_size).replace("-", "/").replace(" r", "r").upper()
        sz_in = sz_in.replace("//", "/").replace("R/", "R")
        df = df[df["size"].fillna("").str.upper() == sz_in]
    if only_model:
        model_in = norm_text(only_model)
        df = df[df["model"].fillna("").apply(norm_text) == model_in]
    return df

def summarize_and_save(unified: pd.DataFrame, out_path: Path, append: bool):
    logger = logging.getLogger("unify")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    mode = "append" if append and out_path.exists() else "replace"
    key = "uid" if "uid" in unified.columns else None
    if key:
        unified = unified.drop_duplicates(subset=[key]).copy()

    con = sqlite3.connect(str(out_path))
    try:
        unified.to_sql("unified_listings", con, if_exists=mode, index=False)
        try:
            con.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_unified ON unified_listings (marketplace, url, seller, collected_at, price)")
        except Exception:
            pass
        summary = summarize_canonical(unified)
        summary.to_sql("canonical_summary", con, if_exists=mode, index=False)
    finally:
        con.close()
    logger.info(f"[save] {len(unified)} linhas (unified_listings) | {len(summary)} linhas (canonical_summary) -> {out_path}")
    print(f"[DONE] Salvo em: {out_path}  (unified_listings={len(unified)} linhas, summary={len(summary)} linhas)")

def _is_file_target(p: Path) -> bool:
    return p.suffix.lower() in {".db", ".sqlite"}

def _safe_part(x: str) -> str:
    try:
        return safe_part(x)
    except Exception:
        s = ("" if x is None else str(x)).strip()
        s = re.sub(r"[^\w\-.]+", "_", s)
        s = re.sub(r"_+", "_", s).strip("_")
        if not s:
            s = "NA"
        return s[:120]

def save_partitioned(
    unified: pd.DataFrame,
    output: Path,
    split_by: Optional[List[str]] = None,
    append: bool = False,
    filename_template: Optional[str] = None,
    suffix: str = ".sqlite",
):
    logger = logging.getLogger("unify")
    output = Path(output)

    # Caso 1: arquivo único
    if _is_file_target(output) or not split_by:
        output.parent.mkdir(parents=True, exist_ok=True)
        mode_append = append or output.exists()
        summarize_and_save(unified, output, append=mode_append)
        logger.info(f"[save_partitioned] {len(unified)} linhas -> {output}")
        return {output}

    # Caso 2: pasta + particionamento
    out_dir = output
    out_dir.mkdir(parents=True, exist_ok=True)
    created = set()

    for col in split_by:
        if col not in unified.columns:
            unified[col] = pd.NA

    for keys, group in unified.groupby(split_by, dropna=False, sort=False):
        keys = keys if isinstance(keys, tuple) else (keys,)
        ctx = {col: (None if (val is None or (isinstance(val, float) and np.isnan(val))) else str(val))
               for col, val in zip(split_by, keys)}

        if filename_template:
            name = filename_template.format(**{k: _safe_part(v) if v is not None else "NA" for k, v in ctx.items()})
            if not name.lower().endswith((".db", ".sqlite")):
                name += suffix
        else:
            parts = [f"{col}={_safe_part(val) if val is not None else 'NA'}" for col, val in ctx.items()]
            name = "__".join(parts) + suffix

        out_path = out_dir / name
        mode_append = append or out_path.exists()
        summarize_and_save(group, out_path, append=mode_append)
        created.add(out_path)
        logger.info(f"[save_partitioned] {len(group)} linhas -> {out_path}")

    return created

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
    ap.add_argument("--log-file", default="./data/unificadoDB/unify.log")
    ap.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR","CRITICAL"])

    args = ap.parse_args()

    logger = setup_logging(args.log_file, args.log_level)
    logger.info("==== Início da unificação ====")
    logger.info(f"Inputs: {args.input} | Output: {args.output}")

    if args.config:
        p = Path(args.config).expanduser().resolve()
        if not p.exists():
            logger.warning(f"[config] --config não encontrado: {p}. Usando defaults embutidos.")
        else:
            try:
                with open(p, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
                for k in ("known_brands","brand_aliases","known_model_phrases","model_aliases"):
                    if k in cfg:
                        CONFIG[k] = cfg[k]
                logger.info("[config] arquivo carregado com sucesso.")
            except Exception as e:
                logger.warning(f"[config] Falha ao ler --config: {e}. Usando defaults.")
    apply_config_lowerdedup()

    input_dirs = [Path(p).expanduser().resolve() for p in args.input]
    out = Path(args.output).expanduser().resolve()

    logger.info(f"[cwd] {Path.cwd()}")
    for dd in input_dirs:
        logger.info(f"[input] {dd}")

    unified = process_input_folders(input_dirs)
    if unified.empty:
        logger.error("[ERRO] Nada para salvar.")
        print("[ERRO] Nada para salvar.")
        sys.exit(2)

    unified = apply_filters(unified, args.only_brand, args.only_size, args.only_model)
    if unified.empty:
        logger.error("[ERRO] Nada após filtros. Ajuste --only-brand/--only-size/--only-model.")
        print("[ERRO] Nada após filtros. Ajuste --only-brand/--only-size/--only-model.")
        sys.exit(3)

    if args.split_by:
        split_cols = [c.strip().lower() for c in args.split_by.split(",") if c.strip()]
        for c in split_cols:
            if c not in {"brand","size","model"}:
                logger.error(f"[ERRO] Coluna inválida em --split-by: {c}. Use brand,size,model.")
                print(f"[ERRO] Coluna inválida em --split-by: {c}. Use brand,size,model.")
                sys.exit(4)
        if out.suffix:
            logger.error("[ERRO] Quando usar --split-by, --output deve ser uma PASTA.")
            print("[ERRO] Quando usar --split-by, --output deve ser uma PASTA.")
            sys.exit(5)
        save_partitioned(unified, out, split_cols, append=args.append)
    else:
        if out.exists() and not args.append:
            out.unlink()
        summarize_and_save(unified, out, append=args.append)

if __name__ == "__main__":
    main()
