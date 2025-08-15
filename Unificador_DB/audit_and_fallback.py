import argparse, sqlite3, re, json, math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
import unicodedata


def norm_text(s: Any) -> str:
    if s is None or (isinstance(s,float) and math.isnan(s)): return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^\w\s/.\-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

SIZE_RES = [
    re.compile(r"(?<!\d)(\d{3})\s*[/\-]\s*(\d{2})\s*[rR]\s*(\d{2})(?!\d)"),
    re.compile(r"(?<!\d)(\d{3})\s*(\d{2})\s*[rR]\s*(\d{2})(?!\d)"),
    re.compile(r"(?<!\d)(\d{3})\s*[/\-]\s*(\d{2})\s*(\d{2})(?!\d)"),
]
KNOWN_BRANDS = ["goodyear","sumitomo","pirelli","continental","michelin","bridgestone","firestone","dunlop","hankook","yokohama"]
BRAND_ALIASES = {"good year":"goodyear","magazineluiza":"magalu"}
KNOWN_MODEL_PHRASES = ["assurance","bc20","f 600","f-600","primacy","eco contact","sp touring"]

def extract_size_from_text(text: str) -> str:
    t = norm_text(text)
    for rx in SIZE_RES:
        m = rx.search(t)
        if m: return f"{m.group(1)}/{m.group(2)}R{m.group(3)}"
    return ""

def extract_brand_from_text(text: str) -> str:
    t = norm_text(text)
    for alias, target in BRAND_ALIASES.items():
        if re.search(rf"(?<!\w){re.escape(alias)}(?!\w)", t): return target
    for b in KNOWN_BRANDS:
        if re.search(rf"(?<!\w){re.escape(b)}(?!\w)", t): return b
    return ""

def canon_model(m: str) -> str:
    m = norm_text(m)
    m = re.sub(r"\br\d{2}\b", " ", m)                 
    m = re.sub(r"\b\d{2,3}[hvtwyz]\b", " ", m)        
    m = re.sub(r"\b(xl|tl|tt|runflat|rft)\b", " ", m)
    m = re.sub(r"\s+", " ", m).strip(" -_/")
    return m

def extract_model_from_title(title: str, brand: str) -> str:
    t = norm_text(title)
    for phrase in KNOWN_MODEL_PHRASES:
        p = re.compile(rf"(?<![a-z0-9]){re.escape(norm_text(phrase))}(?![a-z0-9])")
        if p.search(t): return canon_model(phrase)
    if brand:
        t_sp = f" {t} "
        b = f" {brand} "
        if b in t_sp:
            after = t_sp.split(b, 1)[1].strip()
            for rx in SIZE_RES:
                m = rx.search(after)
                if m: after = after[:m.start()].strip(); break
            after = re.sub(r"[\/\-_,]+", " ", after)
            after = re.sub(r"\s+", " ", after)
            toks = [w for w in after.split() if w not in {"pneu","pneus","aro"}]
            if toks: return canon_model(" ".join(toks[:3]))
    return ""

# --------- Descoberta de tabelas e colunas ---------
def pick_unified_table(con: sqlite3.Connection) -> str:
    tbls = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", con)["name"].tolist()
    if "unified_listings" in tbls: return "unified_listings"
    best, best_n = None, -1
    for t in tbls:
        try:
            n = pd.read_sql_query(f"SELECT COUNT(*) n FROM {t}", con)["n"].iloc[0]
            if n > best_n: best, best_n = t, n
        except Exception: pass
    if not best: raise RuntimeError("Nenhuma tabela encontrada para auditoria.")
    return best

def find_evidence_candidates(con: sqlite3.Connection) -> List[Tuple[str, List[str]]]:
    tbls = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", con)["name"].tolist()
    name_hits = []
    for t in tbls:
        low = t.lower()
        score = 0
        for token in ["evidence","raw","html","json","page","response","files","snapshot"]:
            if token in low: score += 1
        cols = pd.read_sql_query(f"PRAGMA table_info({t})", con)["name"].tolist()
        text_cols = [c for c in cols if any(k in c.lower() for k in ["content","html","json","payload","body","raw","data","text","titulo","title"])]
        if score or text_cols:
            name_hits.append((t, text_cols))
    return name_hits

# --------- Leitura & parsing de uma linha de evidência ---------
def parse_evidence_row(row: pd.Series, text_cols: List[str]) -> Dict[str,str]:
    def pick(*keys):
        for k in keys:
            if k in row and isinstance(row[k], str) and row[k].strip():
                return str(row[k]).strip()
        return ""
    title = pick("title","titulo","name","produto","product_title")
    seller = pick("seller","vendedor","loja","seller_name")
    brand  = pick("brand","brand_raw","marca")
    model  = pick("model","model_raw","modelo")
    size   = pick("size","tamanho","medida")

    for c in text_cols:
        val = row.get(c)
        if isinstance(val, (bytes, bytearray)):
            try: val = val.decode("utf-8","ignore")
            except Exception: continue
        if isinstance(val, str) and ("{" in val or "[" in val):
            try:
                j = json.loads(val)
                if isinstance(j, dict):
                    title = title or str(j.get("title") or j.get("titulo") or "")
                    seller = seller or str(j.get("seller") or j.get("vendedor") or j.get("loja") or "")
                    brand = brand or str(j.get("brand") or j.get("marca") or j.get("brand_raw") or "")
                    model = model or str(j.get("model") or j.get("modelo") or j.get("model_raw") or "")
                    size = size or str(j.get("size") or j.get("tamanho") or j.get("medida") or "")
            except Exception:
                pass

    blob = " ".join([str(row.get(c,"")) for c in text_cols if c in row and isinstance(row[c], (str,bytes,bytearray))])
    if not brand: brand = extract_brand_from_text(title or blob)
    if not size:  size  = extract_size_from_text(title or blob)
    if not model: model = extract_model_from_title(title or blob, extract_brand_from_text(title or blob))

    return {
        "title": title or "",
        "seller": seller or "",
        "brand": brand or "",
        "model": canon_model(model or ""),
        "size": size or "",
    }

# --------- Auditoria e Fallback dentro do DB ---------
def audit_df(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    cols = [c for c in ["marketplace","seller","title","url","price","brand","model","size","collected_at","source_file"] if c in df.columns]
    miss = df[cols].isna() | (df[cols].astype(str)=="")
    miss_rate = miss.mean().rename("missing_rate").reset_index().rename(columns={"index":"column"})
    by_mkt = pd.DataFrame()
    if "marketplace" in df.columns:
        z = df.copy()
        for c in ["brand","model","size","seller"]:
            if c in z.columns: z[f"_missing_{c}"] = z[c].isna() | (z[c].astype(str)=="")
        agg = {"rows":("marketplace","size")}
        for c in ["brand","model","size","seller"]:
            if f"_missing_{c}" in z.columns:
                agg[f"missing_{c}_pct"] = (f"_missing_{c}", "mean")
        by_mkt = z.groupby("marketplace", dropna=False).agg(**agg).reset_index().sort_values("rows", ascending=False)
    sample_issues = df.loc[(df.get("brand","").astype(str)=="") | (df.get("model","").astype(str)=="") | (df.get("size","").astype(str)=="") , 
                           [c for c in ["marketplace","seller","title","url","price","brand","model","size","source_file"] if c in df.columns]].head(5000)
    return miss_rate, by_mkt, sample_issues

def run(db_path: Path, out_dir: Path, apply_updates: bool = False, sample_limit: int = 1_000_000):
    out_dir.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_path))

    target = pick_unified_table(con)
    df = pd.read_sql_query(f"SELECT * FROM {target} LIMIT {sample_limit}", con)

    miss_rate, by_mkt, issues = audit_df(df)
    miss_rate.to_csv(out_dir/"dq_report.csv", index=False)
    by_mkt.to_csv(out_dir/"dq_by_marketplace.csv", index=False)
    issues.to_csv(out_dir/"issues_sample.csv", index=False)

    candidates = find_evidence_candidates(con)
    if not candidates:
        print("[warn] Nenhuma tabela de evidência detectada no DB.")
        con.close()
        return

    if "url" not in df.columns:
        print("[warn] unified não possui coluna 'url' — fallback fica limitado.")
        con.close()
        return
    url_to_idx = df["url"].astype(str).reset_index().set_index("url")["index"]

    fixes = []
    for tbl, text_cols in candidates:
        cols = pd.read_sql_query(f"PRAGMA table_info({tbl})", con)["name"].tolist()
        url_cols = [c for c in cols if c.lower() in ("url","link","href","page_url","request_url")]
        ev = pd.read_sql_query(f"SELECT * FROM {tbl} LIMIT 200000", con)

        if url_cols:
            ev["url_key"] = ev[url_cols[0]].astype(str)
        else:
            continue

        if not text_cols:
            text_cols = [c for c in cols if any(k in c.lower() for k in ["content","html","json","payload","body","raw","data","titulo","title"])]
            if not text_cols:
                continue

        for _, r in ev.iterrows():
            u = str(r.get("url_key") or "")
            if not u or u not in url_to_idx: 
                continue
            idx = int(url_to_idx[u])
            got = parse_evidence_row(r, text_cols)
            filled = {}
            if df.loc[idx,"brand"] in (None,"") and got.get("brand"): filled["brand"] = got["brand"]
            if df.loc[idx,"model"] in (None,"") and got.get("model"): filled["model"] = got["model"]
            if df.loc[idx,"size"]  in (None,"") and got.get("size"):  filled["size"]  = got["size"]
            if df.loc[idx,"seller"] in (None,"") and got.get("seller"): filled["seller"] = got["seller"]
            if filled:
                fixes.append({"row_index": idx, "url": u, "source_table": tbl, **filled})

    con.close()

    if not fixes:
        print("[fallback] Nenhuma sugestão encontrada nas tabelas de evidência do DB.")
        return

    fixes_df = pd.DataFrame(fixes).drop_duplicates(subset=["row_index"])
    fixes_df.to_parquet(out_dir/"_fix_suggestions.parquet", index=False)
    fixes_df.to_csv(out_dir/"_fix_suggestions.csv", index=False)

    con = sqlite3.connect(str(db_path))
    try:
        fixes_df.to_sql("_fix_suggestions", con, if_exists="replace", index=False)
        if apply_updates:
            for col in ["brand","model","size","seller"]:
                con.execute(f"""
                    UPDATE {target}
                    SET {col} = COALESCE(NULLIF({col},''), (
                        SELECT {col} FROM _fix_suggestions s
                        WHERE s.url = {target}.url
                    ))
                    WHERE ({col} IS NULL OR {col}='') AND EXISTS (
                        SELECT 1 FROM _fix_suggestions s WHERE s.url = {target}.url
                    );
                """)
            con.commit()
    finally:
        con.close()

    summary = fixes_df[["brand","model","size","seller"]].replace("", np.nan).notna().mean().rename("fill_rate").reset_index().rename(columns={"index":"field"})
    summary.to_csv(out_dir/"fix_summary.csv", index=False)
    print("[OK] Relatórios gerados em:", out_dir)
    print(summary)
    if apply_updates:
        print("[OK] Correções aplicadas na tabela", target)
