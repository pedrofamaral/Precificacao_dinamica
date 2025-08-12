import argparse
import datetime as dt
import json
import math
import re
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

import requests
from tqdm import tqdm

# -----------------------------
# Heurísticas simples (pré-LLM)
# -----------------------------

KIT_REGEX = re.compile(
    r"(kit|jogo|par|dupla|conjunto|combo|lote|cx|cx\.|caixa|c\/|c/|c x|pack|pckt|pacote|4x|2x|5x|6x|8x|10x|x2|x4|x6|x8|par de|jogo de)",
    flags=re.IGNORECASE
)
MULTI_QTY_REGEX = re.compile(r"(\b\d{1,2}\s?(un|uni|unid|unidades|pçs|pcs|peças)\b)", flags=re.IGNORECASE)
DIM_CONFUSOR_REGEX = re.compile(r"\b(aro\s?\d{2}|rin\s?\d{2})\b", flags=re.IGNORECASE)  # aro/rin extra pode confundir

def precheck_price_sanity(stats):
    alerts = []

    p10 = stats.get("p10")
    p90 = stats.get("p90")
    med = stats.get("median")
    tmean = stats.get("trimmed_mean_10_90")
    minp = stats.get("min_price")
    maxp = stats.get("max_price")
    n = stats.get("n_listings", 0)

    # Monotonia básica
    if not (stats.get("min_price") <= p10 <= med <= p90 <= stats.get("max_price")):
        alerts.append("precheck_price_monotony_violation")

    # trimmed_mean dentro de [p10, p90]
    if tmean is not None and (p10 is not None and p90 is not None):
        if not (p10 <= tmean <= p90):
            alerts.append("precheck_trimmed_mean_outside_p10_p90")

    # P90 - P10 vs mediana (dispersão muito alta)
    if all(v is not None for v in [p10, p90, med]) and (p90 - p10) > 0.6 * med:
        alerts.append("precheck_high_dispersion_between_p10_p90")

    # Amostra pequena: estatísticas pouco confiáveis
    if n < 4:
        alerts.append("precheck_small_sample_size")

    # Outliers gritantes (min ou max muito distante do intervalo interquantil)
    if all(v is not None for v in [minp, p10]):
        if minp < 0.7 * p10:
            alerts.append("precheck_min_far_below_p10")
    if all(v is not None for v in [maxp, p90]):
        if maxp > 1.3 * p90:
            alerts.append("precheck_max_far_above_p90")

    return alerts

def precheck_title_flags(titles):
    alerts = []
    kit_hits = any(KIT_REGEX.search(t) for t in titles)
    multi_hits = any(MULTI_QTY_REGEX.search(t) for t in titles)
    dim_confusor_hits = any(DIM_CONFUSOR_REGEX.search(t) for t in titles)
    if kit_hits:
        alerts.append("precheck_title_maybe_kit_or_multiunit")
    if multi_hits:
        alerts.append("precheck_title_explicit_multiunit")
    if dim_confusor_hits:
        alerts.append("precheck_title_extra_rim_dimension")
    return alerts

# -----------------------------
# Banco de Dados
# -----------------------------

def connect_sqlite(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn

def fetch_canonical_rows(conn, only_brand=None, only_size=None, only_model=None):
    base = "SELECT * FROM canonical_summary"
    clauses = []
    params = []
    if only_brand:
        clauses.append("brand = ?")
        params.append(only_brand)
    if only_size:
        clauses.append("size = ?")
        params.append(only_size)
    if only_model:
        clauses.append("model = ?")
        params.append(only_model)
    if clauses:
        base += " WHERE " + " AND ".join(clauses)
    base += " ORDER BY brand, size, model"
    cur = conn.execute(base, params)
    return cur.fetchall()

def fetch_listings_sample(conn, canonical_key, k_titles=5, k_sellers=3):
    # Amostra representativa: min, mediana, max + títulos mais frequentes
    rows = conn.execute(
        "SELECT title, price, seller, marketplace, url FROM unified_listings WHERE canonical_key = ?",
        (canonical_key,)
    ).fetchall()
    if not rows:
        return {"titles": [], "sellers_top": [], "examples": []}

    prices = sorted([r["price"] for r in rows if r["price"] is not None])
    examples = []
    def pick_example_by_price(target):
        # pega o mais próximo do preço alvo
        if not prices:
            return None
        best = min(rows, key=lambda r: abs((r["price"] or 0) - target))
        return dict(best)

    if prices:
        examples.append(pick_example_by_price(prices[0]))           # mínimo
        examples.append(pick_example_by_price(prices[len(prices)//2]))  # mediano
        examples.append(pick_example_by_price(prices[-1]))          # máximo

    # Títulos e vendedores mais frequentes
    titles = [r["title"] for r in rows if r["title"]]
    sellers = [r["seller"] for r in rows if r["seller"]]
    top_titles = [t for t, _ in Counter(titles).most_common(k_titles)]
    top_sellers = [s for s, _ in Counter(sellers).most_common(k_sellers)]

    # Remove None e duplicatas em examples
    seen_urls = set()
    dedup_examples = []
    for ex in examples:
        if not ex or not ex.get("url"):
            continue
        if ex["url"] in seen_urls:
            continue
        seen_urls.add(ex["url"])
        dedup_examples.append(ex)

    return {
        "titles": top_titles,
        "sellers_top": top_sellers,
        "examples": dedup_examples[:3],
        "n_total": len(rows)
    }

# -----------------------------
# Ollama
# -----------------------------

def call_ollama_generate(
    host="http://localhost:11434",
    model="llama3",
    prompt="",
    temperature=0.0,
    top_p=0.9,
    seed=42,
    max_tokens=None,
    timeout=60
):
    url = f"{host.rstrip('/')}/api/generate"
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": temperature,
            "top_p": top_p,
            "seed": seed
        }
    }
    if max_tokens is not None:
        payload["options"]["num_predict"] = max_tokens

    resp = requests.post(url, json=payload, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    # Ollama /api/generate retorna {"response": "...", ...}
    return data.get("response", "").strip(), data

# -----------------------------
# Prompt
# -----------------------------

SCHEMA_INSTRUCTIONS = """
Responda ESTRITAMENTE em JSON minificado, no formato:
{"ok":true|false,"alerts":[<strings>],"confidence":0..1}

Onde:
- "ok" = true se os dados parecem consistentes (normalização/estatísticas) e não há indícios fortes de problema.
- "alerts" = lista de códigos curtos. Use somente dos exemplos abaixo quando aplicável (pode incluir outros quando necessário):
  - "brand_title_mismatch"
  - "possible_kit_or_multiunit"
  - "ambiguous_model_grouping"
  - "outlier_prices_remaining"
  - "suspicious_trimmed_mean"
  - "inconsistent_titles_vs_size"
  - "seller_cluster_risk"
  - "low_sample_reliability"
- "confidence" = sua confiança na avaliação (0..1).
NÃO inclua texto fora do JSON. NÃO explique.
""".strip()

def build_prompt(brand, model, size, stats, sample):
    # Compacto para reduzir tokens
    obj = {
        "brand": brand,
        "model": model,
        "size": size,
        "n_listings": stats.get("n_listings"),
        "prices": {
            "min": stats.get("min_price"),
            "p10": stats.get("p10"),
            "median": stats.get("median"),
            "p90": stats.get("p90"),
            "max": stats.get("max_price"),
            "trimmed_mean_10_90": stats.get("trimmed_mean_10_90")
        },
        "titles_sample": sample.get("titles", [])[:5],
        "sellers_top": sample.get("sellers_top", [])[:3],
        "examples": [
            {
                "title": e.get("title"),
                "price": e.get("price"),
                "seller": e.get("seller"),
                "marketplace": e.get("marketplace")
            } for e in sample.get("examples", [])
        ]
    }
    preface = (
        "Você é um auditor de catálogo de pneus. Avalie se a normalização (brand/model/size) e as estatísticas de preço "
        "estão consistentes para o grupo canônico informado. Procure: divergência de marca nos títulos, kits/múltiplas unidades, "
        "mistura de modelos parecidos que deveriam estar separados, e se os agregados (p10/p90/mediana/trimmed_mean) "
        "fazem sentido dado o número de ofertas. Se notar outliers remanescentes, sinalize."
    )
    return f"""{preface}

DADOS:
{json.dumps(obj, ensure_ascii=False)}

{SCHEMA_INSTRUCTIONS}
"""

# -----------------------------
# Persistência (JSONL e SQLite)
# -----------------------------

def ensure_ai_audit_table(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS ai_audit (
        canonical_key TEXT PRIMARY KEY,
        brand TEXT,
        model TEXT,
        size TEXT,
        n_listings INTEGER,
        stats_json TEXT,
        titles_sample_json TEXT,
        sellers_top_json TEXT,
        examples_json TEXT,
        precheck_alerts_json TEXT,
        llm_ok INTEGER,
        llm_alerts_json TEXT,
        llm_confidence REAL,
        llm_model TEXT,
        llm_raw_response TEXT,
        created_at TEXT
    )
    """)
    conn.commit()

def upsert_ai_audit(conn, rec):
    conn.execute("""
    INSERT INTO ai_audit (
        canonical_key, brand, model, size, n_listings, stats_json, titles_sample_json,
        sellers_top_json, examples_json, precheck_alerts_json,
        llm_ok, llm_alerts_json, llm_confidence, llm_model, llm_raw_response, created_at
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(canonical_key) DO UPDATE SET
        brand=excluded.brand,
        model=excluded.model,
        size=excluded.size,
        n_listings=excluded.n_listings,
        stats_json=excluded.stats_json,
        titles_sample_json=excluded.titles_sample_json,
        sellers_top_json=excluded.sellers_top_json,
        examples_json=excluded.examples_json,
        precheck_alerts_json=excluded.precheck_alerts_json,
        llm_ok=excluded.llm_ok,
        llm_alerts_json=excluded.llm_alerts_json,
        llm_confidence=excluded.llm_confidence,
        llm_model=excluded.llm_model,
        llm_raw_response=excluded.llm_raw_response,
        created_at=excluded.created_at
    """, (
        rec["canonical_key"], rec["brand"], rec["model"], rec["size"], rec["n_listings"],
        json.dumps(rec["stats"], ensure_ascii=False),
        json.dumps(rec["titles_sample"], ensure_ascii=False),
        json.dumps(rec["sellers_top"], ensure_ascii=False),
        json.dumps(rec["examples"], ensure_ascii=False),
        json.dumps(rec["precheck_alerts"], ensure_ascii=False),
        1 if rec["llm_ok"] else 0,
        json.dumps(rec["llm_alerts"], ensure_ascii=False),
        rec["llm_confidence"],
        rec["llm_model"],
        rec["llm_raw_response"],
        rec["created_at"]
    ))
    conn.commit()

# -----------------------------
# Main
# -----------------------------

def main():
    ap = argparse.ArgumentParser(description="Dupla confirmação com LLM (Ollama) para pneus")
    ap.add_argument("--db", required=True, help="Caminho do SQLite unificado (saída do unificador)")
    ap.add_argument("--model", default="llama3", help="Modelo no Ollama (ex.: llama3, mistral)")
    ap.add_argument("--ollama-host", default="http://localhost:11434", help="Host do Ollama")
    ap.add_argument("--sample-titles", type=int, default=5, help="Qtde de títulos representativos")
    ap.add_argument("--sample-sellers", type=int, default=3, help="Qtde de sellers representativos")
    ap.add_argument("--max-tokens", type=int, default=256, help="Limite de tokens de saída (num_predict)")
    ap.add_argument("--timeout", type=int, default=60, help="Timeout em segundos do request ao Ollama")
    ap.add_argument("--only-brand")
    ap.add_argument("--only-size")
    ap.add_argument("--only-model")
    ap.add_argument("--no-sqlite-write", action="store_true", help="Não escreve na tabela ai_audit")
    ap.add_argument("--append", action="store_true", help="Anexa no JSONL em vez de sobrescrever")
    ap.add_argument("--out", default=None, help="Caminho do arquivo JSONL de auditoria. Default: audit_<timestamp>.jsonl")
    args = ap.parse_args()

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = Path(args.out) if args.out else Path(f"audit_{ts}.jsonl")
    if out_path.exists() and not args.append:
        out_path.unlink()

    conn = connect_sqlite(args.db)
    ensure_ai_audit_table(conn)

    rows = fetch_canonical_rows(conn, args.only_brand, args.only_size, args.only_model)
    if not rows:
        print("Nenhum registro em canonical_summary com os filtros dados.")
        return

    with out_path.open("a", encoding="utf-8") as fo:
        for r in tqdm(rows, desc="Auditing", unit="item"):
            canonical_key = r["canonical_key"]
            brand = r["brand"]; model = r["model"]; size = r["size"]
            stats = {
                "n_listings": r["n_listings"],
                "min_price": r["min_price"],
                "max_price": r["max_price"],
                "mean_price": r["mean_price"],
                "median": r["median_price"],
                "p10": r["p10"],
                "p90": r["p90"],
                "trimmed_mean_10_90": r["trimmed_mean_10_90"],
                "marketplaces": r["marketplaces"]
            }

            sample = fetch_listings_sample(conn, canonical_key, k_titles=args.sample_titles, k_sellers=args.sample_sellers)

            # Pré-checagens determinísticas
            pre_alerts = precheck_price_sanity(stats)
            pre_alerts += precheck_title_flags(sample.get("titles", []))
            if stats["n_listings"] and stats["n_listings"] < 4:
                pre_alerts.append("precheck_low_sample_reliability")

            # Prompt p/ LLM
            prompt = build_prompt(brand, model, size, stats, sample)

            try:
                text, raw = call_ollama_generate(
                    host=args.ollama_host,
                    model=args.model,
                    prompt=prompt,
                    temperature=0.0,
                    top_p=0.9,
                    seed=42,
                    max_tokens=args.max_tokens,
                    timeout=args.timeout
                )
                # Força JSON estrito
                try:
                    llm_obj = json.loads(text)
                    llm_ok = bool(llm_obj.get("ok"))
                    llm_alerts = llm_obj.get("alerts", []) or []
                    llm_conf = float(llm_obj.get("confidence", 0.0))
                except Exception:
                    llm_ok = False
                    llm_alerts = ["llm_invalid_json_response"]
                    llm_conf = 0.0
            except Exception as e:
                text = f"LLM_ERROR: {e}"
                raw = {}
                llm_ok = False
                llm_alerts = ["llm_request_failed"]
                llm_conf = 0.0

            # Registro final
            record = {
                "canonical_key": canonical_key,
                "brand": brand,
                "model": model,
                "size": size,
                "n_listings": stats["n_listings"],
                "stats": stats,
                "titles_sample": sample.get("titles", []),
                "sellers_top": sample.get("sellers_top", []),
                "examples": sample.get("examples", []),
                "precheck_alerts": pre_alerts,
                "llm_ok": llm_ok,
                "llm_alerts": llm_alerts,
                "llm_confidence": llm_conf,
                "llm_model": args.model,
                "llm_raw_response": text,
                "created_at": dt.datetime.utcnow().isoformat() + "Z"
            }

            fo.write(json.dumps(record, ensure_ascii=False) + "\n")
            if not args.no_sqlite_write:
                upsert_ai_audit(conn, record)

    print(f"✓ Auditoria finalizada. Saída JSONL: {out_path}")
    if not args.no_sqlite_write:
        print("✓ Tabela 'ai_audit' atualizada no SQLite.")

if __name__ == "__main__":
    main()
