import argparse
import datetime as dt
import json
import math
import re
import pytz
import os
import sqlite3
import math
import sys
import requests
from collections import Counter, defaultdict
from pathlib import Path
from pytz import timezone
from tqdm import tqdm


KIT_REGEX = re.compile(
    r"(kit|jogo|par|dupla|conjunto|combo|lote|cx|cx\.|caixa|c\/|c/|c x|pack|pckt|pacote|4x|2x|5x|6x|8x|10x|x2|x4|x6|x8|par de|jogo de)",
    flags=re.IGNORECASE
)
MULTI_QTY_REGEX = re.compile(r"(\b\d{1,2}\s?(un|uni|unid|unidades|pçs|pcs|peças)\b)", flags=re.IGNORECASE)
DIM_CONFUSOR_REGEX = re.compile(r"\b(aro\s?\d{2}|rin\s?\d{2})\b", flags=re.IGNORECASE)  # aro/rin extra pode confundir

def _to_float(x):
    try:
        if x is None:
            return None
        if isinstance(x, str) and not x.strip():
            return None
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None

def precheck_price_sanity(stats: dict) -> list[str]:
    alerts = []

    n = int(stats.get("n_listings") or 0)
    if n <= 0:
        return ["precheck_no_listings"]

    min_p = _to_float(stats.get("min_price"))
    max_p = _to_float(stats.get("max_price"))
    mean  = _to_float(stats.get("mean_price"))
    median = _to_float(stats.get("median") or stats.get("median_price"))
    p10   = _to_float(stats.get("p10"))
    p90   = _to_float(stats.get("p90"))

    if min_p is None or max_p is None:
        alerts.append("precheck_missing_min_max")
    else:
        if min_p <= 0:
            alerts.append("precheck_nonpositive_min")
        if max_p < min_p:
            alerts.append("precheck_max_less_than_min")
        elif min_p > 0 and (max_p / min_p) > 3.5:
            alerts.append("precheck_high_spread")

    if mean is not None and median is not None and median != 0:
        if abs(mean - median) / abs(median) > 0.25:
            alerts.append("precheck_mean_median_divergence")

    if p10 is not None and p90 is not None and p10 > 0:
        if (p90 / p10) > 3.0:
            alerts.append("precheck_wide_p10_p90")

    if n < 4:
        alerts.append("precheck_low_sample_reliability")

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
    rows = conn.execute(
        "SELECT title, price, seller, marketplace, url FROM unified_listings WHERE canonical_key = ?",
        (canonical_key,)
    ).fetchall()
    if not rows:
        return {"titles": [], "sellers_top": [], "examples": []}

    prices = sorted([r["price"] for r in rows if r["price"] is not None])
    examples = []
    def pick_example_by_price(target):
        if not prices:
            return None
        best = min(rows, key=lambda r: abs((r["price"] or 0) - target))
        return dict(best)

    if prices:
        examples.append(pick_example_by_price(prices[0]))           
        examples.append(pick_example_by_price(prices[len(prices)//2]))  
        examples.append(pick_example_by_price(prices[-1]))          

    titles = [r["title"] for r in rows if r["title"]]
    sellers = [r["seller"] for r in rows if r["seller"]]
    top_titles = [t for t, _ in Counter(titles).most_common(k_titles)]
    top_sellers = [s for s, _ in Counter(sellers).most_common(k_sellers)]

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
            "media_correta": stats.get("media_correta")
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


def _clear_screen():
    try:
        os.system("cls" if os.name == "nt" else "clear")
    except Exception:
        pass

def main():
    ap = argparse.ArgumentParser(description="Dupla confirmação com LLM (Ollama) para pneus")

    ap.add_argument("--out-dir", default="data/AI",
                    help="Pasta onde salvar JSONL e o SQLite de auditoria (default: data/AI)")
    ap.add_argument("--out-db", default=None,
                    help="Caminho do SQLite de auditoria (default: <out-dir>/ai_audit.db)")
    ap.add_argument("--db", required=True,
                    help="Caminho do SQLite unificado (saída do unificador)")
    ap.add_argument("--out", default=None,
                    help="Caminho do JSONL de auditoria (default: <out-dir>/audit_<timestamp>.jsonl)")
    ap.add_argument("--model", default="llama3.2:3b",
                    help="Modelo no Ollama (ex.: llama3.2:1b, llama3.2:3b)")
    ap.add_argument("--ollama-host", default="http://localhost:11434",
                    help="Host do Ollama")
    ap.add_argument("--sample-titles", type=int, default=5,
                    help="Qtde de títulos representativos")
    ap.add_argument("--sample-sellers", type=int, default=3,
                    help="Qtde de sellers representativos")
    ap.add_argument("--max-tokens", type=int, default=256,
                    help="Limite de tokens de saída (num_predict)")
    ap.add_argument("--timeout", type=int, default=60,
                    help="Timeout em segundos do request ao Ollama")
    ap.add_argument("--only-brand")
    ap.add_argument("--only-size")
    ap.add_argument("--only-model")
    ap.add_argument("--no-sqlite-write", action="store_true",
                    help="Não escreve na tabela ai_audit (apenas JSONL)")
    ap.add_argument("--append", action="store_true",
                    help="Anexa no JSONL em vez de sobrescrever")
    ap.add_argument("--quiet", action="store_true",
                    help="Suprime a barra de progresso e mensagens intermediárias; mostra só o resumo final.")
    ap.add_argument("--clear-screen", action="store_true",
                    help="Limpa o terminal antes do resumo final.")
    ap.add_argument("--make-final", action="store_true",
                    help="Gera final.db + unified_listings_final.csv + relatórios após auditar")
    ap.add_argument("--final-db", default=r".\data\AI\final.db",
                    help="Arquivo SQLite final (soft-delete)")
    ap.add_argument("--final-csv", default=r".\data\AI\unified_listings_final.csv",
                    help="CSV final (soft-delete)")
    ap.add_argument("--final-threshold", type=float, default=0.70,
                    help="Confiança mínima do LLM para manter (default 0.70)")
    ap.add_argument("--no-precheck-filter", action="store_true",
                    help="Se presente, NÃO exclui por precheck_alerts")
    ap.add_argument("--reports-dir", default=None,
                    help="Pasta dos relatórios CSV (default: <out-dir>/reports)")

    args = ap.parse_args()

    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.out_db is None:
        args.out_db = (out_dir / "ai_audit.db").as_posix()
    audit_db_path = Path(args.out_db).expanduser().resolve()
    audit_db_path.parent.mkdir(parents=True, exist_ok=True)

    brazil_tz = pytz.timezone("America/Sao_Paulo")
    now_brazil = dt.datetime.now(brazil_tz)
    ts = now_brazil.strftime("%Y%m%d_%H%M%S")
    out_path = Path(args.out) if args.out else (out_dir / f"audit_{ts}.jsonl")
    if out_path.exists() and not args.append:
        out_path.unlink()

    reports_dir = Path(args.reports_dir).expanduser().resolve() if args.reports_dir else (out_dir / "reports")
    reports_dir.mkdir(parents=True, exist_ok=True)

    if args.make_final and args.no_sqlite_write:
        if args.clear_screen:
            _clear_screen()
        print("[ERRO] --make-final requer escrita no SQLite de auditoria (remova --no-sqlite-write).")
        sys.exit(9)

    conn_uni = connect_sqlite(args.db)
    conn_audit = connect_sqlite(audit_db_path.as_posix())
    ensure_ai_audit_table(conn_audit)

    rows = fetch_canonical_rows(conn_uni, args.only_brand, args.only_size, args.only_model)
    if not rows:
        if args.clear_screen:
            _clear_screen()
        print("Nenhum registro em canonical_summary com os filtros dados.")
        return

    processed = 0
    with out_path.open("a", encoding="utf-8") as fo:
        for r in tqdm(rows, desc="Auditing", unit="item", disable=args.quiet, leave=False):
            canonical_key = r["canonical_key"]
            brand = r["brand"]; model = r["model"]; size = r["size"]
            stats = {
                "n_listings": r["n_listings"],
                "min_price": r["min_price"],
                "max_price": r["max_price"],
                "mean_price": r["mean_price"],
                "median_price": r["median_price"],
                "median": r["median_price"],     # compat
                "p10": r["p10"],
                "p90": r["p90"],
                "media_correta": r["media_correta"],
                "marketplaces": r["marketplaces"]
            }

            sample = fetch_listings_sample(conn_uni, canonical_key,
                                           k_titles=args.sample_titles,
                                           k_sellers=args.sample_sellers)

            pre_alerts = precheck_price_sanity(stats)
            pre_alerts += precheck_title_flags(sample.get("titles", []))
            if stats["n_listings"] and stats["n_listings"] < 4:
                pre_alerts.append("precheck_low_sample_reliability")

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
                upsert_ai_audit(conn_audit, record)

            processed += 1

    try:
        conn_uni.close()
    except Exception:
        pass
    try:
        conn_audit.close()
    except Exception:
        pass

    final_summary = None
    if args.make_final:
        try:
            from Unificador_DB.finalize import generate_final
        except Exception as e:
            if args.clear_screen:
                _clear_screen()
            print(f"[ERRO] finalize.generate_final não encontrado: {e}")
            sys.exit(11)

        try:
            final_summary = generate_final(
                unified_db=Path(args.db).expanduser().resolve().as_posix(),
                audit_db=audit_db_path.as_posix(),
                final_db=Path(args.final_db).expanduser().resolve().as_posix(),
                final_csv=Path(args.final_csv).expanduser().resolve().as_posix(),
                threshold=float(args.final_threshold),
                exclude_precheck_alerts=(not args.no_precheck_filter),
                reports_dir=reports_dir.as_posix(),
                quiet=True,
                clear_screen=False,
            )
        except Exception as e:
            if args.clear_screen:
                _clear_screen()
            print(f"[ERRO] Erro ao gerar final/relatórios: {e}")
            sys.exit(12)

    if args.clear_screen:
        _clear_screen()

    if final_summary:
        print(
            f"[FINAL] kept={final_summary['kept']} | dropped={final_summary['dropped']} | total={final_summary['total']} "
            f"| final.db={Path(args.final_db).as_posix()} | csv={Path(args.final_csv).as_posix()} "
            f"| reports={reports_dir.as_posix()}"
        )
    else:
        try:
            con = sqlite3.connect(audit_db_path.as_posix())
            n_total = con.execute("SELECT COUNT(*) FROM ai_audit;").fetchone()[0]
            con.close()
        except Exception:
            n_total = "?"
        print(
            f"[AUDIT-ONLY] rows_this_run={processed} | ai_audit.db={audit_db_path.as_posix()} "
            f"| audit.jsonl={out_path.as_posix()} | rows_in_db={n_total}"
        )

if __name__ == "__main__":
    main()
