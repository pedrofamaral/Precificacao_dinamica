import argparse
import datetime as dt
import json
import math
import os
import re
import sqlite3
import sys
from collections import Counter
from pathlib import Path

import pandas as pd
import pytz
import requests
from tqdm import tqdm

# ==============================================================================
# SEÇÃO DE FUNÇÕES DE APOIO
# ==============================================================================

KIT_REGEX = re.compile(
    r"(kit|jogo|par|dupla|conjunto|combo|lote|cx|cx\.|caixa|c\/|c\/|c x|pack|pckt|pacote|4x|2x|5x|6x|8x|10x|x2|x4|x6|x8|par de|jogo de)",
    flags=re.IGNORECASE,
)


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
    mean = _to_float(stats.get("mean_price"))
    median = _to_float(stats.get("median") or stats.get("median_price"))
    p10 = _to_float(stats.get("p10"))
    p90 = _to_float(stats.get("p90"))

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
    if any(KIT_REGEX.search(str(t)) for t in titles):
        alerts.append("precheck_title_maybe_kit_or_multiunit")
    return alerts


# ==============================================================================
# FUNÇÕES PARA PROCESSAR JSON/JSONL
# ==============================================================================

def load_and_aggregate_jsonl(jsonl_path: str):
    """
    Lê um arquivo JSON/JSONL com listagens brutas, agrupa-as por uma chave canônica
    e calcula as estatísticas de preço, simulando a tabela 'canonical_summary'.
    Retorna (df_summary, df_listings_full)
    """
    try:
        with open(jsonl_path, "r", encoding="utf-8") as f:
            content = f.read()
            try:
                records = json.loads(content)
                if isinstance(records, dict):
                    records = [records]
            except json.JSONDecodeError:
                f.seek(0)
                records = [json.loads(line) for line in f if line.strip()]
    except FileNotFoundError:
        print(f"[ERRO] Arquivo de entrada não encontrado: {jsonl_path}")
        return None, None

    df_listings = pd.DataFrame(records)

    rename_map = {
        "line_model": "model",
        "size_norm": "size",
        "titulo": "title",
        "preco": "price",
    }
    for k, v in rename_map.items():
        if k in df_listings.columns:
            df_listings.rename(columns={k: v}, inplace=True)

    for col in ["brand", "model", "size", "price"]:
        if col not in df_listings.columns:
            df_listings[col] = None

    if "marketplace" not in df_listings.columns:
        df_listings["marketplace"] = None
    if "title" not in df_listings.columns:
        df_listings["title"] = None
    if "seller" not in df_listings.columns:
        df_listings["seller"] = None
    if "url" not in df_listings.columns:
        df_listings["url"] = None

    df_listings["brand"] = df_listings["brand"].astype(str).str.strip()
    df_listings["model"] = df_listings["model"].astype(str).str.strip()
    df_listings["size"] = df_listings["size"].astype(str).str.strip()
    df_listings["price"] = pd.to_numeric(df_listings["price"], errors="coerce")

    df_listings.dropna(subset=["brand", "model", "size", "price"], inplace=True)

    df_listings["canonical_key"] = (
        df_listings["brand"].astype(str).str.strip()
        + "|"
        + df_listings["model"].astype(str).str.strip()
        + "|"
        + df_listings["size"].astype(str).str.strip()
    )

    def q10(x):
        return x.quantile(0.10)

    def q90(x):
        return x.quantile(0.90)

    df_summary = (
        df_listings.groupby(["canonical_key", "brand", "model", "size"]).agg(
            min_price=("price", "min"),
            max_price=("price", "max"),
            mean_price=("price", "mean"),
            median_price=("price", "median"),
            p10=("price", q10),
            p90=("price", q90),
            n_listings=("price", "size"),  
            marketplaces=("marketplace", lambda s: list(pd.Series(s).dropna().unique())),
        )
    ).reset_index()

    def trimmed_mean(g: pd.Series) -> float:
        if g.empty:
            return float("nan")
        lo = g.quantile(0.10)
        hi = g.quantile(0.90)
        cut = g[(g >= lo) & (g <= hi)]
        return float(cut.mean() if not cut.empty else g.mean())

    media_correta_series = (
        df_listings.groupby("canonical_key")["price"].apply(trimmed_mean).rename("media_correta")
    )
    df_summary = df_summary.merge(media_correta_series, on="canonical_key", how="left")

    return df_summary, df_listings


def fetch_listings_sample_from_df(df_full: pd.DataFrame, canonical_key: str, k_titles: int = 5, k_sellers: int = 3):
    """Extrai uma amostra de listagens de um DataFrame para uma dada chave canônica."""
    rows_df = df_full[df_full["canonical_key"] == canonical_key]
    if rows_df.empty:
        return {"titles": [], "sellers_top": [], "examples": [], "n_total": 0}

    prices = sorted(rows_df["price"].dropna().tolist())
    examples = []

    def pick_example_by_price(target_price: float):
        if not prices:
            return None
        idx = (rows_df["price"] - target_price).abs().argsort().iloc[0]
        r = rows_df.iloc[int(idx)]
        return {
            "title": r.get("title"),
            "price": r.get("price"),
            "seller": r.get("seller"),
            "marketplace": r.get("marketplace"),
            "url": r.get("url"),
        }

    if prices:
        examples.extend(
            [
                pick_example_by_price(prices[0]),
                pick_example_by_price(prices[len(prices) // 2]),
                pick_example_by_price(prices[-1]),
            ]
        )

    titles = rows_df["title"].dropna().astype(str).tolist() if "title" in rows_df else []
    sellers = rows_df["seller"].dropna().astype(str).tolist() if "seller" in rows_df else []
    top_titles = [t for t, _ in Counter(titles).most_common(k_titles)]
    top_sellers = [s for s, _ in Counter(sellers).most_common(k_sellers)]

    seen_urls = set()
    dedup_examples = []
    for ex in examples:
        if not ex:
            continue
        u = ex.get("url")
        if u and u in seen_urls:
            continue
        if u:
            seen_urls.add(u)
        dedup_examples.append(ex)

    return {
        "titles": top_titles,
        "sellers_top": top_sellers,
        "examples": dedup_examples[:3],
        "n_total": len(rows_df),
    }


# ==============================================================================
# FUNÇÕES DE BANCO DE DADOS E OLLAMA
# ==============================================================================

def connect_sqlite(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_canonical_rows(conn, only_brand=None, only_size=None, only_model=None):
    base = "SELECT * FROM canonical_summary"
    clauses, params = [], []
    if only_brand:
        clauses.append("brand = ?"); params.append(only_brand)
    if only_size:
        clauses.append("size = ?"); params.append(only_size)
    if only_model:
        clauses.append("model = ?"); params.append(only_model)
    if clauses:
        base += " WHERE " + " AND ".join(clauses)
    base += " ORDER BY brand, size, model"
    return conn.execute(base, params).fetchall()


def fetch_listings_sample(conn, canonical_key, k_titles=5, k_sellers=3):
    rows = conn.execute(
        "SELECT title, price, seller, marketplace, url FROM unified_listings WHERE canonical_key = ?",
        (canonical_key,),
    ).fetchall()
    if not rows:
        return {"titles": [], "sellers_top": [], "examples": [], "n_total": 0}

    prices = sorted([r["price"] for r in rows if r["price"] is not None])
    examples = []

    def pick_example_by_price(target):
        if not prices:
            return None
        best = min(rows, key=lambda r: abs((r["price"] or 0) - target))
        return {k: best[k] for k in best.keys()}

    if prices:
        examples.extend(
            [
                pick_example_by_price(prices[0]),
                pick_example_by_price(prices[len(prices) // 2]),
                pick_example_by_price(prices[-1]),
            ]
        )

    top_titles = [t for t, _ in Counter(r["title"] for r in rows if r["title"]).most_common(k_titles)]
    top_sellers = [s for s, _ in Counter(r["seller"] for r in rows if r["seller"]).most_common(k_sellers)]

    seen_urls = set()
    dedup_examples = []
    for ex in examples:
        if not ex:
            continue
        u = ex.get("url")
        if u and u in seen_urls:
            continue
        if u:
            seen_urls.add(u)
        dedup_examples.append(ex)

    return {"titles": top_titles, "sellers_top": top_sellers, "examples": dedup_examples[:3], "n_total": len(rows)}


def call_ollama_generate(host="http://localhost:11434", model="llama3", prompt="", **kwargs):
    url = f"{host.rstrip('/')}/api/generate"
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.0, "top_p": 0.9, "seed": 42},
    }
    if kwargs.get("max_tokens"):
        payload["options"]["num_predict"] = kwargs["max_tokens"]

    print(f"[OLLAMA][REQ] url={url} model={model} prompt_chars={len(prompt)} "
          f"num_predict={payload['options'].get('num_predict')} timeout={kwargs.get('timeout', 60)}")

    resp = None
    try:
        resp = requests.post(url, json=payload, timeout=kwargs.get("timeout", 60))
        status = resp.status_code
        if status != 200:
            body = resp.text[:800] if hasattr(resp, "text") else ""
            print(f"[OLLAMA][ERR] status={status} body_snippet={body}")
        resp.raise_for_status()
        data = resp.json()
        print(f"[OLLAMA][OK] prompt_eval_count={data.get('prompt_eval_count')} "
              f"eval_count={data.get('eval_count')} done_reason={data.get('done_reason')}")
        return data.get("response", "").strip(), data
    except requests.exceptions.HTTPError as e:
        body = resp.text[:800] if resp is not None and hasattr(resp, "text") else ""
        raise RuntimeError(f"HTTP {resp.status_code} from Ollama: {body}") from e
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Request to Ollama failed: {e}") from e


def build_prompt(brand, model, size, stats, sample):
    obj = {
        "brand": brand,
        "model": model,
        "size": size,
        "n_listings": stats.get("n_listings"),
        "prices": {
            "min": stats.get("min_price"),
            "p10": stats.get("p10"),
            "median": stats.get("median") or stats.get("median_price"),
            "p90": stats.get("p90"),
            "max": stats.get("max_price"),
            "media_correta": stats.get("media_correta"),
        },
        "titles_sample": (sample.get("titles", []) or [])[:5],
        "sellers_top": (sample.get("sellers_top", []) or [])[:3],
        "examples": [
            {
                "title": e.get("title"),
                "price": e.get("price"),
                "seller": e.get("seller"),
                "marketplace": e.get("marketplace"),
            }
            for e in (sample.get("examples", []) or [])
        ],
    }
    preface = (
        "Você é um auditor de catálogo de pneus. Avalie se a normalização (brand/model/size) e as estatísticas de preço estão consistentes para o grupo canônico informado. "
        "Procure: divergência de marca nos títulos, kits/múltiplas unidades, mistura de modelos parecidos que deveriam estar separados, e se os agregados (p10/p90/mediana/trimmed_mean) fazem sentido dado o número de ofertas. "
        "Se notar outliers remanescentes, sinalize."
    )
    SCHEMA_INSTRUCTIONS = (
        'Responda ESTRITAMENTE em JSON minificado, no formato:\n'
        '{"ok":true|false,"alerts":[<strings>],"confidence":0..1}\n\n'
        'Onde:\n- "ok" = true se os dados parecem consistentes (normalização/estatísticas) e não há indícios fortes de problema.\n'
        '- "alerts" = lista de códigos curtos. Use somente dos exemplos abaixo quando aplicável (pode incluir outros quando necessário):\n'
        '  - "brand_title_mismatch"\n  - "possible_kit_or_multiunit"\n  - "ambiguous_model_grouping"\n  - "outlier_prices_remaining"\n  - "suspicious_trimmed_mean"\n  - "inconsistent_titles_vs_size"\n  - "seller_cluster_risk"\n  - "low_sample_reliability"\n'
        '- "confidence" = sua confiança na avaliação (0..1).\n'
        'NÃO inclua texto fora do JSON. NÃO explique.'
    )
    return f"{preface}\n\nDADOS:\n{json.dumps(obj, ensure_ascii=False)}\n\n{SCHEMA_INSTRUCTIONS}"


def ensure_ai_audit_table(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_audit (
            cod_prod INT PRIMARY KEY,
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
        """
    )
    conn.commit()


def upsert_ai_audit(conn, rec):
    conn.execute(
        """
        INSERT INTO ai_audit (
            canonical_key, brand, model, size, n_listings,
            stats_json, titles_sample_json, sellers_top_json, examples_json, precheck_alerts_json,
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
        """,
        (
            rec["canonical_key"],
            rec["brand"],
            rec["model"],
            rec["size"],
            rec["n_listings"],
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
            rec["created_at"],
        ),
    )
    conn.commit()


def _clear_screen():
    os.system("cls" if os.name == "nt" else "clear")


# ==============================================================================
# FUNÇÃO MAIN
# ==============================================================================

def main():
    ap = argparse.ArgumentParser(description="Dupla confirmação com LLM (Ollama) para pneus")
    ap.add_argument("--db", required=True, help="Caminho do SQLite (.db/.sqlite) OU arquivo .json/.jsonl bruto")
    ap.add_argument("--out-dir", default="data/AI")
    ap.add_argument("--out-db", default=None)
    ap.add_argument("--out", default=None)
    ap.add_argument("--model", default="llama3.2:3b")
    ap.add_argument("--ollama-host", default="http://localhost:11434")
    ap.add_argument("--sample-titles", type=int, default=5)
    ap.add_argument("--sample-sellers", type=int, default=3)
    ap.add_argument("--max-tokens", type=int, default=256)
    ap.add_argument("--timeout", type=int, default=60)
    ap.add_argument("--only-brand")
    ap.add_argument("--only-size")
    ap.add_argument("--only-model")
    ap.add_argument("--no-sqlite-write", action="store_true")
    ap.add_argument("--append", action="store_true")
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument("--clear-screen", action="store_true")
    args = ap.parse_args()

    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    audit_db_path = Path(args.out_db or out_dir / "ai_audit.db").expanduser().resolve()
    audit_db_path.parent.mkdir(parents=True, exist_ok=True)

    ts = dt.datetime.now(pytz.timezone("America/Sao_Paulo")).strftime("%Y%m%d_%H%M%S")
    out_path = Path(args.out or out_dir / f"audit_{ts}.jsonl")
    if out_path.exists() and not args.append:
        out_path.unlink()

    input_path = Path(args.db)
    if not input_path.exists():
        if args.clear_screen:
            _clear_screen()
        print(f"[ERRO] Arquivo de entrada não encontrado: {input_path}")
        sys.exit(1)

    conn_audit = connect_sqlite(audit_db_path.as_posix())
    ensure_ai_audit_table(conn_audit)

    rows_to_process = []
    df_listings_full = None
    conn_uni = None

    suffix = input_path.suffix.lower()
    is_json = suffix in [".json", ".jsonl"]
    is_sqlite = suffix in [".db", ".sqlite"]

    if is_json:
        print(f"Lendo e agregando dados do arquivo JSON(L): {input_path}")
        df_summary, df_listings_full = load_and_aggregate_jsonl(input_path.as_posix())
        if df_summary is None:
            print("[ERRO] Falha ao carregar e agregar o arquivo JSON(L).")
            sys.exit(2)

        if args.only_brand:
            df_summary = df_summary[df_summary["brand"].str.lower() == args.only_brand.lower()]
        if args.only_size:
            df_summary = df_summary[df_summary["size"].str.lower() == args.only_size.lower()]
        if args.only_model:
            df_summary = df_summary[df_summary["model"].str.lower() == args.only_model.lower()]

        rows_to_process = df_summary.to_dict("records")
    elif is_sqlite:
        print(f"Lendo dados do banco de dados SQLite: {input_path}")
        conn_uni = connect_sqlite(input_path.as_posix())
        db_rows = fetch_canonical_rows(conn_uni, args.only_brand, args.only_size, args.only_model)
        rows_to_process = [dict(r) for r in db_rows]
    else:
        print(f"[ERRO] Formato de arquivo não suportado: {suffix}. Use .db, .sqlite, .json ou .jsonl")
        sys.exit(3)

    if not rows_to_process:
        if args.clear_screen:
            _clear_screen()
        print("Nenhum registro encontrado para auditar com os filtros aplicados.")
        return

    # --- LOOP DE PROCESSAMENTO PRINCIPAL ---
    processed = 0
    with out_path.open("a", encoding="utf-8") as fo:
        for r in tqdm(rows_to_process, desc="Auditing", unit="item", disable=args.quiet):
            canonical_key = r["canonical_key"]
            brand = r.get("brand")
            model = r.get("model")
            size = r.get("size")

            if is_json:
                stats = {
                    "n_listings": r.get("n_listings"),
                    "min_price": r.get("min_price"),
                    "max_price": r.get("max_price"),
                    "mean_price": r.get("mean_price"),
                    "median_price": r.get("median_price"),
                    "median": r.get("median_price"),
                    "p10": r.get("p10"),
                    "p90": r.get("p90"),
                    "media_correta": r.get("media_correta"),
                    "marketplaces": r.get("marketplaces") or [],
                }
                sample = fetch_listings_sample_from_df(
                    df_listings_full, canonical_key, k_titles=args.sample_titles, k_sellers=args.sample_sellers
                )
            else:
                stats = {
                    "n_listings": r.get("n_listings"),
                    "min_price": r.get("min_price"),
                    "max_price": r.get("max_price"),
                    "mean_price": r.get("mean_price"),
                    "median_price": r.get("median_price"),
                    "median": r.get("median_price"),
                    "p10": r.get("p10"),
                    "p90": r.get("p90"),
                    "media_correta": r.get("media_correta"),
                    "marketplaces": r.get("marketplaces"),
                }
                sample = fetch_listings_sample(
                    conn_uni, canonical_key, k_titles=args.sample_titles, k_sellers=args.sample_sellers
                )

            pre_alerts = precheck_price_sanity(stats) + precheck_title_flags(sample.get("titles", []))

            prompt = build_prompt(brand, model, size, stats, sample)
            print(f"[AUDIT] key={canonical_key} brand={brand} model={model} size={size} "
                f"titles={len(sample.get('titles', []))} sellers={len(sample.get('sellers_top', []))} "
                f"examples={len(sample.get('examples', []))} prompt_chars={len(prompt)} "
                f"max_tokens={args.max_tokens}")


            try:
                text, raw = call_ollama_generate(
                    host=args.ollama_host,
                    model=args.model,
                    prompt=prompt,
                    max_tokens=args.max_tokens,
                    timeout=args.timeout,
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
                llm_ok = False
                llm_alerts = ["llm_request_or_json_failed"]
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
                "created_at": dt.datetime.utcnow().isoformat() + "Z",
            }

            fo.write(json.dumps(record, ensure_ascii=False) + "\n")
            if not args.no_sqlite_write:
                upsert_ai_audit(conn_audit, record)

            processed += 1

    try:
        if is_sqlite and conn_uni:
            conn_uni.close()
    except Exception:
        pass

    try:
        conn_audit.close()
    except Exception:
        pass

    if args.clear_screen:
        _clear_screen()

    print(f"Processo concluído. {processed} grupos auditados.")
    print(f"Resultados salvos em: {out_path.resolve()}")
    if not args.no_sqlite_write:
        print(f"Banco de dados de auditoria atualizado em: {audit_db_path.resolve()}")


if __name__ == "__main__":
    main()
