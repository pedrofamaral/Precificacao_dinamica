import argparse
import datetime as dt
import json
import math
import os
import re
import sqlite3
import unicodedata
import jsonschema
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Tuple

import pandas as pd
import pytz
import requests
from tqdm import tqdm

# ==============================================================================
# CONFIGURAÇÕES GERAIS
# ==============================================================================

KIT_REGEX = re.compile(
    r"\b("
    r"kit|jogo|par\b|dupla|conjunto|combo|lote|"
    r"cx\.?|caixa|c\s*\/\s*|c\s*x|"
    r"pack|pckt|pacote|"
    r"(?:2x|4x|5x|6x|8x|10x|x2|x4|x6|x8)|"
    r"par de|jogo de"
    r")\b",
    flags=re.IGNORECASE,
)

LLM_RESPONSE_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "ok": {"type": "boolean"},
        "alerts": {
            "type": "array",
            "items": {"type": "string"}
        },
        "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0}
    },
    "required": ["ok", "alerts", "confidence"],
    "additionalProperties": False
}

# ==============================================================================
# FUNÇÕES AUXILIARES
# ==============================================================================

def _to_float(x):
    try:
        if x is None: return None
        if isinstance(x, str) and not x.strip(): return None
        v = float(x)
        if math.isnan(v) or math.isinf(v): return None
        return v
    except Exception:
        return None

def _norm(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii","ignore").decode("ascii")
    return s.lower().strip()

def precheck_price_sanity(stats: dict) -> list[str]:
    alerts = []
    n = int(stats.get("n_listings") or 0)
    if n <= 0:
        return ["precheck_no_listings"]
    min_p, max_p = _to_float(stats.get("min_price")), _to_float(stats.get("max_price"))
    mean, median = _to_float(stats.get("mean_price")), _to_float(stats.get("median_price"))
    p10, p90 = _to_float(stats.get("p10")), _to_float(stats.get("p90"))
    if min_p is None or max_p is None:
        alerts.append("precheck_missing_min_max")
    else:
        if min_p <= 0: alerts.append("precheck_nonpositive_min")
        if max_p < min_p: alerts.append("precheck_max_less_than_min")
        elif min_p > 0 and (max_p / min_p) > 4.0: alerts.append("precheck_high_spread")
    if mean is not None and median is not None and median != 0:
        if abs(mean - median) / abs(median) > 0.3: alerts.append("precheck_mean_median_divergence")
    if p10 is not None and p90 is not None and p10 > 0:
        if (p90 / p10) > 3.5: alerts.append("precheck_wide_p10_p90")
    if n < 4: alerts.append("precheck_low_sample_reliability")
    return alerts

def precheck_title_flags(titles):
    alerts = []
    if any(KIT_REGEX.search(str(t)) for t in titles):
        alerts.append("precheck_title_maybe_kit_or_multiunit")
    return alerts

def numeric_alerts(stats):
    al = []
    p10, p90, med = stats.get("p10"), stats.get("p90"), stats.get("median_price")
    if all(map(lambda x: x is not None and pd.notna(x), [p10,p90,med])):
        if med < 0.7 * p10 or med > 1.3 * p90:
            al.append("price_outlier")
    return al

# ==============================================================================
# BANCO DE DADOS
# ==============================================================================

def connect_sqlite(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_verification_table(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_verification (
            cod_prod TEXT PRIMARY KEY,
            truth_data_json TEXT,
            collected_stats_json TEXT,
            precheck_alerts_json TEXT,
            llm_ok INTEGER,
            llm_alerts_json TEXT,
            llm_confidence REAL,
            llm_model TEXT,
            llm_raw_response TEXT,
            verified_at TEXT
        )
        """
    )
    conn.commit()

def upsert_ai_verification(conn, rec):
    conn.execute(
        """
        INSERT INTO ai_verification (
            cod_prod, truth_data_json, collected_stats_json, precheck_alerts_json,
            llm_ok, llm_alerts_json, llm_confidence, llm_model, llm_raw_response, verified_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(cod_prod) DO UPDATE SET
            truth_data_json=excluded.truth_data_json,
            collected_stats_json=excluded.collected_stats_json,
            precheck_alerts_json=excluded.precheck_alerts_json,
            llm_ok=excluded.llm_ok,
            llm_alerts_json=excluded.llm_alerts_json,
            llm_confidence=excluded.llm_confidence,
            llm_model=excluded.llm_model,
            llm_raw_response=excluded.llm_raw_response,
            verified_at=excluded.verified_at
        """,
        (
            rec["cod_prod"],
            json.dumps(rec["truth_data"], ensure_ascii=False),
            json.dumps(rec["collected_stats"], ensure_ascii=False),
            json.dumps(rec["precheck_alerts"], ensure_ascii=False),
            1 if rec["llm_ok"] else 0,
            json.dumps(rec["llm_alerts"], ensure_ascii=False),
            rec["llm_confidence"],
            rec["llm_model"],
            rec["llm_raw_response"],
            rec["verified_at"],
        ),
    )
    conn.commit()

# ==============================================================================
# OLLAMA
# ==============================================================================

def call_ollama_generate(host="http://localhost:11434", model="llama3", prompt="", **kwargs) -> Tuple[str, dict]:
    """
    Chamada determinística ao Ollama, com JSON mode ativado e tentativas com backoff.
    Retorna (texto_resposta, objeto_json_da_resposta_do_ollama).
    """
    url = f"{host.rstrip('/')}/api/generate"
    options = {"temperature": 0.0, "top_p": 0.1, "seed": 42}
    if kwargs.get("max_tokens"):
        options["num_predict"] = kwargs["max_tokens"]

    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": options
    }

    attempts = int(kwargs.get("retries", 3))
    timeout = kwargs.get("timeout", 60)

    last_exc = None
    for i in range(attempts):
        try:
            resp = requests.post(url, json=payload, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            return data.get("response", "").strip(), data
        except requests.exceptions.RequestException as e:
            last_exc = e
            if i < attempts - 1:
                time_to_sleep = 0.8 * (i + 1)
                try:
                    import time
                    time.sleep(time_to_sleep)
                except Exception:
                    pass

    msg = f"Request to Ollama failed after {attempts} tries: {last_exc}"
    print(f"\n[OLLAMA][ERRO] {msg}")
    print("[OLLAMA][DICA] Verifique se o Ollama está rodando e se o modelo foi baixado com 'ollama pull {model}'")
    raise RuntimeError(msg)

def _sanitize_llm_obj(obj):
    ok = bool(obj.get("ok", False))
    alerts = obj.get("alerts") or []
    if not isinstance(alerts, list): alerts = [str(alerts)]
    conf = obj.get("confidence", 0.0)
    try:
        conf = max(0.0, min(1.0, float(conf)))
    except Exception:
        conf = 0.0
    return ok, alerts, conf

def _validate_llm_schema(obj) -> Tuple[bool, str]:
    """
    Valida o JSON do LLM contra o schema. Retorna (is_valid, error_message).
    """
    try:
        jsonschema.validate(instance=obj, schema=LLM_RESPONSE_SCHEMA)
        return True, ""
    except jsonschema.ValidationError as ve:
        return False, f"llm_schema_invalid: {ve.message}"
    except jsonschema.SchemaError as se:
        return False, f"llm_schema_error: {se.message}"

# ==============================================================================
# PROMPT
# ==============================================================================

def build_verification_prompt(truth_data: dict, stats: dict, sample: dict):
    reference_obj = {
        "cod_prod": truth_data.get("cod_prod"),
        "brand_correta": truth_data.get("brand"),
        "model_correto": truth_data.get("model"),
        "size_correto": truth_data.get("size"),
    }
    collected_obj = {
        "n_listings_encontrados": stats.get("n_listings"),
        "estatisticas_preco": {
            "min": stats.get("min_price"),
            "median": stats.get("median_price"),
            "max": stats.get("max_price"),
            "media_aparada": stats.get("media_correta"),
        },
        "amostra_titulos": (sample.get("titles", []) or [])[:5],
        "principais_vendedores": (sample.get("sellers_top", []) or [])[:3],
    }
    preface = (
        "Você é um auditor de catálogo de pneus. Compare os DADOS DE REFERÊNCIA (a fonte da verdade) com os DADOS COLETADOS de anúncios online para o mesmo `cod_prod`. "
        "Sua tarefa é verificar se os anúncios coletados são realmente do produto de referência. "
        "Procure por: divergência de marca/modelo/medida nos títulos, indícios de kits/combos, produtos usados/recondicionados, ou preços outliers. "
        "Se os dados coletados forem consistentes com a referência, responda com 'ok: true'."
    )
    SCHEMA_INSTRUCTIONS = (
        'Responda ESTRITAMENTE em JSON minificado, no formato:\n'
        '{"ok":true|false,"alerts":[<strings>],"confidence":0..1}\n\n'
        'Onde:\n- "ok" = true se os dados COLETADOS são consistentes com os de REFERÊNCIA.\n'
        '- "alerts" = lista de códigos. Exemplos: "brand_mismatch", "model_mismatch", "size_mismatch", "possible_kit", "used_item", "price_outlier", "low_sample".\n'
        '- "confidence" = sua confiança na avaliação (0 a 1).\n'
        'NÃO inclua texto ou explicações fora do JSON.'
    )
    prompt_content = (
        f"DADOS DE REFERÊNCIA (VERDADE):\n{json.dumps(reference_obj, ensure_ascii=False, indent=2)}\n\n"
        f"DADOS COLETADOS (PARA AUDITAR):\n{json.dumps(collected_obj, ensure_ascii=False, indent=2)}"
    )
    return f"{preface}\n\n{prompt_content}\n\n{SCHEMA_INSTRUCTIONS}"

# ==============================================================================
# MAIN
# ==============================================================================

def main():
    ap = argparse.ArgumentParser(description="Verificação de dados de pneus com LLM (Ollama)")
    ap.add_argument("--truth-json", required=True, help="Caminho do JSON com os dados da empresa (fonte da verdade)")
    ap.add_argument("--parquet-data", required=True, help="Caminho do Parquet com os dados coletados para verificação")
    ap.add_argument("--out-dir", default="data/AI_Verification")
    ap.add_argument("--out-db", default=None, help="Caminho para o banco de dados SQLite de resultados. Padrão: dentro de out-dir.")
    ap.add_argument("--out", default=None, help="Caminho para o arquivo de resultados JSONL. Padrão: dentro de out-dir.")
    ap.add_argument("--model", default="llama3")
    ap.add_argument("--ollama-host", default="http://localhost:11434")
    ap.add_argument("--sample-titles", type=int, default=5)
    ap.add_argument("--sample-sellers", type=int, default=3)
    ap.add_argument("--max-tokens", type=int, default=384)
    ap.add_argument("--timeout", type=int, default=90)
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--no-sqlite-write", action="store_true", help="Desativa a escrita no banco de dados SQLite.")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    db_path = Path(args.out_db or out_dir / "ai_verification.db").expanduser().resolve()

    ts = dt.datetime.now(pytz.timezone("America/Sao_Paulo")).strftime("%Y%m%d_%H%M%S")
    out_path = Path(args.out or out_dir / f"verification_{ts}.jsonl")
    if out_path.exists():
        print(f"[AVISO] Arquivo de saída {out_path} já existe e será sobrescrito.")
        out_path.unlink()

    path_truth = Path(args.truth_json)
    path_parquet = Path(args.parquet_data)

    if not path_truth.exists() or not path_parquet.exists():
        print(f"[ERRO] Verifique os caminhos: \n  - JSON: {path_truth}\n  - Parquet: {path_parquet}")
        sys.exit(1)

    try:
        print(f"Carregando fonte da verdade de: {path_truth}")
        df_truth = pd.read_json(path_truth, encoding='utf-8-sig')

        print(f"Carregando dados coletados de: {path_parquet}")
        df_listings = pd.read_parquet(path_parquet)
        required_cols = {"cod_prod","price","title","seller"}
        missing = required_cols - set(df_listings.columns)
        if missing:
            print(f"[ERRO] Parquet sem colunas obrigatórias: {sorted(missing)}")
            sys.exit(2)

        df_listings["price"] = pd.to_numeric(df_listings["price"], errors="coerce")
    except Exception as e:
        print(f"[ERRO] Falha ao carregar os arquivos de dados: {e}")
        sys.exit(1)

    rename_map_truth = {'line_model': 'model', 'size_norm': 'size'}
    df_truth.rename(columns=rename_map_truth, inplace=True)

    for col in ['cod_prod', 'brand', 'model', 'size']:
        if col not in df_truth.columns:
            print(f"[ERRO] A coluna obrigatória '{col}' (após renomear) não foi encontrada no JSON: {path_truth}")
            sys.exit(2)

    conn_audit = None
    if not args.no_sqlite_write:
        print(f"Resultados serão salvos no banco de dados: {db_path}")
        conn_audit = connect_sqlite(db_path)
        ensure_verification_table(conn_audit)

    processed = 0
    with out_path.open("a", encoding="utf-8") as fo:
        for _, truth_row in tqdm(df_truth.iterrows(), total=len(df_truth), desc="Verificando Produtos", disable=args.quiet):
            cod_prod_to_audit = str(truth_row["cod_prod"])  
            product_listings_df = df_listings[df_listings['cod_prod'].astype(str) == cod_prod_to_audit].copy()

            if product_listings_df.empty:
                continue

            prices = product_listings_df['price'].dropna()
            if prices.empty:
                continue

            def q10(x): return x.quantile(0.10)
            def q90(x): return x.quantile(0.90)
            def trimmed_mean(g):
                if g.empty: return float("nan")
                lo, hi = g.quantile(0.10), g.quantile(0.90)
                cut = g[(g >= lo) & (g <= hi)]
                return float(cut.mean() if not cut.empty else g.mean())

            stats = {
                "n_listings": int(len(prices)),
                "min_price": float(prices.min()),
                "max_price": float(prices.max()),
                "mean_price": float(prices.mean()),
                "median_price": float(prices.median()),
                "p10": float(q10(prices)),
                "p90": float(q90(prices)),
                "media_correta": float(trimmed_mean(prices)),
            }

            titles = [_norm(t) for t in product_listings_df["title"].dropna().astype(str)]
            sellers = product_listings_df["seller"].dropna().astype(str).tolist()
            sample = {
                "titles": [t for t, _ in Counter(titles).most_common(args.sample_titles)],
                "sellers_top": [s for s, _ in Counter(sellers).most_common(args.sample_sellers)],
            }

            pre_alerts = precheck_price_sanity(stats) + precheck_title_flags(sample.get("titles", [])) + numeric_alerts(stats)

            prompt = build_verification_prompt(truth_row.to_dict(), stats, sample)

            if not args.quiet:
                print(f"\n[AUDIT] Verificando cod_prod: {cod_prod_to_audit} ({truth_row['brand']} {truth_row['model']})")
                print(f"  > Enviando prompt de {len(prompt)} caracteres para o modelo {args.model}...")

            try:
                text, _ = call_ollama_generate(
                    host=args.ollama_host, model=args.model, prompt=prompt,
                    max_tokens=args.max_tokens, timeout=args.timeout, retries=args.retries
                )
                
                try:
                    llm_obj = json.loads(text)
                    is_valid, schema_err = _validate_llm_schema(llm_obj)
                    if is_valid:
                        llm_ok, llm_alerts, llm_conf = _sanitize_llm_obj(llm_obj)
                    else:
                        llm_ok, llm_alerts, llm_conf = False, ["llm_invalid_json", schema_err], 0.0
                except (json.JSONDecodeError, AttributeError, TypeError):
                    llm_ok, llm_alerts, llm_conf = False, ["llm_invalid_json"], 0.0
            except Exception as e:
                text, llm_ok, llm_alerts, llm_conf = f"LLM_ERROR: {e}", False, ["llm_request_failed"], 0.0

            record = {
                "cod_prod": cod_prod_to_audit,
                "truth_data": truth_row.to_dict(),
                "collected_stats": stats,
                "precheck_alerts": pre_alerts,
                "llm_ok": llm_ok,
                "llm_alerts": llm_alerts,
                "llm_confidence": llm_conf,
                "llm_model": args.model,
                "llm_raw_response": text,
                "verified_at": dt.datetime.utcnow().isoformat() + "Z",
            }

            fo.write(json.dumps(record, ensure_ascii=False) + "\n")
            if conn_audit and not args.no_sqlite_write:
                upsert_ai_verification(conn_audit, record)

            processed += 1

    if conn_audit:
        conn_audit.close()

    print(f"\nProcesso concluído. {processed} produtos verificados.")
    print(f"Resultados em JSONL salvos em: {out_path.resolve()}")
    if not args.no_sqlite_write:
        print(f"Banco de dados de verificação salvo em: {db_path.resolve()}")


if __name__ == "__main__":
    main()
