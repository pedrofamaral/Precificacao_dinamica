# etl_ingest.py
"""
ETL ÚNICO (3 em 1) + LIMPEZA:
- Lê o arquivo CSV de entrada.
- SEM CACHE: Reprocessa todos os arquivos a cada execução.
- Consome 'marketplace' diretamente do CSV e o salva no Parquet.
- Gera um banco de dados e outros artefatos em um diretório de saída especificado.
- Gera um arquivo de saída principal para o próximo passo do pipeline.

Execução no pipeline:
    python -m ETL.etl_ingest --arquivo-entrada "caminho/para/dados_brutos.csv" --diretorio-saida "caminho/saida" --arquivo-saida "caminho/saida/unifier_input.parquet"
"""
from __future__ import annotations

import json
import argparse
import re
import sqlite3
import unicodedata
from pathlib import Path
from typing import Dict, Any, List, Optional
from urllib.parse import urlparse, unquote, parse_qs
import numpy as np
import pandas as pd

# O bloco de import relativo deve funcionar bem no pipeline se a estrutura estiver correta
try:
    from .common import (
        SETTINGS, logger, ensure_dirs, to_sql, read_sql
    )
except ImportError:
    import sys, os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from ETL.common import (
        SETTINGS, logger, ensure_dirs, to_sql, read_sql
    )

# ============================================================
# Helpers e Configurações (sem alterações)
# ============================================================
def to_float(x):
    if x is None or (isinstance(x, float) and pd.isna(x)): return None
    if isinstance(x, (int, float)): return float(x)
    s = str(x).strip().replace("R$", "").replace("$", "").strip().replace(".", "").replace(",", ".")
    try: return float(s)
    except ValueError: return None

def _save_parquet_with_fallback(df: pd.DataFrame, out_path: Path, csv_name: str):
    try:
        df.to_parquet(out_path, index=False)
        logger.info("💾 Parquet salvo em %s", out_path)
    except Exception as e:
        backup = out_path.with_name(csv_name)
        df.to_csv(backup, index=False, encoding="utf-8", sep=';', decimal=',')
        logger.warning("⚠️ Sem engine parquet (pyarrow/fastparquet). Salvei CSV em %s. Erro: %s", backup, e)

def unwrap_ml_click(url: str) -> str:
    if not isinstance(url, str): return url
    try:
        p = urlparse(url)
        if "mercadolivre.com" in p.netloc and "click" in p.netloc:
            q = parse_qs(p.query)
            for key in ("url", "u", "redirect"):
                if key in q and q[key]: return unquote(q[key][0])
        return url
    except Exception: return url

def infer_marketplace_from_url(url_unwrapped: str | None) -> str | None:
    if not url_unwrapped or not isinstance(url_unwrapped, str): return None
    try: host = urlparse(url_unwrapped).netloc.lower()
    except Exception: return None
    if "mercadolivre.com" in host: return "mercadolivre"
    if "magazineluiza.com.br" in host or "magalu.com" in host: return "magalu"
    if "pneustore.com.br" in host: return "pneustore"
    return host.split(":")[0] if host else None

# ============================================================
# Normalização e Ingestão
# ============================================================
def normalize_record(raw: Dict[str, Any], meta: Dict[str, Any]) -> Dict[str, Any]:
    def pick(*keys):
        for k in keys:
            if k in raw and raw[k] not in (None, "", [], {}): return raw[k]
        return None
    marketplace, cod_prod, title, price, url, sku = (
        pick("marketplace", "source", "site"), pick("cod_prod", "internal_code"),
        pick("title", "name", "nome"), pick("price", "preco", "valor"),
        pick("url", "link"), pick("listing_id", "sku", "id")
    )
    return {
        "cod_prod": int(cod_prod) if cod_prod is not None and str(cod_prod).isdigit() else None,
        "marketplace": marketplace, "query": meta.get("query"), "title": title,
        "sku_text": sku, "price": to_float(price), "url": url,
        "captured_at": pick("observed_at", "captured_at") or meta.get("captured_at"),
    }

# MODIFICADO: para aceitar um único arquivo de entrada, que é o esperado no pipeline
def ingest_file(input_path: Path) -> List[Dict[str, Any]]:
    rows = []
    if not input_path.exists():
        logger.warning(f"⚠️  Arquivo de entrada não encontrado: {input_path}")
        return rows
    
    if not input_path.is_file() or input_path.suffix.lower() not in {".csv", ".json", ".jsonl"}:
        logger.warning(f"⚠️  Caminho de entrada não é um arquivo suportado: {input_path}")
        return rows

    logger.info("Lendo arquivo: %s", input_path)
    meta = {"query": input_path.name}
    try:
        if input_path.suffix.lower() == ".csv":
            try: df = pd.read_csv(input_path, sep=';', decimal=',')
            except Exception: df = pd.read_csv(input_path)
            df.columns = df.columns.str.lower()
            for rec in df.to_dict(orient="records"):
                rows.append(normalize_record(rec, meta))
        # Adicione lógica para JSON/JSONL se necessário
        # elif input_path.suffix.lower() in {".json", ".jsonl"}: ...
    except Exception as e:
        logger.warning("⚠️  Falha ao ler %s: %s", input_path, e)
    return rows

# ============================================================
# Limpeza / Padronização / Geração de Tabelas
# ============================================================
# MODIFICADO: para receber os caminhos de saída como argumentos
def clean_and_snapshot(all_rows_df: pd.DataFrame, out_db_path: Path, output_dir: Path, main_output_file: Path):
    if all_rows_df.empty:
        logger.warning("⚠️  Nenhum dado encontrado nos arquivos de entrada. Nada a fazer.")
        return

    to_sql(all_rows_df, "market_items", if_exists="replace", index=False)
    full = read_sql("SELECT * FROM market_items")

    full["url_unwrapped"] = full["url"].str.lower().apply(lambda u: unwrap_ml_click(u) if isinstance(u, str) else u)
    inferred_mp = full['url_unwrapped'].apply(infer_marketplace_from_url)
    mask_to_fill = full['marketplace'].isna() | (full['marketplace'].str.lower() == 'unknown')
    full.loc[mask_to_fill, 'marketplace'] = inferred_mp[mask_to_fill]
    full['marketplace'].fillna('unknown', inplace=True)
    full["price"] = pd.to_numeric(full["price"], errors='coerce')
    full["captured_at"] = pd.to_datetime(full["captured_at"], errors="coerce", utc=True)

    mask_ess = (full["cod_prod"].notna() & full["price"].notna() & (full["price"] > 0) & 
                full["url_unwrapped"].notna() & (full["url_unwrapped"] != ''))
    clean = full.loc[mask_ess].copy()
    clean.sort_values(["cod_prod", "url", "captured_at"], ascending=False, inplace=True)
    clean.drop_duplicates(subset=["cod_prod", "url"], keep="first", inplace=True)
    
    to_sql(clean, "market_items_clean", if_exists="replace", index=False)
    
    snap = clean.loc[clean.groupby(["cod_prod", "marketplace"])["captured_at"].idxmax()]
    to_sql(snap, "unifier_input", if_exists="replace", index=False)
    
    canon = snap.groupby("cod_prod")["title"].agg(lambda s: s.value_counts().index[0]).rename("product_name").reset_index()
    to_sql(canon, "products_dim", if_exists="replace", index=False)

    # MODIFICADO: usa os diretórios passados como argumento
    output_dir.mkdir(parents=True, exist_ok=True)
    _save_parquet_with_fallback(clean, output_dir / "market_items_clean.parquet", "market_items_clean.csv")
    _save_parquet_with_fallback(snap, main_output_file, "unifier_input.csv") # Salva o arquivo principal no caminho exato
    
    export_sqlite_outputs(clean, snap, canon, out_db_path)
    log_discarded_rows(full, mask_ess)
    logger.info("✅ Limpeza concluída → market_items_clean: %d", len(clean))

def export_sqlite_outputs(clean: pd.DataFrame, snap: pd.DataFrame, canon: pd.DataFrame, out_db: Path):
    out_db.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(out_db) as conn:
        clean.to_sql("market_items_clean", conn, if_exists="replace", index=False)
        snap.to_sql("unifier_input", conn, if_exists="replace", index=False)
        canon.to_sql("products_dim", conn, if_exists="replace", index=False)
    logger.info("📦 Banco de dados de saída atualizado em %s", out_db)

def log_discarded_rows(df: pd.DataFrame, valid_mask: pd.Series):
    miss = df.loc[~valid_mask].copy()
    if miss.empty:
        logger.info("📊 Nenhuma linha descartada. Todos os registros eram válidos.")
        return
    miss["miss_reason"] = np.select(
        [miss["cod_prod"].isna(), miss["price"].isna() | (miss["price"] <= 0), miss["url_unwrapped"].isna() | (miss["url_unwrapped"] == '')],
        ["missing_cod_prod", "bad_price", "missing_url"], default="other"
    )
    logger.info("📊 Diagnóstico de descarte (Top motivos):\n%s", miss["miss_reason"].value_counts().head().to_string())

# ============================================================
# Orquestração Principal
# ============================================================
def main():
    """Função principal que orquestra todo o processo de ETL."""
    # MODIFICADO: argumentos de entrada e saída para uso no pipeline
    parser = argparse.ArgumentParser(description="ETL de Ingestão e Limpeza de Dados de Marketplace.")
    parser.add_argument("--arquivo-entrada", type=Path, required=True, help="Arquivo CSV de entrada com os dados brutos.")
    parser.add_argument("--diretorio-saida", type=Path, required=True, help="Pasta para salvar todos os artefatos (DB, Parquets, etc).")
    parser.add_argument("--arquivo-saida", type=Path, required=True, help="Caminho do arquivo de saída principal para o próximo passo (ex: unifier_input.parquet).")
    args = parser.parse_args()

    # MODIFICADO: caminhos são definidos pelos argumentos
    output_dir = args.diretorio_saida
    output_dir.mkdir(parents=True, exist_ok=True)

    # O DB interno e o warehouse.db serão salvos dentro do diretório de saída
    SETTINGS.db_path = output_dir / "internal_data.db"
    out_db_path = output_dir / "warehouse.db"
    
    logger.info("🚀 Iniciando ETL de ingestão...")
    
    all_rows = ingest_file(args.arquivo_entrada)
    
    clean_and_snapshot(
        all_rows_df=pd.DataFrame(all_rows), 
        out_db_path=out_db_path,
        output_dir=output_dir,
        main_output_file=args.arquivo_saida
    )

if __name__ == "__main__":
    main()
