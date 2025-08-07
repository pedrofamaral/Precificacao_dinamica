import logging
import sqlite3
from pathlib import Path
from datetime import datetime

import pandas as pd

from src.pricing_ai.extract import extract_from_json_folder
from src.pricing_ai.transform import transform_basic
from src.pricing_ai.load import load_to_parquet

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s"
)
logger = logging.getLogger(__name__)

def process_marketplace(
    name: str,
    data_pattern: str,
    dedup_key: str
):
    logger.info(f"▶ Iniciando ETL para: {name}")

    try:
        df_raw = extract_from_json_folder(data_pattern)
    except Exception as e:
        logger.error(f"[Extract] não consegui ler {data_pattern!r}: {e}")
        return

    total = len(df_raw)
    logger.info(f"[Extract] {total} registros lidos de {data_pattern!r}")
    if total == 0:
        return

    import os
    df_raw['marketplace'] = name
    if name == 'pneustore':
        def extract_search_term_pneustore(rel_path):
            parts = rel_path.split(os.sep)
            return parts[-2] if len(parts) > 1 else parts[0]
        df_raw['search_term'] = df_raw['_source_file'].astype(str).apply(extract_search_term_pneustore)
    else:
        def extract_search_term(stem):
            stem = stem.split(os.sep)[-1]  
            parts = stem.split('_')
            if len(parts) > 2 and parts[-2].isdigit() and len(parts[-2]) == 8:
                return '_'.join(parts[:-2])
            if len(parts) > 1 and parts[-1].isdigit():
                return '_'.join(parts[:-1])
            return stem
        df_raw['search_term'] = df_raw['_source_file'].astype(str).apply(extract_search_term)
    df_raw.drop(columns=['_source_file'], inplace=True)

    if dedup_key in df_raw.columns:
        if 'preco' in df_raw.columns:
            df_raw.sort_values('preco', ascending=True, inplace=True)

        before = len(df_raw)
        df_raw = df_raw.drop_duplicates(
            subset=['marketplace', 'search_term', dedup_key],
            keep='first'
        )
        after = len(df_raw)
        logger.info(f"[Quality] removidas {before - after} duplicatas "
                    f"(por ['marketplace','search_term','{dedup_key}'])")
        # Keep only desired columns
        desired_cols = ['marketplace', 'search_term', dedup_key]
        # Plus any columns needed for transform_basic
        for col in ['preco', 'estoque', 'data_coleta']:
            if col in df_raw.columns and col not in desired_cols:
                desired_cols.append(col)
        df_raw = df_raw[[col for col in desired_cols if col in df_raw.columns]]
    else:
        logger.warning(f"Chave de dedup '{dedup_key}' não encontrada em "
                       f"{df_raw.columns.tolist()}")
        return

    nested = [
        c for c in df_raw.columns
        if df_raw[c].apply(lambda x: isinstance(x, dict)).any()
    ]
    if nested:
        logger.info(f"[Clean] removendo colunas aninhadas: {nested}")
        df_raw.drop(columns=nested, inplace=True)

    for col in ('preco', 'estoque'):
        if col in df_raw.columns:
            miss = df_raw[col].isnull().sum()
            logger.info(f"[Quality] {miss} faltando em '{col}'")

    try:
        df_trans = transform_basic(df_raw)
    except Exception as e:
        logger.error(f"[Transform] falha ao transformar dados de {name}: {e}")
        return
    logger.info(f"[Transform] {len(df_trans)} registros após transformação")

    if 'preco' in df_trans.columns:
        stats = df_trans['preco'].agg(['min','max','mean','median','std'])
        logger.info("[Stats] " + ", ".join(f"{k}={v:.2f}" for k,v in stats.items()))

    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_dir = Path('data/processed') / name
    out_dir.mkdir(parents=True, exist_ok=True)
    pf = out_dir / f'pricing_{ts}.parquet'
    try:
        load_to_parquet(df_trans, pf)
        logger.info(f"[Load] Parquet salvo em {pf}")
    except Exception as e:
        logger.error(f"[Load][Parquet] {e}")

    jf = out_dir / f'pricing_{ts}.json'
    try:
        df_trans.to_json(jf, orient='records', force_ascii=False)
        logger.info(f"[Load] JSON salvo em {jf}")
    except Exception as e:
        logger.error(f"[Load][JSON] {e}")

    db_path = 'data/processed/marketplaces.db'
    try:
        with sqlite3.connect(db_path) as conn:
            df_trans.to_sql(name, conn,
                            if_exists='replace', index=False)
        logger.info(f"[Load] SQLite tabela '{name}' em {db_path}")
    except Exception as e:
        logger.error(f"[Load][SQLite] {e}")

def main():
    marketplaces = [
        ('mercadolivre',
         'PriceMonitor/mercadolivre/data/JSON/**/*.json',
         'link'),
        ('amazon',
         'PriceMonitor/amazon/data/processed/amazon/**/*.json',
         'asin'),
        ('pneustore',
         'PriceMonitor/pneustore/dados/pneustore/**/*.json',
         'link'),
    ]
    for name, pattern, dedup in marketplaces:
        process_marketplace(name, pattern, dedup)

if __name__ == '__main__':
    main()
