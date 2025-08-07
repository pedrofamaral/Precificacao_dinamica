import pandas as pd
from pathlib import Path
import logging

def load_to_parquet(df: pd.DataFrame, target_path: str, compress: bool = False):
    Path(target_path).parent.mkdir(parents=True, exist_ok=True)
    try:
        if compress:
            df.to_parquet(target_path, index=False, compression='gzip')
        else:
            df.to_parquet(target_path, index=False)
        logging.info(f'Dados salvos em {target_path}')
    except Exception as e:
        logging.error(f'Falha ao salvar {target_path}: {e}')

def load_to_csv(df: pd.DataFrame, target_path: str):
    Path(target_path).parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_csv(target_path, index=False)
        logging.info(f'Dados salvos em {target_path}')
    except Exception as e:
        logging.error(f'Falha ao salvar {target_path}: {e}')
