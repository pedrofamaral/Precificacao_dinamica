import os
import glob
import sqlite3
import pandas as pd

def load_processed_data(processed_dir: str, 
                        source: str = "auto",
                        table_name: str = "produtos") -> pd.DataFrame:
    """
    Tenta carregar o DataFrame de:
      1) um arquivo SQLite em processed_dir/mercadolivre.db, se existir, ou
      2) todos os CSV/Parquet dentro de processed_dir/mercadolivre/.
    
    Parâmetros:
    - processed_dir: pasta base (ex: "data/processed")
    - source: "db", "folder" ou "auto" ("auto" tenta primeiro o DB, depois a pasta)
    - table_name: nome da tabela dentro do .db (caso use SQLite)
    """
    db_path    = os.path.join(processed_dir, "mercadolivre.db")
    folder_dir = os.path.join(processed_dir, "mercadolivre")

    # 1) Se decidiu usar o DB ou existir mercadolivre.db
    if source in ("db", "auto") and os.path.isfile(db_path):
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()
        return df

    # 2) Se decidiu usar a pasta ou não achou o DB
    if source in ("folder", "auto") and os.path.isdir(folder_dir):
        # suporta CSV e Parquet
        files = glob.glob(os.path.join(folder_dir, "**", "*.*"), recursive=True)
        dfs = []
        for p in files:
            ext = os.path.splitext(p)[1].lower()
            if ext == ".csv":
                dfs.append(pd.read_csv(p))
            elif ext in (".parquet", ".parq"):
                dfs.append(pd.read_parquet(p))
        if not dfs:
            raise FileNotFoundError(f"Nenhum CSV/Parquet em {folder_dir}")
        return pd.concat(dfs, ignore_index=True)

    raise FileNotFoundError(
        f"Não encontrei dados em {db_path} nem em {folder_dir}."
    )
