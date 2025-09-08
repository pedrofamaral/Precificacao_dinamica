from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import pandas as pd
import unicodedata
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Dict, Any, List, Optional, Sequence, Tuple

_SQLITE_TYPE_MAP = {
    "int64": "INTEGER",
    "int32": "INTEGER",
    "int16": "INTEGER",
    "int8":  "INTEGER",
    "uint64": "INTEGER",
    "uint32": "INTEGER",
    "float64": "REAL",
    "float32": "REAL",
    "bool": "INTEGER",
    "datetime64[ns]": "TEXT",
    "object": "TEXT",
    "string": "TEXT",
}

@dataclass
class Settings:
    project_root: Path = Path(__file__).resolve().parent.parent 

    raw_dir: Path = project_root / "data" / "raw"
    sqlite_dir: Path = project_root / "data" / "sqlite"
    processed_dir: Path = project_root / "data" / "processed"
    
    db_url: str = os.getenv(
        "PRICING_DB_URL",
        str((processed_dir / "warehouse.db").resolve())
    )
    chunksize: int = int(os.getenv("PRICING_CHUNKSIZE", "5000"))
    env: str = os.getenv("ENV", "dev")

SETTINGS = Settings()



def _nfkd(s: str) -> str:
    return unicodedata.normalize("NFKD", s or "").encode("ascii","ignore").decode("ascii")


def _norm(s: str) -> str:
    s = _nfkd(str(s or ""))
    s = re.sub(r"[^A-Za-z0-9 /_-]+", " ", s).lower()
    return re.sub(r"\s+", " ", s).strip()


SPEED_RE = re.compile(r"\b\d{2,3}[A-Z]{1,2}\b", re.I)


def clean_speed_tokens(s: str) -> str:
    t = re.sub(r"\(\s*(?:\d{2,3}[A-Z]{1,2})\s*\)", " ", s or "", flags=re.I)
    t = SPEED_RE.sub(" ", t)
    return re.sub(r"\s{2,}", " ", t).strip()


def get_logger(name: str = "pricing_mvp"):
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(level)
        ch = logging.StreamHandler()
        ch.setLevel(level)
        fmt = logging.Formatter("[%(asctime)s] %(levelname)s - %(name)s: %(message)s")
        ch.setFormatter(fmt)
        logger.addHandler(ch)
    return logger


logger = get_logger()


_slug_re = re.compile(r"[^0-9A-Z]+")


def norm_sku(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    t = str(text).strip().upper()
    return _slug_re.sub("-", t).strip("-")


def ensure_dirs():
    SETTINGS.processed_dir.mkdir(parents=True, exist_ok=True)


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_conn():
    try:
        db_file = Path(SETTINGS.db_url) 
        db_file.parent.mkdir(parents=True, exist_ok=True)
        
        conn = sqlite3.connect(SETTINGS.db_url) 
        return conn
    except sqlite3.Error as e:
        print(f"❌ Erro ao conectar ao banco de dados: {e}")
        raise



def to_sql(df: pd.DataFrame, name: str, if_exists: str = "append", index: bool = False, dtype: Optional[dict]=None):
    with get_conn() as conn:
        df.to_sql(name, conn, if_exists=if_exists, index=index, dtype=dtype)


def read_sql(query: str) -> pd.DataFrame:
    with get_conn() as conn:
        return pd.read_sql_query(query, conn)


def _init_state_table():
    with get_conn() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS _ingestion_state (
            table_name TEXT NOT NULL,
            source_id TEXT NOT NULL,
            ts TEXT NOT NULL,
            PRIMARY KEY (table_name, source_id)
        )
        """)
        conn.commit()


def seen(table: str, source_id: str) -> bool:
    _init_state_table()
    with get_conn() as conn:
        row = conn.execute("SELECT 1 FROM _ingestion_state WHERE table_name=? AND source_id=?", (table, source_id)).fetchone()
        return row is not None


def mark_seen(table: str, source_id: str):
    _init_state_table()
    with get_conn() as conn:
        conn.execute("INSERT OR REPLACE INTO _ingestion_state (table_name, source_id, ts) VALUES (?, ?, ?)", (table, source_id, utcnow_iso()))
        conn.commit()


def iter_files(directory: Path, exts: tuple[str, ...]) -> Iterable[Path]:
    if not directory.exists():
        logger.warning("Diretório não existe: %s", directory)
        return []
    for p in sorted(directory.rglob("*")):
        if p.is_file() and p.suffix.lower() in exts:
            yield p


def load_json_lines(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        content = f.read().strip()
        if not content:
            return []
        if "\n" in content:
            items = []
            for line in content.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    items.append(json.loads(line))
                except json.JSONDecodeError:
                    try:
                        arr = json.loads(content)
                        if isinstance(arr, list):
                            return arr
                    except json.JSONDecodeError:
                        raise
            return items
        obj = json.loads(content)
        return obj if isinstance(obj, list) else [obj]


def file_fingerprint(p: Path) -> str:
    stat = p.stat()
    return f"{p}:{stat.st_size}:{int(stat.st_mtime)}"


def salvar_df_sqlite(
    df: pd.DataFrame,
    termo: str,
    output_dir: str = "dados",
    tabela: str = "produtos",
    usar_banco_central: bool = True,
    gerar_dump_sql: bool = False,
) -> Path:
    if df.empty:
        raise ValueError("DataFrame vazio.")
    if usar_banco_central:
        ensure_dirs()
        db_path = Path(SETTINGS.db_url)
        with get_conn() as conn:
            df.to_sql(tabela, conn, if_exists="append", index=False)
            if gerar_dump_sql:
                dump_path = db_path.with_name("export.sql")
                with open(dump_path, "w", encoding="utf-8") as f:
                    for line in conn.iterdump():
                        f.write(line + "\n")
        return db_path
    else:
        base = Path(output_dir)
        base.mkdir(parents=True, exist_ok=True)
        termo_slug = "".join(c if c.isalnum() or c in "-_." else "-" for c in termo.strip().lower())
        db_path = base / f"{termo_slug}.sqlite"
        with sqlite3.connect(db_path) as conn:
            df.to_sql(tabela, conn, if_exists="append", index=False)
            if gerar_dump_sql:
                dump_path = db_path.with_suffix(".sql")
                with open(dump_path, "w", encoding="utf-8") as f:
                    for line in conn.iterdump():
                        f.write(line + "\n")
        return db_path


def _infer_sqlite_type(pd_dtype: str) -> str:
    return _SQLITE_TYPE_MAP.get(str(pd_dtype), "TEXT")


def _nan_to_none(row: Sequence[Any]) -> Tuple[Any, ...]:
    out = []
    for v in row:
        if v is None:
            out.append(None)
        elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            out.append(None)
        else:
            out.append(v)
    return tuple(out)


def ensure_table_with_unique(conn: sqlite3.Connection, table: str, df: pd.DataFrame, key_cols: Sequence[str]):
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    exists = cur.fetchone() is not None
    if not exists:
        cols_sql = []
        for col in df.columns:
            t = _infer_sqlite_type(str(df.dtypes[col]))
            cols_sql.append(f'"{col}" {t}')
        if key_cols:
            unique_cols = ", ".join([f'"{k}"' for k in key_cols])
            unique_sql = f", UNIQUE({unique_cols})"
        else:
            unique_sql = ""
        ddl = f'CREATE TABLE IF NOT EXISTS "{table}" ({", ".join(cols_sql)}{unique_sql});'
        conn.execute(ddl)
        conn.commit()
    else:
        if key_cols:
            idx_name = "ux_" + table + "_" + "_".join(key_cols)
            idx_cols = ", ".join([f'"{k}"' for k in key_cols])
            conn.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS "{idx_name}" ON "{table}" ({idx_cols});')
            conn.commit()


def to_sql_merge(
    df: pd.DataFrame,
    table: str,
    key_cols: Sequence[str],
    update_cols: Sequence[str] | None = None,
):
    if df.empty:
        return
    if not key_cols:
        raise ValueError("key_cols não pode ser vazio para merge.")
    all_cols = list(df.columns)
    for k in key_cols:
        if k not in all_cols:
            raise ValueError(f"Coluna de chave '{k}' não está no DataFrame.")
    if update_cols is None:
        update_cols = [c for c in all_cols if c not in key_cols]
    do_update = bool(update_cols)
    placeholders = ", ".join(["?"] * len(all_cols))
    col_list_sql = ", ".join(f'"{c}"' for c in all_cols)
    conflict_target = ", ".join(f'"{k}"' for k in key_cols)
    if do_update:
        set_clause = ", ".join(f'"{c}"=excluded."{c}"' for c in update_cols)
        sql = f'''
            INSERT INTO "{table}" ({col_list_sql})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_target}) DO UPDATE SET
            {set_clause};
        '''
    else:
        sql = f'''
            INSERT INTO "{table}" ({col_list_sql})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_target}) DO NOTHING;
        '''
    with get_conn() as conn:
        ensure_table_with_unique(conn, table, df, key_cols)
        data = [_nan_to_none(tuple(df.loc[i, all_cols])) for i in range(len(df))]
        conn.executemany(sql, data)
        conn.commit()
