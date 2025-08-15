from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import pandas as pd
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Dict, Any, List, Optional

import pandas as pd

@dataclass
class Settings:
    package_dir: Path = Path(__file__).resolve().parent
    raw_dir: Path = package_dir / "data" / "raw"
    sqlite_dir: Path = package_dir / "data" / "sqlite"
    processed_dir: Path = package_dir / "data" / "processed"
    db_url: str = os.getenv(
        "PRICING_DB_URL",
        str((processed_dir / "pricing.db").resolve())
    )
    chunksize: int = int(os.getenv("PRICING_CHUNKSIZE", "5000"))
    env: str = os.getenv("ENV", "dev")

SETTINGS = Settings()

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

def get_conn() -> sqlite3.Connection:
    ensure_dirs()
    return sqlite3.connect(SETTINGS.db_url)

def to_sql(df: pd.DataFrame, name: str, if_exists: str = "append", index: bool = False, dtype: Optional[dict]=None):
    with get_conn() as conn:
        df.to_sql(name, conn, if_exists=if_exists, index=index, dtype=dtype)

def read_sql(query: str) -> pd.DataFrame:
    with get_conn() as conn:
        return pd.read_sql_query(query, conn)

def exec_sql(sql: str):
    with get_conn() as conn:
        conn.execute(sql)
        conn.commit()

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


