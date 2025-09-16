from __future__ import annotations

if __package__ is None or __package__ == "":
    import os as _os, sys as _sys
    _sys.path.append(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))

import json
import argparse
import re
import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Any, List, Optional, Union, Generator
from urllib.parse import urlparse, unquote, parse_qs
from datetime import datetime
import logging
import warnings
from contextlib import contextmanager

warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
warnings.filterwarnings('ignore', category=FutureWarning)

from ETL.tire_size import (
    extract_tire_size_from_title,
    extract_brand,
    extract_model,
    enrich_with_parsed_fields,
)

try:
    from ETL.common import (
        SETTINGS,
        logger,
        ensure_dirs,
        iter_files,
        load_json_lines,
        norm_sku,
        to_sql,
        read_sql,
        seen,
        mark_seen,
        file_fingerprint,
        SPEED_RE,
        to_sql_merge,
        get_conn,
    )
except ImportError:
    from .common import (
        SETTINGS,
        logger,
        ensure_dirs,
        iter_files,
        load_json_lines,
        norm_sku,
        to_sql,
        read_sql,
        seen,
        mark_seen,
        file_fingerprint,
        SPEED_RE,
        to_sql_merge,
        get_conn,
    )

# Constants
GENERIC_TOKENS = {"p", "produto", "products", "product", "click", "clicks", "count", "item"}

BATCH_SIZE = 10000
MAX_MEMORY_ROWS = 50000


def setup_logging():
    """Configure logging with proper encoding and format"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('etl_prod.log', encoding='utf-8')
        ]
    )
    return logging.getLogger(__name__)


# Use our own logger setup
prod_logger = setup_logging()


@contextmanager
def get_db_connection(db_path: Union[str, Path]):
    """Context manager for database connections with proper error handling"""
    conn = None
    try:
        conn = sqlite3.connect(str(db_path), timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA cache_size=10000")
        yield conn
    except sqlite3.Error as e:
        prod_logger.error(f"Database error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def to_float(x) -> Optional[float]:
    """Enhanced float conversion with better error handling"""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if isinstance(x, (int, float)):
        return float(x)
    
    if not isinstance(x, str):
        try:
            x = str(x)
        except (ValueError, TypeError):
            return None
    
    # Clean string
    s = x.strip()
    if not s:
        return None
        
    # Remove currency symbols and clean
    s = re.sub(r'[R$€$£¥]', '', s)
    s = s.replace(' ', '').replace('\xa0', '')  # Remove spaces and non-breaking spaces
    
    # Handle Brazilian decimal format (1.234,56 -> 1234.56)
    if ',' in s and '.' in s:
        # If both comma and dot, assume Brazilian format
        if s.rfind(',') > s.rfind('.'):
            s = s.replace('.', '').replace(',', '.')
    elif ',' in s and s.count(',') == 1:
        # Single comma, could be decimal separator
        parts = s.split(',')
        if len(parts) == 2 and len(parts[1]) <= 2:
            s = s.replace(',', '.')
    
    try:
        value = float(s)
        # Sanity check for prices
        if 0 < value < 1000000:  # Reasonable price range
            return value
        return None
    except (ValueError, TypeError):
        return None


def safe_json_loads(text: str) -> Optional[Dict]:
    """Safely load JSON with better error handling"""
    if not text or not text.strip():
        return None
    try:
        return json.loads(text.strip())
    except json.JSONDecodeError as e:
        prod_logger.warning(f"JSON decode error: {e}")
        return None


def _save_parquet_with_fallback(df: pd.DataFrame, out_path: Path, csv_name: str):
    """Save parquet with CSV fallback and proper error handling"""
    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out_path, index=False, engine='auto')
        prod_logger.info(f"💾 Parquet saved to {out_path}")
    except Exception as e:
        backup = out_path.with_name(csv_name)
        try:
            df.to_csv(backup, index=False, encoding="utf-8")
            prod_logger.warning(f"⚠️ Parquet failed, saved CSV to {backup}. Error: {e}")
        except Exception as csv_error:
            prod_logger.error(f"Failed to save both parquet and CSV: parquet={e}, csv={csv_error}")
            raise


def unwrap_ml_click(url: Optional[str]) -> Optional[str]:
    """Unwrap MercadoLivre click tracking URLs"""
    if not url or not isinstance(url, str):
        return url
    try:
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        if "mercadolivre.com" in host and "click" in host:
            query_params = parse_qs(parsed.query)
            for key in ("url", "u", "redirect"):
                if key in query_params and query_params[key]:
                    return unquote(query_params[key][0])
        return url
    except Exception as e:
        prod_logger.debug(f"Error unwrapping URL {url}: {e}")
        return url


def name_from_url(url_unwrapped: Optional[str]) -> Optional[str]:
    """Extract product name from URL path"""
    if not url_unwrapped or not isinstance(url_unwrapped, str):
        return None
    try:
        parsed = urlparse(url_unwrapped)
        path = (parsed.path or "").strip("/").lower()
        
        # Clean path
        s = re.sub(r"[-_/]+", " ", path)
        s = re.sub(r"\.(html|htm|php|aspx)$", "", s, flags=re.IGNORECASE)
        s = re.sub(r"\s+", " ", s).strip()
        
        if not s or len(s) < 4:
            return None
        return s.title()
    except Exception as e:
        prod_logger.debug(f"Error extracting name from URL {url_unwrapped}: {e}")
        return None


def name_from_query(query: Optional[str]) -> Optional[str]:
    """Extract product name from query/filename"""
    if not query or not isinstance(query, str):
        return None
    
    # Extract base name from filename pattern
    match = re.search(r"(.+?)_(?:\d{8})_(?:\d{6})\.(?:jsonl|json|csv|txt)$", query, flags=re.I)
    base = match.group(1) if match else query
    
    # Clean base name
    s = re.sub(r"[-_]+", " ", base)
    s = re.sub(r"\s+", " ", s).strip().lower()
    
    if not s or s in GENERIC_TOKENS or len(s) < 4:
        return None
    return s.title()


def infer_marketplace_from_url(url_unwrapped: Optional[str]) -> Optional[str]:
    """Infer marketplace from URL with enhanced detection"""
    if not url_unwrapped or not isinstance(url_unwrapped, str):
        return None
    try:
        host = urlparse(url_unwrapped).netloc.lower()
    except Exception:
        return None
    
    # Marketplace detection mapping
    marketplace_patterns = {
        "mercadolivre": ["mercadolivre.com", "mercadolibre.com", "mlb.com"],
        "magalu": ["magazineluiza.com.br", "magalu.com"],
        "pneustore": ["pneustore.com.br"],
        "amazon": ["amazon.com.br", "amazon.com"],
        "americanas": ["americanas.com.br"],
        "submarino": ["submarino.com.br"],
        "casasbahia": ["casasbahia.com.br"],
        "extra": ["extra.com.br"],
    }
    
    for marketplace, patterns in marketplace_patterns.items():
        if any(pattern in host for pattern in patterns):
            return marketplace
    
    # Return cleaned hostname if no match
    return host.split(":")[0] if host else None


def parse_captured_from_query(query: Optional[str]) -> pd.Timestamp:
    """Parse capture timestamp from query with enhanced error handling"""
    if not query or not isinstance(query, str):
        return pd.NaT
    
    # Try multiple patterns
    patterns = [
        r"_(\d{8})_(\d{6})",  # Original pattern
        r"(\d{8})_(\d{6})",   # Without leading underscore
        r"(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})",  # Alternative format
    ]
    
    for pattern in patterns:
        match = re.search(pattern, query)
        if match:
            try:
                if len(match.groups()) == 2:
                    date_str, time_str = match.groups()
                    return pd.to_datetime(f"{date_str} {time_str}", format="%Y%m%d %H%M%S", utc=True)
                elif len(match.groups()) == 6:
                    y, m, d, h, min_val, s = match.groups()
                    return pd.to_datetime(f"{y}{m}{d} {h}{min_val}{s}", format="%Y%m%d %H%M%S", utc=True)
            except Exception as e:
                prod_logger.debug(f"Date parsing error for {query}: {e}")
                continue
    
    return pd.NaT


def normalize_record(raw: Dict[str, Any], meta: Dict[str, Any]) -> Dict[str, Any]:
    """Enhanced record normalization with better field mapping"""
    
    def pick_field(*keys):
        """Pick first non-empty value from multiple possible keys"""
        for key in keys:
            if key in raw:
                value = raw[key]
                if value not in (None, "", [], {}, "null", "NULL"):
                    return value
        return None
    
    cod_prod = pick_field("cod_prod", "internal_code", "product_id_internal", "codigo_produto", "id_produto")
    title = pick_field("title", "product_title", "productTitle", "name", "nome", "titulo", "Title", "product_name")
    price = pick_field("price", "preco", "product_price", "valor", "salePrice", "sellingPrice", "bestPrice", "final_price", "Price")
    promo_price = pick_field("promo_price", "promotional_price", "sale_price", "discount_price", "preco_promocional")
    seller = pick_field("seller", "sellerName", "seller_name", "loja", "store", "merchant", "vendor", "seller_id")
    url = pick_field("url", "link", "product_url", "productUrl", "urlProduto", "url_produto", "href", "Url")
    sku = pick_field("sku", "listing_id", "product_id", "productId", "code", "identifier", "id", "Id", "sku_text")
    stock = pick_field("stock", "estoque", "quantity", "availableQuantity", "available_quantity", "availability")
    location = pick_field("location", "cidade", "city", "estado", "state")
    currency = pick_field("currency", "moeda", "currency_id", "Currency", "dinheiro") or "BRL"
    condition = pick_field("condition", "condicao", "Condition", "estado_produto")
    shipping = pick_field("shipping_price", "frete", "shipping", "frete_price", "shipping_cost")
    brand = pick_field("brand", "marca", "Brand", "fabricante")
    model = pick_field("model", "modelo", "Model")
    
    width = pick_field("width", "largura", "Width")
    aspect = pick_field("aspect", "perfil", "Aspect", "aspect_ratio")
    rim = pick_field("rim", "aro", "Rim", "rim_size")
    size_norm = pick_field("size_norm", "size", "medida", "tamanho", "size_text")
    
    observed_at = pick_field("observed_at", "scraped_at", "captured_at", "data_coleta", "data_captura")
    captured_at = pick_field("captured_at", "scraped_at") or meta.get("captured_at")
    
    if captured_at and not isinstance(captured_at, str):
        try:
            captured_at = str(captured_at)
        except Exception:
            captured_at = None
    
    if cod_prod is not None:
        try:
            cod_prod = int(float(str(cod_prod)))
        except (ValueError, TypeError):
            cod_prod = None
    
    width_val = to_float(width) if width else None
    aspect_val = to_float(aspect) if aspect else None
    rim_val = to_float(rim) if rim else None
    
    return {
        "cod_prod": cod_prod,
        "source": meta.get("source", "unknown"),
        "marketplace": meta.get("marketplace", meta.get("source", "unknown")),
        "query": meta.get("query"),
        "listing_id": sku,
        "title": title,
        "title_raw": title,
        "sku_text": sku or title,
        "sku_norm": norm_sku(str(sku or title or "")),
        "price": to_float(price),
        "promo_price": to_float(promo_price),
        "currency": currency,
        "condition": condition,
        "seller": seller,
        "seller_id": pick_field("seller_id"),
        "url": url,
        "shipping_price": to_float(shipping),
        "stock": stock if isinstance(stock, (int, float)) else None,
        "location": location,
        "brand": brand,
        "model": model,
        "width": width_val,
        "aspect": aspect_val,
        "rim": rim_val,
        "size_norm": size_norm,
        "captured_at": captured_at,
        "observed_at": observed_at,
        "run_id": meta.get("run_id"),
        "availability": pick_field("availability", "disponibilidade"),
        "size_regex_hit": pick_field("size_regex_hit"),
    }


def meta_from_path(path: Path) -> Dict[str, str]:
    """Extract metadata from file path with enhanced detection"""
    parts_lower = [s.lower() for s in path.parts]
    
    marketplace = "unknown"
    marketplace_indicators = {
        "mercadolivre": ["mercadolivre", "mercadolibre", "ml"],
        "magalu": ["magazineluiza", "magalu", "magazine"],
        "pneustore": ["pneustore", "pneu_store"],
        "amazon": ["amazon"],
        "americanas": ["americanas"],
    }
    
    for mp_name, indicators in marketplace_indicators.items():
        if any(indicator in part for part in parts_lower for indicator in indicators):
            marketplace = mp_name
            break
    
    query = path.name
    timestamp = parse_captured_from_query(query)
    captured_at = timestamp.isoformat() if pd.notna(timestamp) else None
    
    run_id_match = re.search(r'(\d{8}_\d{6}_[^\.]+)', query)
    run_id = run_id_match.group(1) if run_id_match else None
    
    return {
        "source": marketplace,
        "marketplace": marketplace,
        "query": query,
        "captured_at": captured_at,
        "run_id": run_id
    }


def process_csv_file(path: Path) -> List[Dict[str, Any]]:
    """Process CSV file with enhanced error handling and encoding detection"""
    rows = []
    meta = meta_from_path(path)
    
    configs = [
        {"sep": ";", "decimal": ",", "encoding": "utf-8"},
        {"sep": ",", "decimal": ".", "encoding": "utf-8"},
        {"sep": ";", "decimal": ",", "encoding": "latin-1"},
        {"sep": ",", "decimal": ".", "encoding": "latin-1"},
        {"sep": ";", "decimal": ",", "encoding": "cp1252"},
    ]
    
    df = None
    for config in configs:
        try:
            df = pd.read_csv(path, **config, low_memory=False)
            if not df.empty and len(df.columns) > 1:
                prod_logger.debug(f"Successfully read CSV with config: {config}")
                break
        except Exception as e:
            prod_logger.debug(f"Failed to read CSV with config {config}: {e}")
            continue
    
    if df is None or df.empty:
        prod_logger.warning(f"Could not read CSV file: {path}")
        return rows
    
    for i in range(0, len(df), BATCH_SIZE):
        batch = df.iloc[i:i+BATCH_SIZE]
        for record in batch.to_dict(orient="records"):
            try:
                normalized = normalize_record(record, meta)
                rows.append(normalized)
            except Exception as e:
                prod_logger.warning(f"Error normalizing record from {path}: {e}")
                continue
    
    return rows


def iter_input_files():
    extensions = {".jsonl", ".json", ".csv"}
    for marketplace, directories in MARKET_PATHS.items():
        for directory in directories:
            if not directory.exists():
                prod_logger.debug(f"Directory does not exist: {directory}")
                continue
            
            try:
                for path in directory.rglob("*"):
                    if path.suffix.lower() in extensions and path.is_file():
                        yield marketplace, path
            except Exception as e:
                prod_logger.error(f"Error accessing directory {directory}: {e}")
                continue


def ingest_scraper_dirs() -> Generator[List[Dict[str, Any]], None, None]:
    rows = []
    processed_files = 0
    
    for marketplace, path in iter_input_files():
        try:
            file_id = f"{path.suffix.lower().strip('.')}:" + file_fingerprint(path)
            
            if seen("market_items", file_id):
                prod_logger.debug(f"Skipping already processed file: {path}")
                continue
            
            prod_logger.info(f"Processing {marketplace} file: {path}")
            
            if path.suffix.lower() == ".jsonl":
                batch_rows = process_jsonl_file(path)
            elif path.suffix.lower() == ".json":
                batch_rows = process_json_file(path)
            elif path.suffix.lower() == ".csv":
                batch_rows = process_csv_file(path)
            else:
                continue
            
            rows.extend(batch_rows)
            mark_seen("market_items", file_id)
            processed_files += 1
            
            prod_logger.info(f"Processed {len(batch_rows)} records from {path}")
            
            if len(rows) > MAX_MEMORY_ROWS:
                prod_logger.info(f"Memory threshold reached, yielding {len(rows)} rows")
                yield rows
                rows = []
        
        except Exception as e:
            prod_logger.error(f"Error processing file {path}: {e}")
            continue
    
    if rows:  
        yield rows
    
    prod_logger.info(f"Total files processed: {processed_files}")


def process_jsonl_file(path: Path) -> List[Dict[str, Any]]:
    rows = []
    meta = meta_from_path(path)
    
    try:
        with path.open("r", encoding="utf-8") as file:
            for line_num, line in enumerate(file, 1):
                line = line.strip()
                if not line:
                    continue
                
                try:
                    raw_data = safe_json_loads(line)
                    if raw_data:
                        normalized = normalize_record(raw_data, meta)
                        rows.append(normalized)
                except Exception as e:
                    prod_logger.warning(f"Error processing line {line_num} in {path}: {e}")
                    continue
    except Exception as e:
        prod_logger.error(f"Error reading JSONL file {path}: {e}")
    
    return rows


def process_json_file(path: Path) -> List[Dict[str, Any]]:
    rows = []
    meta = meta_from_path(path)
    
    try:
        with path.open("r", encoding="utf-8") as file:
            data = json.load(file)
        
        if isinstance(data, list):
            for item in data:
                try:
                    normalized = normalize_record(item, meta)
                    rows.append(normalized)
                except Exception as e:
                    prod_logger.warning(f"Error normalizing record in {path}: {e}")
                    continue
        elif isinstance(data, dict):
            try:
                normalized = normalize_record(data, meta)
                rows.append(normalized)
            except Exception as e:
                prod_logger.warning(f"Error normalizing single record in {path}: {e}")
    
    except Exception as e:
        prod_logger.error(f"Error reading JSON file {path}: {e}")
    
    return rows


def ingest_json() -> List[Dict[str, Any]]:
    rows = []
    
    for path in iter_files(SETTINGS.raw_dir, (".json", ".jsonl")):
        file_id = f"{path.suffix.lower().strip('.')}:" + file_fingerprint(path)
        
        if seen("market_items", file_id):
            prod_logger.debug(f"Skipping already processed file: {path}")
            continue
        
        try:
            if path.suffix.lower() == ".jsonl":
                batch_rows = process_jsonl_file(path)
            else:
                batch_rows = process_json_file(path)
            
            rows.extend(batch_rows)
            mark_seen("market_items", file_id)
            prod_logger.info(f"Processed {len(batch_rows)} records from {path}")
            
        except Exception as e:
            prod_logger.error(f"Error processing file {path}: {e}")
            continue
    
    return rows


def ingest_csv() -> List[Dict[str, Any]]:
    rows = []
    
    for path in iter_files(SETTINGS.raw_dir, (".csv",)):
        file_id = "csv:" + file_fingerprint(path)
        
        if seen("market_items", file_id):
            prod_logger.debug(f"Skipping already processed CSV: {path}")
            continue
        
        try:
            batch_rows = process_csv_file(path)
            rows.extend(batch_rows)
            mark_seen("market_items", file_id)
            prod_logger.info(f"Processed {len(batch_rows)} records from {path}")
        except Exception as e:
            prod_logger.error(f"Error processing CSV {path}: {e}")
            continue
    
    return rows


def ingest_sqlite() -> List[Dict[str, Any]]:
    rows = []
    sqlite_dir = SETTINGS.sqlite_dir
    
    if not sqlite_dir.exists():
        return rows
    
    for db_path in sqlite_dir.glob("*.db"):
        file_id = f"sqlite:{db_path}:{db_path.stat().st_size}:{int(db_path.stat().st_mtime)}"
        
        if seen("market_items", file_id):
            prod_logger.debug(f"Skipping already processed SQLite: {db_path}")
            continue
        
        try:
            with get_db_connection(db_path) as conn:
                cursor = conn.cursor()
                tables = [row[0] for row in cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()]
                
                preferred_tables = [t for t in tables if t.lower() in 
                                  ("items", "products", "market_prices", "scraped", "market_items")]
                
                target_table = None
                if preferred_tables:
                    target_table = preferred_tables[0]
                else:
                    for table in tables:
                        try:
                            columns = [row[1] for row in cursor.execute(
                                f"PRAGMA table_info('{table}')"
                            ).fetchall()]
                            
                            has_price = any(col.lower() in ("price", "preco", "valor") for col in columns)
                            has_title = any(col.lower() in ("title", "name", "nome", "titulo") for col in columns)
                            
                            if has_price and has_title:
                                target_table = table
                                break
                        except Exception as e:
                            prod_logger.debug(f"Error checking table {table}: {e}")
                            continue
                
                if not target_table:
                    prod_logger.warning(f"No suitable table found in {db_path}")
                    continue
                
                df = pd.read_sql_query(f'SELECT * FROM "{target_table}"', conn)
                meta = {
                    "source": db_path.stem,
                    "marketplace": db_path.stem,
                    "query": db_path.name,
                    "captured_at": None
                }
                
                for record in df.to_dict(orient="records"):
                    try:
                        normalized = normalize_record(record, meta)
                        rows.append(normalized)
                    except Exception as e:
                        prod_logger.warning(f"Error normalizing SQLite record: {e}")
                        continue
                
                mark_seen("market_items", file_id)
                prod_logger.info(f"Processed {len(df)} records from SQLite {db_path}")
        
        except Exception as e:
            prod_logger.error(f"Error reading SQLite {db_path}: {e}")
            continue
    
    return rows

def export_sqlite_outputs(clean: pd.DataFrame, snap: pd.DataFrame, canon: pd.DataFrame, out_db: Path) -> None:
    out_db.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with get_db_connection(out_db) as conn:
            if not clean.empty:
                clean.to_sql("market_items_clean", conn, if_exists="replace", index=False)
            
            if not snap.empty:
                snap.to_sql("unifier_input", conn, if_exists="replace", index=False)
            
            if not canon.empty:
                canon.to_sql("products_dim", conn, if_exists="replace", index=False)

            
            index_queries = [
                "CREATE INDEX IF NOT EXISTS ix_clean_marketplace_sku ON market_items_clean(marketplace, sku_norm)",
                "CREATE INDEX IF NOT EXISTS ix_clean_captured_at ON market_items_clean(captured_at)",
                "CREATE INDEX IF NOT EXISTS ix_clean_cod_prod ON market_items_clean(cod_prod)",
                "CREATE INDEX IF NOT EXISTS ix_clean_price ON market_items_clean(price)",
                "CREATE INDEX IF NOT EXISTS ix_clean_brand_model ON market_items_clean(brand, model)",
                "CREATE INDEX IF NOT EXISTS ix_clean_size_triplet ON market_items_clean(width, aspect, rim)",
                "CREATE INDEX IF NOT EXISTS ix_unifier_marketplace_sku ON unifier_input(marketplace, sku_norm)",
                "CREATE INDEX IF NOT EXISTS ix_unifier_cod_prod ON unifier_input(cod_prod)",
            ]
            
            for query in index_queries:
                try:
                    conn.execute(query)
                except Exception as e:
                    prod_logger.warning(f"Error creating index: {e}")
            
            conn.commit()
            prod_logger.info(f"📦 Successfully exported to SQLite: {out_db}")
    
    except Exception as e:
        prod_logger.error(f"Error exporting to SQLite {out_db}: {e}")
        raise


def _table_exists(table_name: str) -> bool:
    try:
        with get_conn() as conn:
            cursor = conn.cursor()
            result = cursor.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", 
                (table_name,)
            ).fetchone()
            return result is not None
    except Exception as e:
        prod_logger.error(f"Error checking table existence: {e}")
        return False


def validate_dataframe(df: pd.DataFrame, name: str) -> pd.DataFrame:
    if df.empty:
        prod_logger.warning(f"DataFrame {name} is empty")
        return df
    
    original_count = len(df)
    
    df = df.dropna(how='all')
    
    numeric_columns = ['price', 'promo_price', 'width', 'aspect', 'rim', 'cod_prod']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    string_columns = ['title', 'brand', 'model', 'seller', 'marketplace']
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).replace(['nan', 'None', 'null', ''], None)
    
    cleaned_count = len(df)
    if cleaned_count != original_count:
        prod_logger.info(f"Cleaned {name}: {original_count} -> {cleaned_count} rows")
    
    return df


def clean_and_snapshot(all_rows_df: pd.DataFrame, out_db: Optional[Path] = None):
    if not all_rows_df.empty:
        all_rows_df = validate_dataframe(all_rows_df, "input_data")
        
        required_columns = {
            'marketplace': None,
            'sku_norm': '',
            'captured_at': pd.NaT
        }
        
        for col, default_val in required_columns.items():
            if col not in all_rows_df.columns:
                all_rows_df[col] = default_val
        
        if 'sku_norm' not in all_rows_df.columns or all_rows_df['sku_norm'].isna().all():
            sku_source = all_rows_df.get('sku_text', all_rows_df.get('title', ''))
            all_rows_df['sku_norm'] = sku_source.fillna('').apply(
                lambda x: norm_sku(str(x)) if x else ''
            )
        
        if 'captured_at' not in all_rows_df.columns or all_rows_df['captured_at'].isna().all():
            all_rows_df['captured_at'] = all_rows_df.get('query', '').apply(parse_captured_from_query)
        
        all_rows_df['captured_at'] = pd.to_datetime(all_rows_df['captured_at'], errors='coerce', utc=True)
        
        try:
            key_columns = ["marketplace", "sku_norm", "captured_at"]
            to_sql_merge(all_rows_df, "market_items", key_cols=key_columns)
            prod_logger.info(f"Merged {len(all_rows_df)} new records into market_items")
        except Exception as e:
            prod_logger.warning(f"Merge failed, using append: {e}")
            to_sql(all_rows_df, "market_items", if_exists="append", index=False)
    
    if not _table_exists("market_items"):
        prod_logger.info("Table 'market_items' does not exist. Nothing to rebuild yet.")
        return
    
    try:
        full_data = read_sql("SELECT * FROM market_items")
    except Exception as e:
        prod_logger.error(f"Error reading market_items table: {e}")
        return
    
    if full_data.empty:
        prod_logger.info("market_items table is empty after ingestion.")
        return
    
    prod_logger.info(f"Processing {len(full_data)} total records from market_items")
    
    try:
        full_data['url_unwrapped'] = full_data['url'].apply(unwrap_ml_click)
        
        inferred_marketplace = full_data['url_unwrapped'].apply(infer_marketplace_from_url)
        full_data['marketplace'] = np.where(
            full_data['marketplace'].isna() | (full_data['marketplace'] == full_data.get('source', '')),
            inferred_marketplace,
            full_data['marketplace']
        )
        
        title_from_url = full_data['url_unwrapped'].apply(name_from_url)
        title_from_query = full_data['query'].apply(name_from_query)
        full_data['title'] = full_data['title'].fillna(title_from_url).fillna(title_from_query)
        
        full_data['price'] = pd.to_numeric(full_data['price'], errors='coerce')
        full_data['promo_price'] = pd.to_numeric(full_data['promo_price'], errors='coerce')
        
        parsed_timestamps = full_data['query'].apply(parse_captured_from_query)
        full_data['captured_at'] = pd.to_datetime(full_data.get('captured_at'), errors='coerce', utc=True)
        full_data.loc[full_data['captured_at'].isna(), 'captured_at'] = parsed_timestamps
        full_data['captured_date'] = full_data['captured_at'].dt.date.astype('string')
        
        prod_logger.info("Data enrichment completed successfully")
        
    except Exception as e:
        prod_logger.error(f"Error during data enrichment: {e}")
        raise
    
    essential_mask = (
        (~full_data['price'].isna()) &
        (full_data['price'] > 0) &
        (~full_data['url_unwrapped'].isna()) &
        (~full_data['title'].isna()) &
        (~full_data['marketplace'].isna())
    )
    
    clean_data = full_data.loc[essential_mask].copy()
    
    if clean_data.empty:
        prod_logger.warning("No records passed essential filters")
        return
    
    prod_logger.info(f"After essential filters: {len(clean_data)} records remain")
    
    clean_data['url'] = clean_data['url_unwrapped']
    clean_data.drop(columns=['url_unwrapped'], inplace=True, errors='ignore')
    
    clean_data.sort_values('captured_at', ascending=False, inplace=True)
    
    try:
        has_cod_prod = clean_data['cod_prod'].notna()
        
        df_with_cod = clean_data[has_cod_prod].drop_duplicates(
            subset=['marketplace', 'cod_prod'], keep='first'
        )
        
        df_without_cod = clean_data[~has_cod_prod].drop_duplicates(
            subset=['marketplace', 'url'], keep='first'
        )
        
        clean_data = pd.concat([df_with_cod, df_without_cod], ignore_index=True)
        clean_data = clean_data.drop_duplicates(
            subset=['marketplace', 'title', 'price'], keep='last'
        )
        
        prod_logger.info(f"After deduplication: {len(clean_data)} records remain")
        
    except Exception as e:
        prod_logger.error(f"Error during deduplication: {e}")
        raise
    
    try:
        has_cod_prod = clean_data['cod_prod'].notna()
        if has_cod_prod.any():
            canonical_names_cod = (
                clean_data.loc[has_cod_prod]
                .groupby('cod_prod', dropna=False)['title']
                .agg(lambda x: x.value_counts().index[0] if not x.empty else None)
                .rename('product_name_cod')
                .reset_index()
            )
            clean_data = clean_data.merge(canonical_names_cod, on='cod_prod', how='left')
        else:
            clean_data['product_name_cod'] = None
        
        no_cod_mask = ~has_cod_prod
        if no_cod_mask.any():
            canonical_names_sku = (
                clean_data.loc[no_cod_mask]
                .groupby(['marketplace', 'sku_norm'], dropna=False)['title']
                .agg(lambda x: x.value_counts().index[0] if not x.empty else None)
                .rename('product_name_sku')
                .reset_index()
            )
            clean_data = clean_data.merge(canonical_names_sku, on=['marketplace', 'sku_norm'], how='left')
        else:
            clean_data['product_name_sku'] = None
        
        clean_data['product_name'] = clean_data['product_name_cod'].fillna(clean_data['product_name_sku'])
        clean_data.drop(columns=['product_name_cod', 'product_name_sku'], inplace=True, errors='ignore')
        
        prod_logger.info("Canonical product names generated successfully")
        
    except Exception as e:
        prod_logger.error(f"Error generating canonical names: {e}")
        clean_data['product_name'] = clean_data['title']
    
    if 'product_name' not in clean_data.columns or clean_data['product_name'].isna().all():
        prod_logger.warning("Coluna 'product_name' não foi criada, usando 'title' como fallback.")
        clean_data['product_name'] = clean_data['title']
    
    try:
        clean_data = enrich_with_parsed_fields(clean_data)
        prod_logger.info("Tire size parsing completed successfully")
    except Exception as e:
        prod_logger.error(f"Error during tire size parsing: {e}")
    
    clean_data = validate_dataframe(clean_data, "clean_data")
    
    try:
        to_sql(clean_data, "market_items_clean", if_exists="replace", index=False)
        prod_logger.info(f"Saved {len(clean_data)} clean records to market_items_clean")
    except Exception as e:
        prod_logger.error(f"Error saving clean data: {e}")
        raise
    
    try:
        if clean_data['cod_prod'].notna().any():
            key_columns = ['cod_prod']
        else:
            key_columns = ['marketplace', 'sku_norm']
        
        snapshot_data = clean_data[
            clean_data.groupby(key_columns)['captured_at'].transform('max') == clean_data['captured_at']
        ].drop_duplicates(subset=key_columns)
        
        to_sql(snapshot_data, "unifier_input", if_exists="replace", index=False)
        prod_logger.info(f"Created snapshot with {len(snapshot_data)} records")
        
    except Exception as e:
        prod_logger.error(f"Error creating snapshot: {e}")
        snapshot_data = clean_data.copy()  
    
    try:
        if 'cod_prod' in key_columns:
            canonical_products = (
                clean_data.groupby('cod_prod', dropna=False)['product_name']
                .agg(lambda x: x.value_counts().index[0] if not x.empty else None)
                .reset_index()
            )
        else:
            canonical_products = (
                clean_data.groupby(['marketplace', 'sku_norm'], dropna=False)['product_name']
                .agg(lambda x: x.value_counts().index[0] if not x.empty else None)
                .reset_index()
            )
        
        canonical_products = canonical_products.rename(columns={'product_name': 'product_name'})
        to_sql(canonical_products, "products_dim", if_exists="replace", index=False)
        prod_logger.info(f"Created products dimension with {len(canonical_products)} records")
        
    except Exception as e:
        prod_logger.error(f"Error creating canonical products: {e}")
        canonical_products = pd.DataFrame()  
    
    try:
        ensure_dirs()
        _save_parquet_with_fallback(
            clean_data, 
            SETTINGS.processed_dir / "market_items_clean.parquet", 
            "market_items_clean.csv"
        )
        _save_parquet_with_fallback(
            snapshot_data, 
            SETTINGS.processed_dir / "unifier_input.parquet", 
            "unifier_input.csv"
        )
        
    except Exception as e:
        prod_logger.error(f"Error saving parquet files: {e}")
    
    if out_db is not None:
        try:
            export_sqlite_outputs(clean_data, snapshot_data, canonical_products, out_db)
        except Exception as e:
            prod_logger.error(f"Error exporting to SQLite {out_db}: {e}")
    
    try:
        generate_diagnostic_report(full_data, clean_data, essential_mask)
    except Exception as e:
        prod_logger.debug(f"Error generating diagnostic report: {e}")
    
    prod_logger.info(f"✅ Processing completed successfully")
    prod_logger.info(f"   → market_items_clean: {len(clean_data)} records")
    prod_logger.info(f"   → unifier_input: {len(snapshot_data)} records")
    prod_logger.info(f"   → products_dim: {len(canonical_products)} records")


def generate_diagnostic_report(full_data: pd.DataFrame, clean_data: pd.DataFrame, mask: pd.Series):
    try:
        excluded_data = full_data.loc[~mask].copy()
        
        if excluded_data.empty:
            prod_logger.info("📊 All records passed essential filters")
            return
        
        excluded_data['exclusion_reason'] = np.select([
            excluded_data['url'].isna(),
            excluded_data['title'].isna(),
            excluded_data['price'].isna() | (excluded_data['price'] <= 0),
            excluded_data['marketplace'].isna(),
        ], [
            'missing_url',
            'missing_title', 
            'bad_price',
            'missing_marketplace'
        ], default='other')
        
        exclusion_summary = (
            excluded_data.assign(marketplace=excluded_data['marketplace'].fillna('unknown'))
            .groupby('marketplace')['exclusion_reason']
            .value_counts()
            .sort_values(ascending=False)
            .head(15)
        )
        
        marketplace_distribution = clean_data['marketplace'].value_counts()
        
        prod_logger.info("📊 Data Quality Report:")
        prod_logger.info(f"   → Total records processed: {len(full_data):,}")
        prod_logger.info(f"   → Records passed filters: {len(clean_data):,}")
        prod_logger.info(f"   → Records excluded: {len(excluded_data):,}")
        
        prod_logger.info("📊 Top exclusion reasons:")
        for (marketplace, reason), count in exclusion_summary.items():
            prod_logger.info(f"   → {marketplace} - {reason}: {count:,}")
        
        prod_logger.info("📊 Clean data by marketplace:")
        for marketplace, count in marketplace_distribution.items():
            prod_logger.info(f"   → {marketplace}: {count:,}")
        
        if not clean_data['price'].empty:
            price_stats = clean_data['price'].describe()
            prod_logger.info("📊 Price statistics:")
            prod_logger.info(f"   → Mean: R$ {price_stats['mean']:.2f}")
            prod_logger.info(f"   → Median: R$ {price_stats['50%']:.2f}")
            prod_logger.info(f"   → Min: R$ {price_stats['min']:.2f}")
            prod_logger.info(f"   → Max: R$ {price_stats['max']:.2f}")
        
    except Exception as e:
        prod_logger.debug(f"Error in diagnostic report: {e}")


def rebuild_from_existing():
    prod_logger.info("Attempting to rebuild from existing data...")
    
    parquet_path = SETTINGS.processed_dir / "market_items_clean.parquet"
    if parquet_path.exists():
        try:
            clean_data = pd.read_parquet(parquet_path)
            prod_logger.info(f"Loaded {len(clean_data)} records from existing parquet")
            
            if clean_data['cod_prod'].notna().any():
                key_columns = ['cod_prod']
            else:
                key_columns = ['marketplace', 'sku_norm']
            
            snapshot_data = clean_data[
                clean_data.groupby(key_columns)['captured_at'].transform('max') == clean_data['captured_at']
            ].drop_duplicates(subset=key_columns)
            
            if 'cod_prod' in key_columns:
                canonical_products = (
                    clean_data.groupby('cod_prod', dropna=False)['product_name']
                    .agg(lambda x: x.value_counts().index[0] if not x.empty else None)
                    .reset_index()
                )
            else:
                canonical_products = (
                    clean_data.groupby(['marketplace', 'sku_norm'], dropna=False)['product_name']
                    .agg(lambda x: x.value_counts().index[0] if not x.empty else None)
                    .reset_index()
                )
            
            return clean_data, snapshot_data, canonical_products
            
        except Exception as e:
            prod_logger.error(f"Error loading from parquet: {e}")
    
    if _table_exists("market_items_clean"):
        try:
            clean_data = read_sql("SELECT * FROM market_items_clean")
            if not clean_data.empty:
                prod_logger.info(f"Loaded {len(clean_data)} records from database")
                return clean_data, pd.DataFrame(), pd.DataFrame()
        except Exception as e:
            prod_logger.error(f"Error loading from database: {e}")
    
    return None, None, None


def main():
    global BATCH_SIZE
    global MARKET_PATHS
    parser = argparse.ArgumentParser(description="ETL Pipeline for Marketplace Data")
    parser.add_argument("--raw_dir", help="Directory containing JSON/CSV files")
    parser.add_argument("--sqlite_dir", help="Directory containing SQLite databases")
    parser.add_argument("--out_db", help="Output SQLite database path")
    parser.add_argument("--force_rebuild", action="store_true", 
                       help="Force rebuild even if no new data")
    parser.add_argument("--batch_size", type=int, default=BATCH_SIZE,
                       help=f"Batch size for processing (default: {BATCH_SIZE})")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Enable verbose logging")
    parser.add_argument("--scraper_root", type=Path, help="Diretório raiz onde as pastas dos scrapers (mercadolivre, magalu, etc.) estão localizadas.")

    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        prod_logger.setLevel(logging.DEBUG)

    if args.scraper_root:
        if not args.scraper_root.exists():
            prod_logger.error(f"O diretório raiz dos scrapers especificado não existe: {args.scraper_root}")
            return 1
            
        SCRAPER_ROOT = args.scraper_root
        MARKET_PATHS = {
            "mercadolivre": [SCRAPER_ROOT / "mercadolivre" / "data" / "raw"],
            "magalu": [SCRAPER_ROOT / "MagazineLuiza" / "data" / "raw"],
            "pneustore": [SCRAPER_ROOT / "pneustore" / "dados" / "raw"],
        }
        prod_logger.info(f"Usando diretório raiz de scrapers personalizado: {SCRAPER_ROOT}")

    
    try:
        ensure_dirs()
        
        if args.raw_dir:
            SETTINGS.raw_dir = Path(args.raw_dir)
        if args.sqlite_dir:
            SETTINGS.sqlite_dir = Path(args.sqlite_dir)
        
        BATCH_SIZE = args.batch_size

        default_out_db = SETTINGS.processed_dir / "warehouse.db"
        out_db = Path(args.out_db) if args.out_db else default_out_db
        
        prod_logger.info("🚀 Starting ETL Pipeline")
        prod_logger.info(f"   → Raw directory: {SETTINGS.raw_dir}")
        prod_logger.info(f"   → SQLite directory: {SETTINGS.sqlite_dir}")
        prod_logger.info(f"   → Output database: {out_db}")
        prod_logger.info(f"   → Batch size: {BATCH_SIZE}")
        
        all_new_rows = []
        
        prod_logger.info("📂 Ingesting data from all sources...")
        
        try:
            scraper_rows = ingest_scraper_dirs()
            for batch in scraper_rows:  
                all_new_rows.extend(batch)
            prod_logger.info(f"   → Scraper directories: {len(all_new_rows)} records")
        except Exception as e:
            prod_logger.error(f"Error ingesting scraper directories: {e}")
        
        try:
            json_rows = ingest_json()
            all_new_rows.extend(json_rows)
            prod_logger.info(f"   → JSON/JSONL files: {len(json_rows)} records")
        except Exception as e:
            prod_logger.error(f"Error ingesting JSON files: {e}")
        
        try:
            csv_rows = ingest_csv()
            all_new_rows.extend(csv_rows)
            prod_logger.info(f"   → CSV files: {len(csv_rows)} records")
        except Exception as e:
            prod_logger.error(f"Error ingesting CSV files: {e}")
        
        try:
            sqlite_rows = ingest_sqlite()
            all_new_rows.extend(sqlite_rows)
            prod_logger.info(f"   → SQLite databases: {len(sqlite_rows)} records")
        except Exception as e:
            prod_logger.error(f"Error ingesting SQLite files: {e}")
        
        if not all_new_rows and not args.force_rebuild:
            prod_logger.info("📝 No new data found (idempotent run)")
            
            if _table_exists("market_items"):
                prod_logger.info("Rebuilding from existing database...")
                clean_and_snapshot(pd.DataFrame(), out_db)
                return
            
            clean_data, snapshot_data, canonical_products = rebuild_from_existing()
            if clean_data is not None:
                export_sqlite_outputs(clean_data, snapshot_data, canonical_products, out_db)
                prod_logger.info("✅ Rebuilt SQLite from existing parquet files")
                return
            
            prod_logger.warning("⚠️ No new data and no existing data found. "
                              "Run with --raw_dir pointing to data directory")
            return
        
        if all_new_rows:
            new_data_df = pd.DataFrame(all_new_rows)
            prod_logger.info(f"📊 Processing {len(new_data_df)} total new records")
            
            try:
                _save_parquet_with_fallback(
                    new_data_df, 
                    SETTINGS.processed_dir / "market_items_new.parquet", 
                    "market_items_new.csv"
                )
            except Exception as e:
                prod_logger.warning(f"Could not save new data backup: {e}")
        else:
            new_data_df = pd.DataFrame()
        
        clean_and_snapshot(new_data_df, out_db)
        
        prod_logger.info("🎉 ETL Pipeline completed successfully!")
        
    except KeyboardInterrupt:
        prod_logger.info("⚠️ ETL Pipeline interrupted by user")
        return 1
    except Exception as e:
        prod_logger.error(f"💥 ETL Pipeline failed with error: {e}", exc_info=True)
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)