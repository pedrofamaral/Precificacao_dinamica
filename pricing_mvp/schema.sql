PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS competitors (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sku_key TEXT,
  marketplace TEXT,
  title TEXT,
  link TEXT,
  price REAL,        
  raw_price REAL,    
  collected_at TEXT, 
  available INTEGER,
  seller TEXT,
  freight REAL,
  delivery_time_days INTEGER
);

CREATE TABLE IF NOT EXISTS aggregates_daily (
  sku_key TEXT,
  date TEXT,         
  comp_p10 REAL,
  comp_p50 REAL,
  comp_p90 REAL,
  comp_min REAL,
  comp_max REAL,
  PRIMARY KEY (sku_key, date)
);

CREATE TABLE IF NOT EXISTS internal_data (
  sku_key     TEXT,
  date        TEXT,    
  cost_price  REAL,
  sale_price  REAL,
  stock       INTEGER,
  PRIMARY KEY (sku_key, date)
);

-- (Opcional) Regras e dados internos: você pode popular via CSV depois
CREATE TABLE IF NOT EXISTS rules (
  sku_key TEXT PRIMARY KEY,
  map_price REAL,    -- Minimum Advertised Price
  min_margin REAL,   -- ex.: 0.10 (10%)
  min_price REAL,
  max_price REAL
);

CREATE TABLE IF NOT EXISTS costs (
  sku_key TEXT,
  date TEXT,
  cost REAL,
  PRIMARY KEY (sku_key, date)
);

CREATE TABLE IF NOT EXISTS prices_internal (
  sku_key TEXT,
  date TEXT,
  price REAL,
  PRIMARY KEY (sku_key, date)
);

CREATE TABLE IF NOT EXISTS suggestions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sku_key TEXT,
  date TEXT,
  suggested_price REAL,
  rationale TEXT,
  evidence_json TEXT
);
