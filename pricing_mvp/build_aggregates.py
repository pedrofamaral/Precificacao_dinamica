import os
import sqlite3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
DB_PATH = os.getenv("DB_PATH", "./data/processed/pricing.db")

# 2) Conecta e busca todos os registros de competitors
con = sqlite3.connect(DB_PATH)
df = pd.read_sql_query("SELECT sku_key, price, collected_at FROM competitors", con)
con.close()

# 3) Converte data e ordena
df["date"] = pd.to_datetime(df["collected_at"], format='mixed').dt.strftime("%Y-%m-%d")

# 4) **Filtra preços entre R$ 250 e R$ 700**
df = df[(df["price"] >= 250) & (df["price"] <= 700)]

# 5) Agrupa e calcula percentis + min/max
agg = df.groupby(["sku_key", "date"])["price"].agg(
    comp_p10=lambda x: x.quantile(0.10),
    comp_p50=lambda x: x.quantile(0.50),
    comp_p90=lambda x: x.quantile(0.90),
    comp_min="min",
    comp_max="max"
).reset_index()

# 6) Salva na tabela aggregates_daily (substitui tudo)
con = sqlite3.connect(DB_PATH)
con.execute("DROP TABLE IF EXISTS aggregates_daily;")
con.executescript("""
CREATE TABLE aggregates_daily (
  sku_key TEXT,
  date     TEXT,
  comp_p10 REAL,
  comp_p50 REAL,
  comp_p90 REAL,
  comp_min REAL,
  comp_max REAL,
  PRIMARY KEY (sku_key, date)
);
""")
agg.to_sql("aggregates_daily", con, if_exists="append", index=False)
con.close()

print(f"✅ Agregados diários recalculados: {len(agg)} linhas (filtro 250–700 aplicado).")
