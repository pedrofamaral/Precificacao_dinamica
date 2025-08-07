# merge_features.py

import os
import sqlite3
import pandas as pd
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

DB_PATH  = os.getenv("DB_PATH", os.path.join(BASE_DIR, "data", "processed", "pricing.db"))
CSV_IN   = os.path.join(BASE_DIR, "data", "raw", "Dados_locais", "sistema.csv")
CSV_OUT  = os.path.join(BASE_DIR, "data", "processed", "Features_locais.csv")

# 1) Carrega CSV local
df_int = pd.read_csv(CSV_IN, skipinitialspace=True, parse_dates=["last_buy_date"])
df_int.columns = df_int.columns.str.strip()
df_int.rename(columns={"last_buy_date": "date"}, inplace=True)
df_int["date"] = df_int["date"].dt.strftime("%Y-%m-%d")

# 2) Valida colunas mínimas
req = ["sku_key","name","date","cost_price","sale_price","stock"]
missing = [c for c in req if c not in df_int.columns]
if missing:
    raise ValueError(f"Faltam colunas no CSV interno: {missing}")

# 3) Lê agregados de concorrência
con = sqlite3.connect(DB_PATH)
df_comp = pd.read_sql_query("""
    SELECT sku_key, date    AS comp_date,
           comp_p10, comp_p50, comp_p90, comp_min, comp_max
    FROM aggregates_daily
""", con)
con.close()

# 4) Pega o snapshot MAIS RECENTE por sku_key
df_comp["comp_date"] = pd.to_datetime(df_comp["comp_date"]).dt.strftime("%Y-%m-%d")
df_comp_latest = (
    df_comp
    .sort_values("comp_date")
    .drop_duplicates("sku_key", keep="last")
    .loc[:, ["sku_key","comp_p10","comp_p50","comp_p90","comp_min","comp_max","comp_date"]]
)

# 5) Merge local x concorrentes por sku_key
df_out = pd.merge(
    df_int,
    df_comp_latest,
    on="sku_key",
    how="left"
)

# 6) Salva features
os.makedirs(os.path.dirname(CSV_OUT), exist_ok=True)
df_out.to_csv(CSV_OUT, index=False)

print(f"✅ Features geradas em {CSV_OUT}")
print(f"   Linhas: {df_out.shape[0]}, Colunas: {df_out.shape[1]}")
if df_out[["comp_p10","comp_p50","comp_p90","comp_min","comp_max"]].isna().any().any():
    print("⚠️ Há NaN em comp_* — verifique se algum sku_key local não existe em aggregates_daily.")
