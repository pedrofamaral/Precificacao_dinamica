# load_internal_data.py
import os, sys, sqlite3, pandas as pd
from dotenv import load_dotenv

load_dotenv()
DB = os.getenv("DB_PATH", "./data/processed/pricing.db")

# Ex.: python load_internal_data.py data/internal.csv
csv_path = sys.argv[1]
df = pd.read_csv(csv_path, parse_dates=["date"])
df["date"] = df["date"].dt.strftime("%Y-%m-%d")  # para texto ISO

con = sqlite3.connect(DB)
df.to_sql("internal_data", con, if_exists="append", index=False)
con.close()
print(f"Inseridos {len(df)} registros em 'internal_data'.")
