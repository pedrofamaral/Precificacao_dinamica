# debug_price.py
import sqlite3, pandas as pd
con = sqlite3.connect("data/processed/pricing.db")
q = """
SELECT raw_price, price, collected_at, marketplace
FROM competitors
WHERE sku_key='pneu-17570r13-goodyear-assurance'
  AND price=182.4
"""
df = pd.read_sql(q, con)
print(df)
