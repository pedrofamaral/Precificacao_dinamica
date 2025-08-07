import os, sqlite3
from dotenv import load_dotenv

load_dotenv()
DB = os.environ.get("DB_PATH", "./data/processed/pricing.db")

os.makedirs(os.path.dirname(DB), exist_ok=True)
con = sqlite3.connect(DB)
with open("schema.sql", "r", encoding="utf-8") as f:
    con.executescript(f.read())
con.commit(); con.close()
print("Banco criado em:", DB)
