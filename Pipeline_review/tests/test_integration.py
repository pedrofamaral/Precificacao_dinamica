import sys
import subprocess
from pathlib import Path
import sqlite3
import csv
import time

# Ajuda a escrever CSV com UTF-8 no Windows também
def write_csv(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)

def test_end_to_end_cli_multiple_inputs(tmp_path: Path):
    """
    Gera dados sintéticos para 3 marketplaces, roda o script via CLI e valida:
    - unified_listings possui 'source_file' com tags usando '/'
    - canonical_summary agrega 'evidence_files' com as três fontes
    - marketplaces agregados estão presentes
    """
    project_root = Path(__file__).resolve().parent.parent
    script = project_root / "unificar_marketplaces.py"
    assert script.exists(), "unificar_marketplaces.py não encontrado no nível acima de tests/"

    # --------- 1) Fixtures de entrada (3 marketplaces) ----------
    # MercadoLivre
    ml_base = tmp_path / "PriceMonitor" / "MercadoLivre" / "data" / "raw"
    ml_csv = ml_base / "lote_ml.csv"
    write_csv(ml_csv, [
        {
            "title": "Pneu Dunlop SP Touring FM800 195/65R15 91H",
            "price": "200.0",
            "url": "https://ml/item1",
            "collected_at": "2025-08-12T10:00:00",
            "seller": "Loja ML"
        }
    ])

    # MagazineLuiza
    mglu_base = tmp_path / "PriceMonitor" / "MagazineLuiza" / "data" / "raw"
    mglu_csv = mglu_base / "lote_mglu.csv"
    write_csv(mglu_csv, [
        {
            "title": "Pneu Dunlop SP Touring FM800 195/65R15 91H",
            "price": "210.0",
            "url": "https://mglu/item1",
            "collected_at": "2025-08-12T11:00:00",
            "seller": "Loja MGLU"
        }
    ])

    # pneustore
    pneu_base = tmp_path / "PriceMonitor" / "pneustore" / "dados" / "raw"
    pneu_csv = pneu_base / "lote_pn.csv"
    write_csv(pneu_csv, [
        {
            "title": "Pneu Dunlop SP Touring FM800 195/65R15 91H",
            "price": "205.0",
            "url": "https://pn/item1",
            "collected_at": "2025-08-12T09:30:00",
            "seller": "Loja PN"
        }
    ])

    # --------- 2) Executa o script via CLI com múltiplos --input ----------
    out_db = tmp_path / "unified_products.db"
    cmd = [
        sys.executable,
        str(script),
        "--input", str(ml_base), str(mglu_base), str(pneu_base),
        "--output", str(out_db)
    ]
    # Se o seu script tiver flags de filtro, você pode acrescentar aqui.
    print("Running:", " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd=str(project_root))
    if proc.returncode != 0:
        print("STDOUT:\n", proc.stdout)
        print("STDERR:\n", proc.stderr)
    assert proc.returncode == 0, "Falha ao executar o script (veja stdout/stderr acima)."

    assert out_db.exists(), "DB de saída não foi gerado."

    # --------- 3) Validações no SQLite ----------
    con = sqlite3.connect(str(out_db))
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # a) Tabelas e colunas
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {r[0] for r in cur.fetchall()}
    assert "unified_listings" in tables, "Tabela unified_listings ausente."
    assert "canonical_summary" in tables, "Tabela canonical_summary ausente."

    def columns(table):
        cur.execute(f"PRAGMA table_info({table})")
        return {r[1] for r in cur.fetchall()}

    ul_cols = columns("unified_listings")
    cs_cols = columns("canonical_summary")

    for col in ["marketplace","title","price","url","brand","model","size","canonical_key","collected_at","seller","source_file"]:
        assert col in ul_cols, f"Coluna {col} ausente em unified_listings."

    for col in ["canonical_key","brand","model","size","n_listings","marketplaces","min_price","max_price","mean_price","median_price","p10","p90","media_correta","evidence_files"]:
        assert col in cs_cols, f"Coluna {col} ausente em canonical_summary."

    # b) unified_listings tem 3 linhas (uma por marketplace) e source_file sane
    cur.execute("SELECT COUNT(*) FROM unified_listings")
    n = cur.fetchone()[0]
    assert n == 3, f"Esperava 3 linhas em unified_listings, veio {n}."

    # c) source_file deve usar '/' e incluir os tails esperados
    cur.execute("SELECT DISTINCT source_file FROM unified_listings ORDER BY 1")
    sources = [r[0] for r in cur.fetchall()]
    assert all("\\" not in s for s in sources), f"source_file com '\\': {sources}"
    assert any(s.endswith("MercadoLivre/data/raw/lote_ml.csv") for s in sources), f"Fonte MercadoLivre ausente: {sources}"
    assert any(s.endswith("MagazineLuiza/data/raw/lote_mglu.csv") for s in sources), f"Fonte MagazineLuiza ausente: {sources}"
    assert any(s.endswith("pneustore/dados/raw/lote_pn.csv") for s in sources), f"Fonte pneustore ausente: {sources}"

    # d) canonical_summary agregou 1 linha para este (brand,model,size), com evidence_files contendo as 3 fontes
    cur.execute("""
        SELECT brand, model, size, n_listings, marketplaces, evidence_files
        FROM canonical_summary
        ORDER BY n_listings DESC
        LIMIT 1
    """)
    row = cur.fetchone()
    assert row is not None, "canonical_summary vazio."
    brand, model, size, n_listings, marketplaces, evidence = row
    # marca/modelo/medida devem estar preenchidos
    assert brand and model and size, f"Tripla canônica incompleta: {(brand, model, size)}"
    assert n_listings == 3, f"Esperava n_listings=3, veio {n_listings}"
    # marketplaces deve incluir pelo menos 2/3 nomes (dependendo de como você os normaliza)
    assert "Mercado" in marketplaces or "mercado" in marketplaces or "mercadolivre" in marketplaces, f"marketplaces não inclui MercadoLivre: {marketplaces}"
    # as três evidências
    assert "MercadoLivre/data/raw/lote_ml.csv" in evidence, f"evidence_files sem ML: {evidence}"
    assert "MagazineLuiza/data/raw/lote_mglu.csv" in evidence, f"evidence_files sem MGLU: {evidence}"
    assert "pneustore/dados/raw/lote_pn.csv" in evidence, f"evidence_files sem pneustore: {evidence}"

    con.close()
