import os, json, csv, urllib.parse, argparse, subprocess, pyodbc
from datetime import datetime
from typing import Dict, List
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path

MARKETPLACES = ["mercadolivre", "magalu", "pneustore"]
PROJECT_ROOT = Path(__file__).resolve().parents[1]

QUERY = r"""
WITH src AS (
  SELECT
    p.FILIAL,
    p.COD_PROD,
    p.NOME,
    p.MARCA_PROD,
    p.REFERENCIA_PROD,
    p.CODBARRA_PROD,
    p.ESTOQUE_PROD,
    p.PRECOBASE_PROD,
    p.DATAULTIMACOMPRA_PROD,
    COALESCE(p.DATAATUALIZACAOPRECOBASE_PROD, p.DATAULTIMACOMPRA_PROD) AS ModifiedAt
  FROM dbo.PRODUTOS AS p
  WHERE p.FILIAL = :filial
    AND p.NOME LIKE :nome_like
    AND p.ESTOQUE_PROD >= :min_estoque
    AND p.DATAULTIMACOMPRA_PROD >= :date_from
    AND p.DATAULTIMACOMPRA_PROD <  :date_to
),
norm AS (
  SELECT
    FILIAL,
    COD_PROD,
    EAN         = CODBARRA_PROD,
    ESTOQUE     = ESTOQUE_PROD,
    PRECO_BASE  = PRECOBASE_PROD,
    DATAULTIMACOMPRA_PROD,
    ModifiedAt,
    MarcaNorm   = UPPER(LTRIM(RTRIM(COALESCE(MARCA_PROD, '')))),
    ModeloNorm  = UPPER(LTRIM(RTRIM(COALESCE(REFERENCIA_PROD, NOME, '')))),
    FonteTam    = UPPER(LTRIM(RTRIM(COALESCE(NOME, ''))))
  FROM src
),
prep AS (
  SELECT
    n.*,
    FonteUni = REPLACE(REPLACE(REPLACE(REPLACE(n.FonteTam, '-', '/'), ' ', '/'), '/R', 'R'), 'R/', 'R')
  FROM norm AS n
),
pos AS (
  SELECT
    p.*,
    pos1 = PATINDEX('%[0-9][0-9][0-9]/[0-9][0-9]R[0-9][0-9]%', p.FonteUni),
    pos2 = PATINDEX('%[0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9]%', p.FonteUni)
  FROM prep AS p
),
calc AS (
  SELECT *,
         CASE WHEN pos1 > 0 THEN pos1 ELSE pos2 END AS use_pos
  FROM pos
),
sizes AS (
  SELECT
    c.*,
    TRY_CONVERT(INT, SUBSTRING(FonteUni, use_pos,     3)) AS width,
    TRY_CONVERT(INT, SUBSTRING(FonteUni, use_pos + 4, 2)) AS aspect,
    TRY_CONVERT(INT, SUBSTRING(FonteUni, use_pos + 7, 2)) AS rim
  FROM calc AS c
)
SELECT
  FILIAL,
  COD_PROD,
  Marca      = MarcaNorm,
  Modelo     = ModeloNorm,
  MedidaNorm = CASE WHEN width IS NOT NULL AND aspect IS NOT NULL AND rim IS NOT NULL
                    THEN CONCAT(width, '/', aspect, 'R', rim) END,
  width, aspect, rim,
  EAN,
  ESTOQUE,
  PRECO_BASE,
  DATAULTIMACOMPRA_PROD,
  termo_busca_1 = CONCAT('PNEU ', CONCAT(width, '/', aspect, 'R', rim), ' ', MarcaNorm, ' ', ModeloNorm),
  termo_busca_2 = CONCAT('PNEU ', CONCAT(width, '/', aspect, '/', rim),  ' ', MarcaNorm, ' ', ModeloNorm),
  termo_busca_3 = CONCAT(MarcaNorm, ' ', ModeloNorm, ' ', CONCAT(width, '/', aspect, 'R', rim)),
  size_regex    = CASE WHEN width IS NOT NULL AND aspect IS NOT NULL AND rim IS NOT NULL
                       THEN CONCAT('\\b', width, '[/\\s-]?', aspect, '\\s*R?\\s*', rim, '\\b') END,
  ModifiedAt
FROM sizes
WHERE width IS NOT NULL AND aspect IS NOT NULL AND rim IS NOT NULL
ORDER BY MarcaNorm, ModeloNorm, MedidaNorm, COD_PROD;
"""

def build_engine():
    load_dotenv()

    candidates = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "SQL Server Native Client 11.0",  
        "SQL Server"                      
    ]
    installed = [d.strip() for d in pyodbc.drivers()]
    driver = None
    for c in candidates:
        if c in installed:
            driver = c
            break
    if not driver:
        raise SystemExit(
            f"Nenhum driver ODBC da Microsoft encontrado. Instale o 'ODBC Driver 18 for SQL Server' (x64). "
            f"Drivers encontrados: {installed}"
        )

    server = os.getenv("MSSQL_SERVER")
    db     = os.getenv("MSSQL_DB", "SISTEMAEMPRESARIAL")
    user   = os.getenv("MSSQL_USER")
    pwd    = os.getenv("MSSQL_PWD")
    if not (server and (user and pwd)):
        raise SystemExit("Configure MSSQL_SERVER, MSSQL_USER e MSSQL_PWD (ex.: via .env).")

    encrypt = os.getenv("MSSQL_ENCRYPT", "yes")
    trust   = os.getenv("MSSQL_TRUST_SERVER_CERT", "yes")

    print(f"[INFO] Usando driver ODBC: {driver}")

    odbc = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};DATABASE={db};"
        f"UID={user};PWD={pwd};"
        f"Encrypt={encrypt};TrustServerCertificate={trust};"
    )
    params = urllib.parse.quote_plus(odbc)
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}", pool_pre_ping=True)


def row_to_item(r: Dict) -> Dict:
    w = str(r["width"])
    a = str(r["aspect"])
    rim = str(r["rim"])
    brand = " ".join(p.capitalize() for p in (r.get("Marca") or "").split())
    model = " ".join(p.capitalize() for p in (r.get("Modelo") or "").split())
    size_norm = f"{w} {a} r{rim}"
    k1 = f"{w}/{a}R{rim}"
    k2 = f"{w} {a} R{rim}"
    k3 = f"{w} {a} r{rim}"
    query_strict = f"pneu {k3} {brand} {model}".strip()
    return {
        "cod_prod": r["COD_PROD"],
        "width": w,
        "aspect": a,
        "rim": rim,
        "size_norm": size_norm,
        "brand": brand,
        "line_model": model,
        "original_label": (r.get("NOME") or "").strip(),
        "query_strict": query_strict,
        "keywords": [k1, k2, k3, brand, model],
        "size_regex": rf"\b{w}[/\s-]?{a}\s*R?\s*{rim}\b",
        "ean_gtin": r.get("EAN"),
        "estoque": int(r.get("ESTOQUE") or 0),
        "preco_base": float(r.get("PRECO_BASE") or 0.0)
    }

def fetch_all(engine, filial, nome_prefix, min_estoque, year, page_size=None, max_pages=None):
    date_from = f"{year}-01-01"
    date_to   = f"{year+1}-01-01"
    with engine.begin() as conn:
        rows = conn.execute(text(QUERY), {
            "filial": filial,
            "nome_like": f"{nome_prefix}%",
            "min_estoque": min_estoque,
            "date_from": date_from,
            "date_to": date_to,
        }).mappings().all()

    print(f"[INFO] Linhas SQL: {len(rows)}")

    seen = set()
    items = []
    for r in rows:
        r = dict(r)
        if r["COD_PROD"] in seen:
            continue
        seen.add(r["COD_PROD"])
        w, a, rm = str(r["width"]), str(r["aspect"]), str(r["rim"])
        brand  = (r["Marca"] or "").title()
        model  = (r["Modelo"] or "").title()
        items.append({
            "cod_prod": r["COD_PROD"],
            "brand": brand,
            "line_model": model,
            "width": w, "aspect": a, "rim": rm,
            "size_norm": f"{w} {a} r{rm}",
            "original_label": r.get("NOME",""),
            "query_strict": f"pneu {w} {a} r{rm} {brand} {model}".strip(),
            "keywords": [f"{w}/{a}R{rm}", f"{w} {a} R{rm}", brand, model],
            "size_regex": rf"\b{w}[/\s-]?{a}\s*R?\s*{rm}\b",
            "ean_gtin": r.get("EAN"),
        })
    print(f"[INFO] Após dedupe por COD_PROD: {len(items)}")
    return items

def run_scrapers(json_path, cmd_magalu, cmd_meli, cmd_pstore, debug, formatos, base_out_dir=None,
                 idx_from=0, idx_to=375):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_out_dir = base_out_dir or os.path.join(os.path.dirname(json_path), "out_jsonl")
    os.makedirs(base_out_dir, exist_ok=True)
    common = ['--lote-json', json_path, '--idx-from', str(idx_from)]
    if idx_to is not None:
        common += ['--idx-to', str(idx_to)]

    cmds = []
    if cmd_meli:
        cmds.append(['python', cmd_meli, '--lote-json', json_path,
                     '--idx-from', str(idx_from), '--idx-to', str(idx_to),
                     '--run-id', f'{ts}_mercadolivre',
                     '--out-jsonl', os.path.join(base_out_dir, 'mercadolivre', f'{ts}.jsonl'),
                     '--formatos', *formatos] + (['--debug'] if debug else []))
    if cmd_magalu:
        cmds.append(['python', cmd_magalu, '--lote-json', json_path,
                     '--idx-from', str(idx_from), '--idx-to', str(idx_to),
                     '--run-id', f'{ts}_magalu',
                     '--out-jsonl', os.path.join(base_out_dir, 'magalu', f'{ts}.jsonl'),
                     '--formatos', *formatos] + (['--debug'] if debug else []))
    if cmd_pstore:
        cmds.append(['python', cmd_pstore, '--lote-json', json_path,
                     '--idx-from', str(idx_from), '--idx-to', str(idx_to),
                     '--run-id', f'{ts}_pneustore',
                     '--out-jsonl', os.path.join(base_out_dir, 'pneustore', f'{ts}.jsonl'),
                     '--formatos', *formatos] + (['--debug'] if debug else []))
    
    env = os.environ.copy()
    env['PYTHONPATH'] = str(PROJECT_ROOT) + os.pathsep + env.get('PYTHONPATH', '')

    for c in cmds:
        print("[RUN]", " ".join(c))
        os.makedirs(os.path.dirname(c[c.index('--out-jsonl')+1]), exist_ok=True)
        subprocess.run(c, check=False)



def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--filial", type=int, default=4)
    ap.add_argument("--nome-prefix", default="P")
    ap.add_argument("--min-estoque", type=int, default=1)
    ap.add_argument("--year", type=int, default=2025)
    ap.add_argument("--page-size", type=int, default=5000)
    ap.add_argument("--max-pages", type=int, default=200)
    ap.add_argument("--out-json", default="query_products.json")
    ap.add_argument("--out-csv", default="")
    ap.add_argument("--rodar", action="store_true")
    ap.add_argument("--cmd-magalu", default=r"C:\Users\user\Desktop\Precificação_AI\Scraper_em_geral\MagazineLuiza\scraper.py")
    ap.add_argument("--cmd-meli",   default=r"C:\Users\user\Desktop\Precificação_AI\Scraper_em_geral\mercadolivre\scraper2.0.py")
    ap.add_argument("--cmd-pstore", default=r"C:\Users\user\Desktop\Precificação_AI\Scraper_em_geral\pneustore\scraperps.py")
    ap.add_argument("--formatos", nargs="+", default=["csv","json"])
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--idx-from", type=int, default=0, help="Índice inicial (inclusive) no JSON do lote")
    ap.add_argument("--idx-to", type=int, default=None, help="Índice final (exclusivo) no JSON do lote")

    args = ap.parse_args()

    engine = build_engine()
    items = fetch_all(engine, args.filial, args.nome_prefix, args.min_estoque, args.year, args.page_size, args.max_pages)
    start = max(0, args.idx_from or 0)
    end = len(items) if args.idx_to is None else min(len(items), args.idx_to)
    subset = items[start:end]
    print(f"[INFO] Slice aplicado: {start}:{end} -> {len(subset)} itens")

    if not items:
        print("Nenhum item encontrado.")
        return

    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    print(f"JSON salvo em: {os.path.abspath(args.out_json)} (itens: {len(items)})")

    if args.out_csv:
        with open(args.out_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(items[0].keys()))
            writer.writeheader()
            writer.writerows(items)
        print(f"CSV salvo em: {os.path.abspath(args.out_csv)}")

    if args.rodar:
      run_scrapers(
        os.path.abspath(args.out_json),
        args.cmd_magalu, args.cmd_meli, args.cmd_pstore,
        args.debug, args.formatos,
        idx_from=start, idx_to=end
    )


if __name__ == "__main__":
    main()
