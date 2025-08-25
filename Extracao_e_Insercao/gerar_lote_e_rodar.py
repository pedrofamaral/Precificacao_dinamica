import os, json, csv, urllib.parse, argparse, subprocess, pyodbc
import pandas as pd
from datetime import datetime
from typing import Dict, List
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path
import sys

CURRENT_DIR = Path(__file__).resolve().parent
DATA_DIR = CURRENT_DIR / "data"
MARKETPLACES = ["mercadolivre", "magalu", "pneustore"]

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
),
-- ETAPA 1: Pega os dados brutos como antes, mas sem o UPPER
norm_raw AS (
  SELECT
    *,
    RawMarca   = LTRIM(RTRIM(COALESCE(MARCA_PROD, ''))),
    RawModelo  = LTRIM(RTRIM(COALESCE(REFERENCIA_PROD, NOME, ''))),
    FonteTam   = UPPER(LTRIM(RTRIM(COALESCE(NOME, ''))))
  FROM src
),
-- ETAPA 2: Nova etapa para limpar os prefixos (79), (86H), etc.
norm_cleaned AS (
  SELECT
    *,
    -- Lógica para remover o prefixo: se começar com '(', remove tudo até o ')'
    MarcaNorm = UPPER(
        CASE 
            WHEN LEFT(RawMarca, 1) = '(' AND CHARINDEX(')', RawMarca) > 2 THEN 
                LTRIM(SUBSTRING(RawMarca, CHARINDEX(')', RawMarca) + 1, LEN(RawMarca)))
            ELSE RawMarca
        END
    ),
    ModeloNorm = UPPER(
        CASE 
            WHEN LEFT(RawModelo, 1) = '(' AND CHARINDEX(')', RawModelo) > 2 THEN 
                LTRIM(SUBSTRING(RawModelo, CHARINDEX(')', RawModelo) + 1, LEN(RawModelo)))
            ELSE RawModelo
        END
    )
  FROM norm_raw
),
prep AS (
  SELECT
    n.*,
    FonteUni = REPLACE(REPLACE(REPLACE(REPLACE(n.FonteTam, '-', '/'), ' ', '/'), '/R', 'R'), 'R/', 'R')
  -- A query agora usa a CTE 'norm_cleaned' que contém os dados limpos
  FROM norm_cleaned AS n
),
pos AS (
  SELECT
    p.*,
    pos1 = PATINDEX('%[0-9][0-9][0-9]/[0-9][0-9]R[0-9][0-9]%', p.FonteUni),
    pos2 = PATINDEX('%[0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9]%', p.FonteUni)
  FROM prep AS p
),
calc AS (
  SELECT * ,
         CASE WHEN pos1 > 0 THEN pos1 ELSE pos2 END AS use_pos
  FROM pos
),
sizes AS (
  SELECT
    c.*,
    TRY_CONVERT(INT, SUBSTRING(FonteUni, use_pos,     3)) AS width,
    TRY_CONVERT(INT, SUBSTRING(FonteUni, use_pos + 4, 2)) AS aspect,
    TRY_CONVERT(INT, SUBSTRING(FonteUni, use_pos + 7, 2)) AS rim
  FROM calc AS c
)
SELECT
  FILIAL,
  COD_PROD,
  Marca      = MarcaNorm,
  Modelo     = ModeloNorm,
  MedidaNorm = CASE WHEN width IS NOT NULL AND aspect IS NOT NULL AND rim IS NOT NULL
                    THEN CONCAT(width, '/', aspect, 'R', rim) END,
  width, aspect, rim,
  -- A CORREÇÃO ESTÁ AQUI --
  EAN = CODBARRA_PROD,
  ESTOQUE = ESTOQUE_PROD,
  PRECO_BASE = PRECOBASE_PROD,
  --------------------------
  DATAULTIMACOMPRA_PROD,
  termo_busca_1 = CONCAT('PNEU ', CONCAT(width, '/', aspect, 'R', rim), ' ', MarcaNorm, ' ', ModeloNorm),
  termo_busca_2 = CONCAT('PNEU ', CONCAT(width, '/', aspect, '/', rim),  ' ', MarcaNorm, ' ', ModeloNorm),
  termo_busca_3 = CONCAT(MarcaNorm, ' ', ModeloNorm, ' ', CONCAT(width, '/', aspect, 'R', rim)),
  size_regex    = CASE WHEN width IS NOT NULL AND aspect IS NOT NULL AND rim IS NOT NULL
                       THEN CONCAT('\\b', width, '[/\\s-]?', aspect, '\\s*R?\\s*', rim, '\\b') END,
  ModifiedAt
FROM sizes
WHERE width IS NOT NULL AND aspect IS NOT NULL AND rim IS NOT NULL
ORDER BY MarcaNorm, ModeloNorm, MedidaNorm, COD_PROD;
"""

# ------------------ DB ------------------
def build_engine():
    load_dotenv()

    candidates = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server",
    ]
    installed = [d.strip() for d in pyodbc.drivers()]
    driver = next((c for c in candidates if c in installed), None)
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

def fetch_all(engine, filial, nome_prefix, min_estoque, year, page_size=None, max_pages=None):
    with engine.begin() as conn:
        rows = conn.execute(text(QUERY), {
            "filial": filial,
            "nome_like": f"{nome_prefix}%",
            "min_estoque": min_estoque,
        }).mappings().all()

    print(f"[INFO] Linhas SQL: {len(rows)}")

    seen = set()
    items: List[Dict] = []
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

def converter_jsonl_para_csv(lista_arquivos_jsonl: List[str], caminho_saida_csv: str):
    todos_os_dados = []
    print("\n[INFO] Iniciando conversão de .jsonl para .csv...")
    for arquivo in lista_arquivos_jsonl:
        if not Path(arquivo).exists():
            print(f"[AVISO] Arquivo de resultado não encontrado, pulando: {arquivo}")
            continue
        
        print(f"[INFO] Lendo dados de: {arquivo}")
        with open(arquivo, 'r', encoding='utf-8') as f:
            for linha in f:
                try:
                    todos_os_dados.append(json.loads(linha))
                except json.JSONDecodeError:
                    print(f"[AVISO] Linha inválida no arquivo {arquivo}, pulando linha.")
    
    if not todos_os_dados:
        print("[AVISO] Nenhum dado encontrado nos arquivos .jsonl para converter.")
        return

    print(f"[INFO] Total de {len(todos_os_dados)} registros consolidados.")
    
    df = pd.DataFrame(todos_os_dados)
    
    df.to_csv(caminho_saida_csv, index=False, sep=';', decimal=',', encoding='utf-8-sig')
    print(f"[SUCESSO] Arquivo CSV consolidado salvo em: {caminho_saida_csv}")

def ensure_data_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    for sub in ("mercadolivre", "magalu", "pneustore", "manifest"):
        (DATA_DIR / sub).mkdir(parents=True, exist_ok=True)

def run_scrapers(json_path, cmd_magalu, cmd_meli, cmd_pstore, debug=False, formatos=None,
                 idx_from=0, idx_to=None):
    ensure_data_dirs()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    arquivos_de_saida = []

    env = os.environ.copy()
    # habilitar imports compartilhados, adicionar o Scraper_em_geral ao PYTHONPATH:
    # common_root = (CURRENT_DIR.parent / "Scraper_em_geral").resolve()
    # env["PYTHONPATH"] = str(common_root) + os.pathsep + env.get("PYTHONPATH", "")
    # Mantendo sua ideia (não atrapalha, mas não resolve imports por si só):
    env["PYTHONPATH"] = str(DATA_DIR) + os.pathsep + env.get("PYTHONPATH", "")

    if cmd_meli:
        meli_out = str((DATA_DIR / "mercadolivre" / f"{ts}.jsonl").resolve())
        arquivos_de_saida.append(meli_out)
        meli_cmd = [
            sys.executable, cmd_meli,
            "--lote-json", str(json_path),
            "--idx-from", str(idx_from),
            "--run-id", f"{ts}_mercadolivre",
            "--out-jsonl", meli_out,
        ]
        if idx_to is not None:
            meli_cmd += ["--idx-to", str(idx_to)]
        if debug:
            meli_cmd += ["--debug"]
        print("[RUN]", " ".join(meli_cmd))
        os.makedirs(Path(meli_out).parent, exist_ok=True)
        subprocess.run(meli_cmd, check=False, env=env)

    if cmd_magalu:
        magalu_out = str((DATA_DIR / "magalu" / f"{ts}.jsonl").resolve())
        arquivos_de_saida.append(magalu_out)
        magalu_cmd = [
            sys.executable, cmd_magalu,
            "--lote-json", str(json_path),
            "--idx-from", str(idx_from),
            "--run-id", f"{ts}_magalu",
            "--out-jsonl", magalu_out,
        ]
        if idx_to is not None:
            magalu_cmd += ["--idx-to", str(idx_to)]
        if formatos:
            magalu_cmd += ["--formatos", *formatos]  
        print("[RUN]", " ".join(magalu_cmd))
        os.makedirs(Path(magalu_out).parent, exist_ok=True)
        subprocess.run(magalu_cmd, check=False, env=env)

    if cmd_pstore:
        pstore_out = str((DATA_DIR / "pneustore" / f"{ts}.jsonl").resolve())
        arquivos_de_saida.append(pstore_out)
        pstore_cmd = [
            sys.executable, cmd_pstore,
            "--lote-json", str(json_path),
            "--idx-from", str(idx_from),
            "--run-id", f"{ts}_pneustore",
            "--out-jsonl", pstore_out,
        ]
        if idx_to is not None:
            pstore_cmd += ["--idx-to", str(idx_to)]
        if debug:
            pstore_cmd += ["--debug"]
        if formatos:
            pstore_cmd += ["--formatos", *formatos]
        print("[RUN]", " ".join(pstore_cmd))
        os.makedirs(Path(pstore_out).parent, exist_ok=True)
        subprocess.run(pstore_cmd, check=False, env=env)
        return arquivos_de_saida

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--filial", type=int, default=4)
    ap.add_argument("--nome-prefix", default="P")
    ap.add_argument("--min-estoque", type=int, default=1)
    ap.add_argument("--year", type=int, default=2025)
    ap.add_argument("--page-size", type=int, default=5000)
    ap.add_argument("--max-pages", type=int, default=200)
    ap.add_argument("--out-json", default="query_products.json")  # será forçado para DATA_DIR/<nome>
    ap.add_argument("--out-csv", default="query_products.csv")    # idem
    ap.add_argument("--rodar", action="store_true")
    ap.add_argument("--cmd-magalu", default=r"C:\Users\user\Desktop\Precificação_AI\Scraper_em_geral\MagazineLuiza\scraper.py")
    ap.add_argument("--cmd-meli",   default=r"C:\Users\user\Desktop\Precificação_AI\Scraper_em_geral\mercadolivre\scraper2.0.py")
    ap.add_argument("--cmd-pstore", default=r"C:\Users\user\Desktop\Precificação_AI\Scraper_em_geral\pneustore\scraperps.py")
    ap.add_argument("--formatos", nargs="+", default=["json","csv"])
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--idx-from", type=int, default=0, help="Índice inicial (inclusive) no JSON do lote")
    ap.add_argument("--idx-to", type=int, default=None, help="Índice final (exclusivo) no JSON do lote")
    args = ap.parse_args()
    
    ensure_data_dirs()
    out_json_path = DATA_DIR / Path(args.out_json).name
    out_csv_path  = DATA_DIR / Path(args.out_csv).name 

    print("Conectando ao banco de dados para gerar o lote...")
    engine = build_engine()
    items = fetch_all(engine, args.filial, args.nome_prefix, args.min_estoque, args.year, args.page_size, args.max_pages)

    start = max(0, args.idx_from or 0)
    end = len(items) if args.idx_to is None else min(len(items), args.idx_to)
    print(f"[INFO] Slice aplicado: {start}:{end} -> {end - start} itens")

    if not items:
        print("Nenhum item encontrado.")
        return

    with open(out_json_path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
        print(f"JSON salvo em: {out_json_path.resolve()} (itens: {len(items)})")

    if out_csv_path:
        with open(out_csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(items[0].keys()))
            writer.writeheader()
            writer.writerows(items)
        print(f"CSV salvo em: {out_csv_path.resolve()}")

    if args.rodar:
        while True:
            try:
                print("\nIniciando a execução dos scrapers...")
                lista_jsonl_gerados = run_scrapers(
                    json_path=str(out_json_path.resolve()),
                    cmd_magalu=args.cmd_magalu,
                    cmd_meli=args.cmd_meli,
                    cmd_pstore=args.cmd_pstore,
                    debug=args.debug,
                    formatos=args.formatos,
                    idx_from=start, idx_to=end
                )

                if lista_jsonl_gerados:
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    caminho_csv_final = DATA_DIR / f"dados_consolidados_{ts}.csv"
                    converter_jsonl_para_csv(lista_jsonl_gerados, str(caminho_csv_final.resolve()))
            except KeyboardInterrupt:
                print("\n\n[AVISO] Interrupção detectada no orquestrador!")
                resposta = input("Deseja realmente parar todo o processo? (s/n): ").lower().strip()
                if resposta == 's':
                    print("Processo finalizado pelo usuário.")
                    break
                elif resposta == 'n':
                    print("\n[AVISO] Reiniciando o processo de scraping. Pressione Ctrl+C novamente para sair.")
                    continue
                else:
                    print("\n[AVISO] Resposta não reconhecida. Pressione Ctrl+C novamente para sair.")

    print("\n[INFO] Script orquestrador finalizado.")

if __name__ == "__main__":
    main()
