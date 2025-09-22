import os, json, csv, urllib.parse, argparse, subprocess, pyodbc, logging, time, uuid
import pandas as pd
import sys
from datetime import datetime
from typing import Dict, List
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
DATA_DIR = CURRENT_DIR / "data"
MARKETPLACES = ["mercadolivre", "magalu", "pneustore"]

QUERY_PNEUS = r"""
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
norm_raw AS (
  SELECT
    *,
    RawMarca   = LTRIM(RTRIM(COALESCE(MARCA_PROD, ''))),
    RawModelo  = LTRIM(RTRIM(COALESCE(REFERENCIA_PROD, NOME, ''))),
    FonteTam   = UPPER(LTRIM(RTRIM(COALESCE(NOME, ''))))
  FROM src
),
norm_cleaned AS (
  SELECT
    *,
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
    TRY_CONVERT(INT, SUBSTRING(FonteUni, use_pos,     3)) AS width,
    TRY_CONVERT(INT, SUBSTRING(FonteUni, use_pos + 4, 2)) AS aspect,
    TRY_CONVERT(INT, SUBSTRING(FonteUni, use_pos + 7, 2)) AS rim
  FROM calc AS c
)
SELECT
  FILIAL,
  COD_PROD,
  Marca       = MarcaNorm,
  Modelo      = ModeloNorm,
  MedidaNorm  = CASE WHEN width IS NOT NULL AND aspect IS NOT NULL AND rim IS NOT NULL
                     THEN CONCAT(width, '/', aspect, 'R', rim) END,
  width, aspect, rim,
  EAN         = CODBARRA_PROD,
  ESTOQUE     = ESTOQUE_PROD,
  PRECO_BASE  = PRECOBASE_PROD,
  DATAULTIMACOMPRA_PROD,
  termo_busca_1 = CONCAT('PNEU ', CONCAT(width, '/', aspect, 'R', rim), ' ', MarcaNorm, ' ', ModeloNorm),
  termo_busca_2 = CONCAT('PNEU ', CONCAT(width, '/', aspect, '/', rim),  ' ', MarcaNorm, ' ', ModeloNorm),
  termo_busca_3 = CONCAT(MarcaNorm, ' ', ModeloNorm, ' ', CONCAT(width, '/', aspect, 'R', rim)),
  size_regex     = CASE WHEN width IS NOT NULL AND aspect IS NOT NULL AND rim IS NOT NULL
                        THEN CONCAT('\\b', width, '[/\\s-]?', aspect, '\\s*R?\\s*', rim, '\\b') END,
  ModifiedAt
FROM sizes
WHERE width IS NOT NULL AND aspect IS NOT NULL AND rim IS NOT NULL
ORDER BY MarcaNorm, ModeloNorm, MedidaNorm, COD_PROD;
"""

QUERY_PECAS = r"""
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
    AND p.ESTOQUE_PROD >= :min_estoque
    AND (p.NOME IS NULL OR LEFT(LTRIM(p.NOME),1) <> :nome_not_prefix)
),
norm AS (
  SELECT
    *,
    RawMarca = LTRIM(RTRIM(COALESCE(MARCA_PROD, ''))),
    RawRef   = LTRIM(RTRIM(COALESCE(REFERENCIA_PROD, NOME, '')))
  FROM src
),
clean AS (
  SELECT
    *,
    MarcaNorm = UPPER(
      CASE 
        WHEN LEFT(RawMarca,1)='(' AND CHARINDEX(')',RawMarca) > 2 THEN LTRIM(SUBSTRING(RawMarca, CHARINDEX(')',RawMarca)+1, LEN(RawMarca)))
        ELSE RawMarca
      END
    ),
    RefNorm = UPPER(
      CASE 
        WHEN LEFT(RawRef,1)='(' AND CHARINDEX(')',RawRef) > 2 THEN LTRIM(SUBSTRING(RawRef, CHARINDEX(')',RawRef)+1, LEN(RawRef)))
        ELSE RawRef
      END
    )
  FROM norm
)
SELECT
  FILIAL,
  COD_PROD,
  Marca       = MarcaNorm,
  Referencia  = RefNorm,
  EAN         = CODBARRA_PROD,
  ESTOQUE     = ESTOQUE_PROD,
  PRECO_BASE  = PRECOBASE_PROD,
  DATAULTIMACOMPRA_PROD,
  ModifiedAt,
  NOME as NOME_ORIGINAL
FROM clean
ORDER BY MarcaNorm, RefNorm, COD_PROD;
"""

def setup_logging(level: str, log_file: Path):
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    fh = logging.FileHandler(str(log_file), encoding="utf-8")
    fh.setFormatter(fmt)
    logger.handlers.clear()
    logger.addHandler(ch)
    logger.addHandler(fh)

def log_kv(level: str, msg: str, **kwargs):
    payload = " ".join(f"{k}={repr(v)}" for k, v in kwargs.items())
    logging.log(getattr(logging, level.upper(), logging.INFO), f"{msg} {payload}".strip())

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
            f"Nenhum driver ODBC da Microsoft encontrado. Instale o 'ODBC Driver 18 for SQL Server' (x64). Drivers encontrados: {installed}"
        )
    server = os.getenv("MSSQL_SERVER")
    db     = os.getenv("MSSQL_DB", "SISTEMAEMPRESARIAL")
    user   = os.getenv("MSSQL_USER")
    pwd    = os.getenv("MSSQL_PWD")
    if not (server and (user and pwd)):
        raise SystemExit("Configure MSSQL_SERVER, MSSQL_USER e MSSQL_PWD (ex.: via .env).")
    encrypt = os.getenv("MSSQL_ENCRYPT", "yes")
    trust   = os.getenv("MSSQL_TRUST_SERVER_CERT", "yes")
    log_kv("INFO", "Conectando no SQL Server", driver=driver, server=server, db=db, encrypt=encrypt, trust=trust)
    odbc = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};DATABASE={db};"
        f"UID={user};PWD={pwd};"
        f"Encrypt={encrypt};TrustServerCertificate={trust};"
    )
    params = urllib.parse.quote_plus(odbc)
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}", pool_pre_ping=True)

def fetch_all(engine, tipo, filial, nome_prefix, nome_not_prefix, min_estoque, year, page_size=None, max_pages=None):
    t0 = time.perf_counter()
    if tipo == "pneus":
        query = QUERY_PNEUS
        params = {"filial": filial, "nome_like": f"{nome_prefix}%", "min_estoque": min_estoque}
    else:
        query = QUERY_PECAS
        params = {"filial": filial, "nome_not_prefix": nome_not_prefix, "min_estoque": min_estoque}
    with engine.begin() as conn:
        rows = conn.execute(text(query), params).mappings().all()
    log_kv("INFO", "Linhas SQL obtidas", count=len(rows), tipo=tipo, elapsed=f"{time.perf_counter()-t0:.3f}s")
    seen = set()
    items: List[Dict] = []
    if tipo == "pneus":
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
    else:
        for r in rows:
            r = dict(r)
            if r["COD_PROD"] in seen:
                continue
            seen.add(r["COD_PROD"])
            brand = (r["Marca"] or "").title()
            ref   = (r["Referencia"] or "").upper()
            nome  = r.get("NOME_ORIGINAL","")
            query_strict = " ".join(x for x in [ref, brand] if x).strip()
            keywords = [k for k in [ref, brand] if k]
            items.append({
                "cod_prod": r["COD_PROD"],
                "brand": brand,
                "reference": ref,
                "original_label": nome,
                "query_strict": query_strict,
                "keywords": keywords,
                "ean_gtin": r.get("EAN"),
            })
    log_kv("INFO", "Itens após dedupe", count=len(items))
    return items

def converter_jsonl_para_csv(lista_arquivos_jsonl: List[str], caminho_saida_csv: str):
    t0 = time.perf_counter()
    todos_os_dados = []
    logging.info("Iniciando conversão de .jsonl para .csv")
    for arquivo in lista_arquivos_jsonl:
        if not Path(arquivo).exists():
            log_kv("WARNING", "Arquivo de resultado não encontrado", path=arquivo)
            continue
        log_kv("INFO", "Lendo dados", path=arquivo)
        with open(arquivo, 'r', encoding='utf-8') as f:
            for linha in f:
                try:
                    todos_os_dados.append(json.loads(linha))
                except json.JSONDecodeError:
                    log_kv("WARNING", "Linha inválida em JSONL, pulando")
    if not todos_os_dados:
        logging.warning("Nenhum dado encontrado nos arquivos .jsonl para converter")
        return
    log_kv("INFO", "Total de registros consolidados", total=len(todos_os_dados))
    df = pd.DataFrame(todos_os_dados)
    df.to_csv(caminho_saida_csv, index=False, sep=';', decimal=',', encoding='utf-8-sig')
    log_kv("INFO", "CSV consolidado salvo", path=caminho_saida_csv, elapsed=f"{time.perf_counter()-t0:.3f}s")

def ensure_data_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    for sub in ("mercadolivre", "magalu", "pneustore", "manifest","query","logs"):
        (DATA_DIR / sub).mkdir(parents=True, exist_ok=True)

def run_scrapers(json_path, cmd_magalu, cmd_meli, cmd_pstore, debug=False, formatos=None,
                 idx_from=0, idx_to=None):
    ensure_data_dirs()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    arquivos_de_saida = []
    env = os.environ.copy()
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
        os.makedirs(Path(meli_out).parent, exist_ok=True)
        log_kv("INFO", "Executando scraper Mercado Livre", cmd=" ".join(meli_cmd))
        t0 = time.perf_counter()
        rc = subprocess.run(meli_cmd, check=False, env=env).returncode
        log_kv("INFO", "Scraper Mercado Livre finalizado", returncode=rc, elapsed=f"{time.perf_counter()-t0:.3f}s")
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
        os.makedirs(Path(magalu_out).parent, exist_ok=True)
        log_kv("INFO", "Executando scraper Magalu", cmd=" ".join(magalu_cmd))
        t0 = time.perf_counter()
        rc = subprocess.run(magalu_cmd, check=False, env=env).returncode
        log_kv("INFO", "Scraper Magalu finalizado", returncode=rc, elapsed=f"{time.perf_counter()-t0:.3f}s")
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
        os.makedirs(Path(pstore_out).parent, exist_ok=True)
        log_kv("INFO", "Executando scraper Pneustore", cmd=" ".join(pstore_cmd))
        t0 = time.perf_counter()
        rc = subprocess.run(pstore_cmd, check=False, env=env).returncode
        log_kv("INFO", "Scraper Pneustore finalizado", returncode=rc, elapsed=f"{time.perf_counter()-t0:.3f}s")
    return arquivos_de_saida

def main():
    project_base_dir = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser()
    ap.add_argument("--tipo", choices=["pecas","pneus"], default="pecas")
    ap.add_argument("--filial", type=int, default=4)
    ap.add_argument("--nome-prefix", default="P")
    ap.add_argument("--nome-not-prefix", default="P")
    ap.add_argument("--min-estoque", type=int, default=1)
    ap.add_argument("--year", type=int, default=2025)
    ap.add_argument("--page-size", type=int, default=5000)
    ap.add_argument("--max-pages", type=int, default=200)
    ap.add_argument("--out-json", default="query_products.json")
    ap.add_argument("--out-csv", default="query_products.csv")
    ap.add_argument("--rodar", action="store_true")
    ap.add_argument("--cmd-magalu", default=str(project_base_dir / "MagazineLuiza" / "scraper.py"))
    ap.add_argument("--cmd-meli",   default=str(project_base_dir / "mercadolivre" / "scraper2.0.py"))
    ap.add_argument("--cmd-pstore", default=str(project_base_dir / "pneustore" / "scraperps.py"))
    ap.add_argument("--formatos", nargs="+", default=["json","csv"])
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--idx-from", type=int, default=0)
    ap.add_argument("--idx-to", type=int, default=None)
    ap.add_argument("--log-level", default="INFO")
    ap.add_argument("--log-file", default=None)
    args = ap.parse_args()
    ensure_data_dirs()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id = f"{ts}_{uuid.uuid4().hex[:6]}"
    log_path = Path(args.log_file) if args.log_file else (DATA_DIR / "logs" / f"lote_{run_id}.log")
    setup_logging(args.log_level, log_path)
    log_kv("INFO", "Início do orquestrador", run_id=run_id, tipo=args.tipo)
    out_json_path = DATA_DIR / "query" / f"{Path(args.out_json).stem}_{ts}.json"
    out_csv_path  = DATA_DIR / "query" / f"{Path(args.out_csv).stem}_{ts}.csv"
    try:
        t0 = time.perf_counter()
        engine = build_engine()
        items = fetch_all(
            engine=engine,
            tipo=args.tipo,
            filial=args.filial,
            nome_prefix=args.nome_prefix,
            nome_not_prefix=args.nome_not_prefix,
            min_estoque=args.min_estoque,
            year=args.year,
            page_size=args.page_size,
            max_pages=args.max_pages
        )
        if not items:
            logging.warning("Nenhum item encontrado no banco de dados. Encerrando.")
            return
        with open(out_json_path, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
        log_kv("INFO", "JSON salvo", path=str(out_json_path.resolve()), itens=len(items))
        if out_csv_path:
            df = pd.DataFrame(items)
            df.to_csv(out_csv_path, index=False, sep=';', encoding='utf-8-sig')
            log_kv("INFO", "CSV salvo", path=str(out_csv_path.resolve()))
        log_kv("INFO", "Geração do lote concluída", elapsed=f"{time.perf_counter()-t0:.3f}s")
        if args.rodar:
            start = max(0, args.idx_from or 0)
            end = len(items) if args.idx_to is None else min(len(items), args.idx_to)
            log_kv("INFO", "Slice a executar nos scrapers", start=start, end=end, total=end-start)
            try:
                lista_jsonl_gerados = run_scrapers(
                    json_path=str(out_json_path.resolve()),
                    cmd_magalu=args.cmd_magalu if args.tipo in ("pneus","pecas") else None,
                    cmd_meli=args.cmd_meli,
                    cmd_pstore=args.cmd_pstore if args.tipo == "pneus" else None,
                    debug=args.debug,
                    formatos=args.formatos,
                    idx_from=start, idx_to=end
                )
                if lista_jsonl_gerados:
                    caminho_csv_final = DATA_DIR / f"dados_consolidados_{ts}.csv"
                    converter_jsonl_para_csv(lista_jsonl_gerados, str(caminho_csv_final.resolve()))
            except KeyboardInterrupt:
                logging.warning("Processo finalizado pelo usuário")
            except Exception as e:
                log_kv("ERROR", "Erro durante execução dos scrapers", error=str(e))
    finally:
        log_kv("INFO", "Script orquestrador finalizado", log_path=str(log_path.resolve()))

if __name__ == "__main__":
    main()
