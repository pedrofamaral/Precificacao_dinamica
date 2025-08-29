# gerar_lote_e_rodar.py
import os, json, csv, urllib.parse, argparse, subprocess, pyodbc
import pandas as pd
from datetime import datetime
from typing import Dict, List
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path
import sys

# O diretório base para dados será determinado pelos argumentos, não mais fixo.
# CURRENT_DIR = Path(__file__).resolve().parent
# DATA_DIR = CURRENT_DIR / "data"
MARKETPLACES = ["mercadolivre", "magalu", "pneustore"]

QUERY = r"""
-- SUA QUERY SQL FICA AQUI (sem alterações)
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
    RawMarca   = LTRIM(RTRIM(COALESCE(MARCA_PROD, ''))),
    RawModelo  = LTRIM(RTRIM(COALESCE(REFERENCIA_PROD, NOME, ''))),
    FonteTam   = UPPER(LTRIM(RTRIM(COALESCE(NOME, ''))))
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
    TRY_CONVERT(INT, SUBSTRING(FonteUni, use_pos,     3)) AS width,
    TRY_CONVERT(INT, SUBSTRING(FonteUni, use_pos + 4, 2)) AS aspect,
    TRY_CONVERT(INT, SUBSTRING(FonteUni, use_pos + 7, 2)) AS rim
  FROM calc AS c
)
SELECT
  FILIAL,
  COD_PROD,
  Marca      = MarcaNorm,
  Modelo     = ModeloNorm,
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
  termo_busca_2 = CONCAT('PNEU ', CONCAT(width, '/', aspect, '/', rim),  ' ', MarcaNorm, ' ', ModeloNorm),
  termo_busca_3 = CONCAT(MarcaNorm, ' ', ModeloNorm, ' ', CONCAT(width, '/', aspect, 'R', rim)),
  size_regex    = CASE WHEN width IS NOT NULL AND aspect IS NOT NULL AND rim IS NOT NULL
                       THEN CONCAT('\\b', width, '[/\\s-]?', aspect, '\\s*R?\\s*', rim, '\\b') END,
  ModifiedAt
FROM sizes
WHERE width IS NOT NULL AND aspect IS NOT NULL AND rim IS NOT NULL
ORDER BY MarcaNorm, ModeloNorm, MedidaNorm, COD_PROD;
"""

# ------------------ DB ------------------
def build_engine():
    # A função load_dotenv continua útil para desenvolvimento local
    load_dotenv()

    candidates = [
        "ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server", "SQL Server Native Client 11.0", "SQL Server"
    ]
    installed = [d.strip() for d in pyodbc.drivers()]
    driver = next((c for c in candidates if c in installed), None)
    if not driver:
        raise SystemExit(f"Nenhum driver ODBC da Microsoft encontrado. Drivers encontrados: {installed}")

    server, db, user, pwd = (os.getenv("MSSQL_SERVER"), os.getenv("MSSQL_DB", "SISTEMAEMPRESARIAL"),
                             os.getenv("MSSQL_USER"), os.getenv("MSSQL_PWD"))
    if not (server and user and pwd):
        raise SystemExit("Configure as variáveis de ambiente: MSSQL_SERVER, MSSQL_USER e MSSQL_PWD.")

    encrypt = os.getenv("MSSQL_ENCRYPT", "yes")
    trust = os.getenv("MSSQL_TRUST_SERVER_CERT", "yes")

    print(f"[INFO] Usando driver ODBC: {driver}")
    odbc = f"DRIVER={{{driver}}};SERVER={server};DATABASE={db};UID={user};PWD={pwd};Encrypt={encrypt};TrustServerCertificate={trust};"
    params = urllib.parse.quote_plus(odbc)
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}", pool_pre_ping=True)

def fetch_all(engine, filial, nome_prefix, min_estoque):
    with engine.begin() as conn:
        rows = conn.execute(text(QUERY), {
            "filial": filial, "nome_like": f"{nome_prefix}%", "min_estoque": min_estoque,
        }).mappings().all()

    print(f"[INFO] Linhas SQL: {len(rows)}")
    seen, items = set(), []
    for r in rows:
        r_dict = dict(r)
        if r_dict["COD_PROD"] in seen: continue
        seen.add(r_dict["COD_PROD"])
        
        w, a, rm = str(r_dict["width"]), str(r_dict["aspect"]), str(r_dict["rim"])
        brand, model = (r_dict["Marca"] or "").title(), (r_dict["Modelo"] or "").title()
        
        items.append({
            "cod_prod": r_dict["COD_PROD"], "brand": brand, "line_model": model,
            "width": w, "aspect": a, "rim": rm, "size_norm": f"{w} {a} r{rm}",
            "original_label": r_dict.get("NOME", ""),
            "query_strict": f"pneu {w} {a} r{rm} {brand} {model}".strip(),
            "keywords": [f"{w}/{a}R{rm}", f"{w} {a} R{rm}", brand, model],
            "size_regex": rf"\b{w}[/\s-]?{a}\s*R?\s*{rm}\b",
            "ean_gtin": r_dict.get("EAN"),
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
                try: todos_os_dados.append(json.loads(linha))
                except json.JSONDecodeError: print(f"[AVISO] Linha inválida no arquivo {arquivo}, pulando.")
    
    if not todos_os_dados:
        print("[AVISO] Nenhum dado encontrado nos arquivos .jsonl para converter.")
        return
    
    print(f"[INFO] Total de {len(todos_os_dados)} registros consolidados.")
    df = pd.DataFrame(todos_os_dados)
    df.to_csv(caminho_saida_csv, index=False, sep=';', decimal=',', encoding='utf-8-sig')
    print(f"[SUCESSO] Arquivo CSV consolidado salvo em: {caminho_saida_csv}")

def run_scrapers(json_path, data_dir, cmd_magalu, cmd_meli, cmd_pstore, debug=False, formatos=None, idx_from=0, idx_to=None):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    arquivos_de_saida = []
    env = os.environ.copy()

    # Garante que o diretório de dados existe
    data_dir_path = Path(data_dir)
    data_dir_path.mkdir(parents=True, exist_ok=True)

    scraper_map = {
        "mercadolivre": (cmd_meli, data_dir_path / "mercadolivre"),
        "magalu": (cmd_magalu, data_dir_path / "magalu"),
        "pneustore": (cmd_pstore, data_dir_path / "pneustore"),
    }

    for name, (cmd_path, out_dir) in scraper_map.items():
        if not cmd_path:
            continue
        
        out_dir.mkdir(parents=True, exist_ok=True)
        output_file = out_dir / f"{ts}.jsonl"
        arquivos_de_saida.append(str(output_file))
        
        command = [
            sys.executable, cmd_path,
            "--lote-json", str(json_path),
            "--idx-from", str(idx_from),
            "--run-id", f"{ts}_{name}",
            "--out-jsonl", str(output_file),
        ]
        if idx_to is not None: command.extend(["--idx-to", str(idx_to)])
        if debug: command.append("--debug")
        if formatos and name in ["magalu", "pneustore"]: command.extend(["--formatos", *formatos])
        
        print("[RUN]", " ".join(command))
        subprocess.run(command, check=False, env=env)
        
    return arquivos_de_saida

def main():
    ap = argparse.ArgumentParser(description="Gera lote de produtos, executa scrapers e consolida os resultados.")
    
    # Argumentos para conexão e query
    ap.add_argument("--filial", type=int, default=4)
    ap.add_argument("--nome-prefix", default="P")
    ap.add_argument("--min-estoque", type=int, default=1)

    # Argumentos para controlar o que o script faz
    ap.add_argument("--rodar", action="store_true", help="Executa os scrapers após gerar o lote.")
    ap.add_argument("--idx-from", type=int, default=0, help="Índice inicial do lote a ser processado.")
    ap.add_argument("--idx-to", type=int, default=None, help="Índice final (exclusivo) do lote.")
    
    # Argumentos para definir caminhos de arquivos e diretórios
    ap.add_argument("--data-dir", default="data", help="Diretório base para salvar todos os arquivos de dados.")
    ap.add_argument("--arquivo-saida", help="Caminho completo para o arquivo CSV consolidado final. Obrigatório se --rodar for usado.")
    
    # Argumentos para os scrapers
    ap.add_argument("--cmd-magalu", help="Caminho para o script scraper da Magazine Luiza.")
    ap.add_argument("--cmd-meli", help="Caminho para o script scraper do Mercado Livre.")
    ap.add_argument("--cmd-pstore", help="Caminho para o script scraper da PneuStore.")
    ap.add_argument("--formatos", nargs="+", default=["csv"], help="Formatos de saída para scrapers que suportam.")
    ap.add_argument("--debug", action="store_true")
    
    args = ap.parse_args()
    
    # Validação de argumentos
    if args.rodar and not args.arquivo_saida:
        raise SystemExit("ERRO: O argumento --arquivo-saida é obrigatório quando --rodar é especificado.")

    # Define os caminhos de saída com base no data-dir
    data_dir_path = Path(args.data_dir)
    data_dir_path.mkdir(parents=True, exist_ok=True)
    lote_json_path = data_dir_path / "lote_de_produtos.json"
    
    print("Conectando ao banco de dados para gerar o lote...")
    engine = build_engine()
    items = fetch_all(engine, args.filial, args.nome_prefix, args.min_estoque)
    
    if not items:
        print("Nenhum item encontrado. Finalizando.")
        return

    with open(lote_json_path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    print(f"Lote de produtos salvo em: {lote_json_path.resolve()} (itens: {len(items)})")

    if args.rodar:
        print("\nIniciando a execução dos scrapers...")
        
        start = max(0, args.idx_from or 0)
        end = len(items) if args.idx_to is None else min(len(items), args.idx_to)
        print(f"[INFO] Slice a ser processado pelos scrapers: {start}:{end} -> {end - start} itens")
        
        lista_jsonl_gerados = run_scrapers(
            json_path=str(lote_json_path.resolve()),
            data_dir=args.data_dir,
            cmd_magalu=args.cmd_magalu,
            cmd_meli=args.cmd_meli,
            cmd_pstore=args.cmd_pstore,
            debug=args.debug,
            formatos=args.formatos,
            idx_from=start,
            idx_to=end
        )

        if lista_jsonl_gerados:
            converter_jsonl_para_csv(lista_jsonl_gerados, args.arquivo_saida)

    print("\n[INFO] Script finalizado com sucesso.")

if __name__ == "__main__":
    main()
