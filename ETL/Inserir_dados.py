import os
import argparse
import uuid
import urllib.parse
import logging
from logging.handlers import TimedRotatingFileHandler
import json
from pathlib import Path
from datetime import datetime

import pandas as pd
import pyodbc
from sqlalchemy import create_engine, text, event
from sqlalchemy.engine import URL
from dotenv import load_dotenv
from pathlib import Path


# --------------------------- util ---------------------------
LOGGER = logging.getLogger("etl")

def setup_file_logger(log_dir:str, pipeline_name:str, keep_days: int = 30):
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    LOGGER.setlevel(logging.INFO)
    


def log(msg: str):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts} UTC] {msg}")


def build_engine(server=None, db=None, user=None, pwd=None):
    load_dotenv()

    server = server or os.getenv("MSSQL_SERVER")
    db     = db or os.getenv("MSSQL_DB", "COMPARAPRECOS")
    user   = user or os.getenv("MSSQL_USER")
    pwd    = pwd or os.getenv("MSSQL_PWD")

    candidates = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server", "SQL Server"]
    installed = [d.strip() for d in pyodbc.drivers()]
    driver = next((c for c in candidates if c in installed), None)
    if not driver:
        raise SystemExit(f"Nenhum driver ODBC encontrado. Instale Microsoft ODBC Driver 18/17. Drivers atuais: {installed}")

    if not all([server, db, user, pwd]):
        raise SystemExit("Defina MSSQL_SERVER, MSSQL_DB, MSSQL_USER, MSSQL_PWD (ou passe via flags).")

    odbc = f"DRIVER={{{driver}}};SERVER={server};DATABASE={db};UID={user};PWD={pwd};Encrypt=yes;TrustServerCertificate=yes;"
    params = urllib.parse.quote_plus(odbc)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", pool_pre_ping=True, future=True)

    @event.listens_for(engine, "before_cursor_execute")
    def receive_before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        if executemany:
            try:
                cursor.fast_executemany = True
            except Exception:
                pass

    log(f"Conectando '{server}/{db}' com {driver}")
    return engine


def ensure_schemas_and_logs(engine):
    with engine.begin() as conn:
        conn.exec_driver_sql("""
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'stg')  EXEC('CREATE SCHEMA stg');
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'logs') EXEC('CREATE SCHEMA logs');

        IF OBJECT_ID(N'logs.EtlBatches', N'U') IS NULL
        BEGIN
            CREATE TABLE logs.EtlBatches(
                id                INT IDENTITY(1,1) PRIMARY KEY,
                pipeline_name     NVARCHAR(100) NOT NULL,
                tabela_destino    NVARCHAR(256) NOT NULL,
                tabela_staging    NVARCHAR(256) NOT NULL,
                batch_id          NVARCHAR(64) NOT NULL,
                started_at_utc    DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
                finished_at_utc   DATETIME2 NULL,
                status            NVARCHAR(20) NOT NULL,  -- STARTED|SUCCESS|FAILED
                error_message     NVARCHAR(MAX) NULL,
                rows_in_csv       INT NULL,
                rows_in_staging   INT NULL,
                merged_inserted   INT NULL,
                merged_updated    INT NULL,
                soft_deleted      INT NULL
            );
            CREATE INDEX IX_EtlBatches_started ON logs.EtlBatches(started_at_utc);
        END;
        """)


def ensure_tables(conn, staging_fq: str, destino_fq: str, enable_soft_delete: bool):
    conn.exec_driver_sql(f"""
    IF OBJECT_ID(N'{staging_fq}', N'U') IS NULL
    BEGIN
        CREATE TABLE {staging_fq}(
            cod_prod              NVARCHAR(20) NOT NULL,
            product_title         NVARCHAR(500) NULL,
            final_ref_price_mean  DECIMAL(18,2) NULL,
            total_competitors     INT NULL,
            num_marketplaces      INT NULL,
            batch_id              NVARCHAR(64) NOT NULL,
            loaded_at_utc         DATETIME2 NOT NULL
        );
        CREATE INDEX IX_{staging_fq.replace('.', '_')}_cod ON {staging_fq}(cod_prod);
        CREATE INDEX IX_{staging_fq.replace('.', '_')}_batch ON {staging_fq}(batch_id);
    END;
    """)

    conn.exec_driver_sql(f"""
    IF OBJECT_ID(N'{destino_fq}', N'U') IS NULL
    BEGIN
        CREATE TABLE {destino_fq}(
            COD_PROD            NVARCHAR(20) NOT NULL PRIMARY KEY,
            TITULO_PRODUTO      NVARCHAR(500) NULL,
            PRECO_FINAL_MEDIA   DECIMAL(18,2) NULL,
            TOTAL_CONCORRENTES  INT NULL,
            NUM_MARKETPLACES    INT NULL,
            DATA_ATUALIZACAO    DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            CRIADO_EM           DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            ULTIMO_BATCH_ID     NVARCHAR(64) NULL
        );
        CREATE INDEX IX_{destino_fq.replace('.', '_')}_upd ON {destino_fq}(DATA_ATUALIZACAO);
        ALTER TABLE {destino_fq}
          ADD CONSTRAINT CK_{destino_fq.replace('.', '_')}_PRECO_POS CHECK (PRECO_FINAL_MEDIA IS NULL OR PRECO_FINAL_MEDIA >= 0);
    END;
    """)

    if enable_soft_delete:
        conn.exec_driver_sql(f"""
        IF COL_LENGTH('{destino_fq}', 'IS_ACTIVE') IS NULL
        BEGIN
            ALTER TABLE {destino_fq} ADD IS_ACTIVE BIT NOT NULL CONSTRAINT DF_{destino_fq.replace('.','_')}_IS_ACTIVE DEFAULT(1);
            UPDATE {destino_fq} SET IS_ACTIVE = 1;
        END;
        """)


def parse_and_normalize_csv(csv_path: Path) -> pd.DataFrame:
    log(f"Lendo CSV: {csv_path}")
    df = pd.read_csv(csv_path, sep=';', decimal=',', dtype={'cod_prod': str})

    expected = {"cod_prod", "product_title", "final_ref_price_mean", "total_competitors", "num_marketplaces"}
    missing = expected - set(map(str.lower, df.columns))
    if missing:
        raise SystemExit(f"CSV não contém colunas esperadas: {missing}. Colunas recebidas: {list(df.columns)}")

    def parse_decimal(x):
        if pd.isna(x):
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        return float(s.replace('.', '').replace(',', '.'))

    out = pd.DataFrame()
    out["cod_prod"] = df["cod_prod"].astype(str)
    out["product_title"] = df["product_title"].astype(str)
    out["final_ref_price_mean"] = df["final_ref_price_mean"].apply(parse_decimal).astype(float)
    out["total_competitors"] = df["total_competitors"].apply(parse_decimal).astype(float).astype("Int64")
    out["num_marketplaces"]  = df["num_marketplaces"].apply(parse_decimal).astype(float).astype("Int64")
    out["batch_id"] = str(uuid.uuid4())
    out["loaded_at_utc"] = pd.Timestamp.utcnow()
    return out


# --------------------------- main ---------------------------

def main():
    parser = argparse.ArgumentParser(description="ETL | Load Produtos Finais (CSV -> SQL Server)")
    parser.add_argument("csv", help="Caminho do CSV (separador ';', decimal ',').")
    parser.add_argument("--server")
    parser.add_argument("--db")
    parser.add_argument("--user")
    parser.add_argument("--pwd")
    parser.add_argument("--tabela_destino", default="dbo.ProdutosFinais", help="Tabela final (schema.nome)")
    parser.add_argument("--tabela_staging", default="stg.ProdutosFinais_STG", help="Tabela staging (schema.nome)")
    parser.add_argument("--truncate_staging", action="store_true", help="TRUNCATE staging antes de inserir")
    parser.add_argument("--cleanup_staging_days", type=int, default=14, help="Apaga da staging registros mais antigos que N dias (0 = não limpa)")
    parser.add_argument("--soft_delete", action="store_true", help="Ativa soft delete (cria IS_ACTIVE e marca 0 se não veio no lote)")
    parser.add_argument("--pipeline_name", default="ETL_ProdutosFinais", help="Nome p/ logs")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise SystemExit(f"Arquivo CSV não encontrado: {csv_path}")

    df = parse_and_normalize_csv(csv_path)
    batch_id = df["batch_id"].iloc[0]
    rows_csv = len(df)
    log(f"batch_id: {batch_id} | linhas no CSV: {rows_csv}")

    engine = build_engine(args.server, args.db, args.user, args.pwd)

    ensure_schemas_and_logs(engine)

    with engine.begin() as conn:
        row = conn.execute(
            text("""INSERT INTO logs.EtlBatches
                    (pipeline_name, tabela_destino, tabela_staging, batch_id, status, rows_in_csv)
                    OUTPUT INSERTED.id
                    VALUES (:p, :d, :s, :b, 'STARTED', :n)"""),
            {"p": args.pipeline_name, "d": args.tabela_destino, "s": args.tabela_staging, "b": batch_id, "n": rows_csv}
        ).fetchone()
        log_id = int(row[0])
        log(f"Log STARTED id={log_id}")

    try:
        with engine.begin() as conn:
            ensure_tables(conn, args.tabela_staging, args.tabela_destino, args.soft_delete)

            if args.truncate_staging:
                log(f"TRUNCATE {args.tabela_staging}")
                conn.exec_driver_sql(f"TRUNCATE TABLE {args.tabela_staging}")
            elif args.cleanup_staging_days and args.cleanup_staging_days > 0:
                log(f"Limpando staging com retenção de {args.cleanup_staging_days} dias…")
                conn.exec_driver_sql(f"""
                    DELETE FROM {args.tabela_staging}
                    WHERE loaded_at_utc < DATEADD(day, -{int(args.cleanup_staging_days)}, SYSUTCDATETIME())
                """)

            log(f"Inserindo {len(df)} linhas em {args.tabela_staging}…")
            schema_name, table_name = (args.tabela_staging.split('.', 1) + [None])[:2]
            df.to_sql(
                name=table_name,
                schema=schema_name,
                con=conn,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )

            rows_in_staging = conn.execute(
                text(f"SELECT COUNT(*) FROM {args.tabela_staging} WHERE batch_id = :b"),
                {"b": batch_id}
            ).scalar_one()

            log("Executando MERGE…")
            merge_sql = f"""
            DECLARE @do_soft_delete BIT = :do_soft_delete;

            -- Garante destino (pode já existir; se não existir, cria compatível)
            IF OBJECT_ID(N'{args.tabela_destino}', N'U') IS NULL
            BEGIN
                CREATE TABLE {args.tabela_destino}(
                    COD_PROD            NVARCHAR(20) NOT NULL PRIMARY KEY,
                    TITULO_PRODUTO      NVARCHAR(500) NULL,
                    PRECO_FINAL_MEDIA   DECIMAL(18,2) NULL,
                    TOTAL_CONCORRENTES  INT NULL,
                    NUM_MARKETPLACES    INT NULL,
                    DATA_ATUALIZACAO    DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
                    CRIADO_EM           DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
                    ULTIMO_BATCH_ID     NVARCHAR(64) NULL
                );
                CREATE INDEX IX_{args.tabela_destino.replace('.', '_')}_upd ON {args.tabela_destino}(DATA_ATUALIZACAO);
            END;

            IF @do_soft_delete = 1 AND COL_LENGTH('{args.tabela_destino}', 'IS_ACTIVE') IS NULL
            BEGIN
                ALTER TABLE {args.tabela_destino} ADD IS_ACTIVE BIT NOT NULL CONSTRAINT DF_{args.tabela_destino.replace('.','_')}_IS_ACTIVE DEFAULT(1);
                UPDATE {args.tabela_destino} SET IS_ACTIVE = 1;
            END;

            DECLARE @merge_out TABLE(action NVARCHAR(10));
            MERGE {args.tabela_destino} WITH (HOLDLOCK) AS tgt
            USING (
                SELECT s.cod_prod,
                       s.product_title,
                       s.final_ref_price_mean,
                       s.total_competitors,
                       s.num_marketplaces,
                       s.batch_id
                FROM {args.tabela_staging} s
                WHERE s.batch_id = :batch_id
            ) AS src
            ON tgt.COD_PROD = src.cod_prod

            WHEN MATCHED THEN
                UPDATE SET
                    tgt.TITULO_PRODUTO     = src.product_title,
                    tgt.PRECO_FINAL_MEDIA  = TRY_CONVERT(DECIMAL(18,2), src.final_ref_price_mean),
                    tgt.TOTAL_CONCORRENTES = TRY_CONVERT(INT, src.total_competitors),
                    tgt.NUM_MARKETPLACES   = TRY_CONVERT(INT, src.num_marketplaces),
                    tgt.DATA_ATUALIZACAO   = SYSUTCDATETIME(),
                    tgt.ULTIMO_BATCH_ID    = src.batch_id
                    {", tgt.IS_ACTIVE = 1" if args.soft_delete else ""}

            WHEN NOT MATCHED BY TARGET THEN
                INSERT (COD_PROD, TITULO_PRODUTO, PRECO_FINAL_MEDIA, TOTAL_CONCORRENTES, NUM_MARKETPLACES, DATA_ATUALIZACAO, CRIADO_EM, ULTIMO_BATCH_ID {", IS_ACTIVE" if args.soft_delete else ""})
                VALUES (src.cod_prod, src.product_title, TRY_CONVERT(DECIMAL(18,2), src.final_ref_price_mean),
                        TRY_CONVERT(INT, src.total_competitors), TRY_CONVERT(INT, src.num_marketplaces),
                        SYSUTCDATETIME(), SYSUTCDATETIME(), src.batch_id {", 1" if args.soft_delete else ""})

            OUTPUT $action INTO @merge_out(action);

            DECLARE @ins INT = (SELECT COUNT(*) FROM @merge_out WHERE action = 'INSERT');
            DECLARE @upd INT = (SELECT COUNT(*) FROM @merge_out WHERE action = 'UPDATE');
            DECLARE @soft INT = 0;

            IF @do_soft_delete = 1
            BEGIN
                UPDATE tgt
                   SET tgt.IS_ACTIVE = 0,
                       tgt.DATA_ATUALIZACAO = SYSUTCDATETIME()
                  FROM {args.tabela_destino} tgt
                  WHERE tgt.IS_ACTIVE = 1
                    AND NOT EXISTS (
                        SELECT 1 FROM {args.tabela_staging} s
                        WHERE s.batch_id = :batch_id AND s.cod_prod = tgt.COD_PROD
                    );
                SET @soft = @@ROWCOUNT;
            END;

            SELECT @ins AS inserted, @upd AS updated, @soft AS soft_deleted;
            """

            res = conn.execute(text(merge_sql), {"batch_id": batch_id, "do_soft_delete": 1 if args.soft_delete else 0})
            counts = res.mappings().first()
            merged_inserted = int(counts["inserted"])
            merged_updated  = int(counts["updated"])
            soft_deleted    = int(counts["soft_deleted"])

            conn.execute(
                text("""UPDATE logs.EtlBatches
                        SET finished_at_utc = SYSUTCDATETIME(),
                            status = 'SUCCESS',
                            rows_in_staging = :r,
                            merged_inserted = :i,
                            merged_updated  = :u,
                            soft_deleted    = :s
                        WHERE id = :id"""),
                {"r": rows_in_staging, "i": merged_inserted, "u": merged_updated, "s": soft_deleted, "id": log_id}
            )

            log(f"MERGE OK | inserted={merged_inserted} updated={merged_updated} soft_deleted={soft_deleted} | staging_rows={rows_in_staging}")

    except Exception as e:
        err = str(e)[:4000]
        log(f"ERRO: {err}")
        with engine.begin() as conn:
            conn.execute(
                text("""UPDATE logs.EtlBatches
                        SET finished_at_utc = SYSUTCDATETIME(),
                            status = 'FAILED',
                            error_message = :err
                        WHERE id = :id"""),
                {"err": err, "id": log_id}
            )
        raise

    log("Carga finalizada com sucesso.")


if __name__ == "__main__":
    main()