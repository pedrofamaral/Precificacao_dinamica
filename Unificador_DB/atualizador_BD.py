# Nome do arquivo: carregar_precos_mercado.py

import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# ==============================================================================
# PARTE A: CONEXÃO COM O BANCO (Continua a mesma)
# ==============================================================================
def conectar_ao_banco():
    """
    Lê as credenciais do arquivo .env, monta a URL de conexão
    e cria uma engine do SQLAlchemy para se conectar ao banco.
    """
    load_dotenv()

    dialect = os.getenv("DB_DIALECT")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    dbname = os.getenv("DB_NAME")

    if not all([dialect, user, password, host, port, dbname]):
        raise ValueError("Uma ou mais variáveis de ambiente do banco de dados não foram definidas no arquivo .env")

    if "mssql" in dialect:
        driver = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server").replace(" ", "+")
        password_quoted = quote_plus(password)
        database_url = f"{dialect}://{user}:{password_quoted}@{host}:{port}/{dbname}?driver={driver}"
    else:
        database_url = f"{dialect}://{user}:{password}@{host}:{port}/{dbname}"

    try:
        print("Conectando ao banco de dados da empresa...")
        engine = create_engine(database_url)
        with engine.connect() as connection:
            print("Conexão bem-sucedida!")
        return engine
    except Exception as e:
        print(f"ERRO: Falha ao conectar ao banco de dados: {e}")
        return None

# ==============================================================================
# PARTE B: LÓGICA DE CARGA PARA AS TABELAS DIÁRIA E HISTÓRICA
# ==============================================================================
def carregar_precos_para_banco(df: pd.DataFrame, engine):
    TABELA_DIARIA = "PRECOS_MERCADO"
    TABELA_HISTORICA = "HISTORICO_PRECOS_MERCADO"
    SCHEMA = "dbo" 

    df_para_inserir = df[df['preco_mercado'].notna()].copy()
    
    colunas_db = {
        'COD_PROD': 'cod_prod_interno',
        'preco_mercado': 'preco_mercado',
        'url_mercado': 'url_concorrente',
        'vendedor_mercado': 'vendedor_concorrente',
        'marketplace_mercado': 'marketplace',
        'captured_at': 'data_coleta'
    }
    df_para_inserir = df_para_inserir[colunas_db.keys()].rename(columns=colunas_db)
    df_para_inserir['data_coleta'] = pd.to_datetime(df_para_inserir['data_coleta'], errors='coerce')

    if df_para_inserir.empty:
        print("Nenhum produto com preço de mercado encontrado para carregar.")
        return

    print(f"Preparando para carregar {len(df_para_inserir)} registros...")

    sql_create_template = text(f"""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{{table_name}}' and xtype='U')
    CREATE TABLE {SCHEMA}.{{table_name}} (
        cod_prod_interno VARCHAR(50),
        preco_mercado DECIMAL(10, 2),
        url_concorrente VARCHAR(1024),
        vendedor_concorrente VARCHAR(255),
        marketplace VARCHAR(100),
        data_coleta DATETIME
    )
    """)
    
    try:
        with engine.begin() as conn: 
            
            print(f"--> Atualizando tabela diária: {SCHEMA}.{TABELA_DIARIA}")
            conn.execute(sql_create_template.format(table_name=TABELA_DIARIA))
            conn.execute(text(f"DELETE FROM {SCHEMA}.{TABELA_DIARIA}"))
            df_para_inserir.to_sql(name=TABELA_DIARIA, con=conn, schema=SCHEMA, if_exists='append', index=False, chunksize=500)
            print(f"Tabela diária atualizada com {len(df_para_inserir)} registros.")
            
            print(f"--> Atualizando tabela histórica: {SCHEMA}.{TABELA_HISTORICA}")
            conn.execute(sql_create_template.format(table_name=TABELA_HISTORICA))
            df_para_inserir.to_sql(name=TABELA_HISTORICA, con=conn, schema=SCHEMA, if_exists='append', index=False, chunksize=500)
            print("Novos registros adicionados à tabela histórica.")

        print("\nCarga de dados para as tabelas diária e histórica concluída com sucesso!")

    except Exception as e:
        print(f"ERRO: Falha durante a carga de dados para o banco. A transação foi revertida (rollback). Erro: {e}")

# ==============================================================================
# PARTE C: FUNÇÃO MAIN (Orquestrador)
# ==============================================================================
def main():
    """Função principal para orquestrar o processo de carga."""
    
    ARQUIVO_VINCULADO = "caminho/para/seu/resultado_vinculado.csv"
    
    print("--- Iniciando Script de Carga para o Banco de Dados ---")
    
    engine = conectar_ao_banco()
    if not engine:
        print("--- Script finalizado com erro na conexão ---")
        return
    
    try:
        print(f"Lendo dados unificados de: {ARQUIVO_VINCULADO}")
        df_unificado = pd.read_csv(ARQUIVO_VINCULADO, sep=';', decimal=',')
    except FileNotFoundError:
        print(f"ERRO: Arquivo de entrada não encontrado em: {ARQUIVO_VINCULADO}")
        print("Verifique o caminho e se o script 'vincular_produtos.py' foi executado.")
        print("--- Script finalizado com erro ---")
        return
        
    carregar_precos_para_banco(df_unificado, engine)
    
    print("--- Script finalizado com sucesso ---")


if __name__ == "__main__":
    main()