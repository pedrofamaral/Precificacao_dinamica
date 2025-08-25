import pandas as pd
import sqlite3
import unicodedata
import re
import argparse

def carregar_produtos_internos(caminho_json: str) -> pd.DataFrame:
    print(f"Carregando produtos internos de: {caminho_json}")
    df_internos = pd.read_json(caminho_json)
    print(f"-> Encontrados {len(df_internos)} produtos internos.")
    return df_internos

def carregar_dados_de_mercado(caminho_db: str) -> pd.DataFrame:
    print(f"Carregando dados de mercado de: {caminho_db}")
    try:
        con = sqlite3.connect(f"file:{caminho_db}?mode=ro", uri=True)
        df_mercado = pd.read_sql("SELECT * FROM unifier_input", con)
        con.close()
        print(f"-> Encontrados {len(df_mercado)} produtos de mercado na tabela 'unifier_input'.")
        return df_mercado
    except Exception as e:
        print(f"ERRO ao ler o banco de dados do ETL: {e}")
        print("Verifique se o caminho está correto e se o script etl_ingest.py foi executado com sucesso.")
        return pd.DataFrame()
    
def _normalizar_texto(texto: str) -> str:
    """Função de apoio para limpeza e padronização de textos para a chave."""
    if texto is None:
        return ""
    texto = str(texto).lower().strip()
    texto = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    texto = re.sub(r'\s+', ' ', texto) 
    return texto

def vincular_produtos(df_internos: pd.DataFrame, df_mercado: pd.DataFrame) -> pd.DataFrame:
    print("Iniciando processo de vinculação...")

    df_internos['marca_norm'] = df_internos['Marca'].apply(_normalizar_texto)
    df_internos['modelo_norm'] = df_internos['Modelo'].apply(_normalizar_texto)
    df_internos['medida_norm'] = df_internos['MedidaNorm'].apply(_normalizar_texto)
    df_internos['chave_vinculacao'] = (
        df_internos['marca_norm'] + '|' + 
        df_internos['modelo_norm'] + '|' + 
        df_internos['medida_norm']
    )

    df_mercado['marca_norm'] = df_mercado['brand'].apply(_normalizar_texto)
    df_mercado['modelo_norm'] = df_mercado['model'].apply(_normalizar_texto)
    df_mercado['medida_norm'] = df_mercado['size_norm'].apply(_normalizar_texto)
    df_mercado['chave_vinculacao'] = (
        df_mercado['marca_norm'] + '|' + 
        df_mercado['modelo_norm'] + '|' + 
        df_mercado['medida_norm']
    )

    print("Executando o merge dos dataframes com base na chave de vinculação...")
    df_final = pd.merge(
        left=df_internos,
        right=df_mercado,
        on='chave_vinculacao',
        how='left'  
    )

    df_final.rename(columns={
        'price': 'preco_mercado',
        'url': 'url_mercado',
        'seller': 'vendedor_mercado',
        'marketplace': 'marketplace_mercado'
    }, inplace=True)

    produtos_encontrados = df_final['preco_mercado'].notna().sum()
    total_internos = len(df_internos)
    percentual = (produtos_encontrados / total_internos) * 100 if total_internos > 0 else 0
    
    print(f"\n--- Resultado da Vinculação ---")
    print(f"Produtos internos: {total_internos}")
    print(f"Produtos com correspondência no mercado: {produtos_encontrados} ({percentual:.2f}%)")
    print(f"Produtos sem correspondência: {total_internos - produtos_encontrados}")
    print("---------------------------------")
    
    return df_final

def main():
    ARQUIVO_PRODUTOS_INTERNOS = "Precificação_AI/Extracao_e_Insercao/data/query_products.json"
    BANCO_DE_DADOS_ETL = "Precificação_AI/ETL/data/processed/pricing.db"
    ARQUIVO_SAIDA = "Precificação_AI/data/resultado_vinculado.csv"

    df_internos = carregar_produtos_internos(ARQUIVO_PRODUTOS_INTERNOS)
    df_mercado = carregar_dados_de_mercado(BANCO_DE_DADOS_ETL)

    if df_internos.empty or df_mercado.empty:
        print("Um dos dataframes está vazio. Encerrando o processo.")
        return

    df_final = vincular_produtos(df_internos, df_mercado)
    
    colunas_finais = [
        'COD_PROD', 'Marca', 'Modelo', 'MedidaNorm', 'PRECO_BASE', 'ESTOQUE',
        'preco_mercado', 'url_mercado', 'vendedor_mercado', 'marketplace_mercado', 'captured_at',
        'chave_vinculacao'
    ]
    colunas_existentes = [col for col in colunas_finais if col in df_final.columns]
    
    df_final[colunas_existentes].to_csv(ARQUIVO_SAIDA, index=False, sep=';', decimal=',')
    
    print(f"\nProcesso concluído! Resultado salvo em: {ARQUIVO_SAIDA}")

if __name__ == "__main__":
    main()