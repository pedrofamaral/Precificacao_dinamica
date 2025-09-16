import pandas as pd
import numpy as np
import argparse
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from ETL.common import SETTINGS, get_conn

# --- Configurações ---
BASE_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT_FILE = BASE_DIR / "data" / "processed" / "market_items_clean.parquet"
DEFAULT_DB_PATH = SETTINGS.db_url
DEFAULT_OUTPUT_PER_MARKETPLACE = BASE_DIR / "data" / "processed" / "precos_referencia_por_marketplace.csv"
DEFAULT_OUTPUT_FINAL = BASE_DIR / "data" / "processed" / "precos_referencia_final.csv"

def calculate_reference_price(prices: pd.Series) -> pd.Series:
    prices = prices.dropna()
    count_original = len(prices)
    
    if count_original == 0:
        return pd.Series({
            'ref_price_mean': np.nan, 'ref_price_median': np.nan,
            'competitors_count_original': 0, 'competitors_count_final': 0,
            'min_price': np.nan, 'max_price': np.nan, 'std_dev': np.nan
        })
    
    if count_original <= 3:
        return pd.Series({
            'ref_price_mean': prices.mean(), 'ref_price_median': prices.median(),
            'competitors_count_original': count_original, 'competitors_count_final': count_original,
            'min_price': prices.min(), 'max_price': prices.max(), 'std_dev': prices.std()
        })

    p10 = prices.quantile(0.10)
    p90 = prices.quantile(0.90)
    
    prices_filtered = prices[(prices >= p10) & (prices <= p90)]
    count_final = len(prices_filtered)
    
    if count_final == 0:
        prices_filtered = prices
        count_final = count_original

    return pd.Series({
        'ref_price_mean': prices_filtered.mean(), 'ref_price_median': prices_filtered.median(),
        'competitors_count_original': count_original, 'competitors_count_final': count_final,
        'min_price': prices.min(), 'max_price': prices.max(), 'std_dev': prices_filtered.std()
    })

def calculate_stats_from_group(group):
    return calculate_reference_price(group['price'])

def title_normalization(row):
    try:
        medida = f"{int(row['width'])} {int(row['aspect'])}R{int(row['rim'])}"
        marca = str(row['brand']).strip().title()
        modelo = str(row['model']).strip().title()

        return f"P{medida} {marca} {modelo}"
    except(ValueError, TypeError):
        return row['title']

def main(input_path: Path, output_per_marketplace_path: Path, output_final_path: Path):
    print("🚀 Iniciando processo de precificação...")
    
    if not input_path.exists():
        print(f"❌ ERRO: Arquivo de entrada não encontrado em '{input_path}'")
        print("➡️  Execute o script de ingestão/limpeza primeiro.")
        return

    print(f"📄 Lendo dados de '{input_path}'...")
    try:
        df = pd.read_parquet(input_path)
    except Exception as e:
        print(f"❌ Falha ao ler o arquivo de entrada: {e}")
        return
        
    print(f"✅ {len(df)} registros lidos.")
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    
    required_cols = ['price', 'cod_prod', 'marketplace', 'title', 'width', 'aspect', 'rim', 'brand', 'model']
    df_valid = df.dropna(subset=required_cols)
    
    print("\n Normalizando o título de cada produto...")
    df_valid['titulo_produto'] = df_valid.apply(title_normalization, axis=1)
    
    product_titles = df_valid.groupby('cod_prod')['titulo_produto'].first()

    print("\n📊 Etapa 1: Agrupando por 'cod_prod' e 'marketplace'...")
    pricing_per_marketplace = df_valid.groupby(['cod_prod', 'marketplace']).apply(calculate_stats_from_group).reset_index()
    pricing_per_marketplace = pricing_per_marketplace.merge(product_titles, on='cod_prod', how='left')

    price_cols = ['ref_price_mean', 'ref_price_median', 'min_price', 'max_price', 'std_dev']
    for col in price_cols:
        if col in pricing_per_marketplace.columns:
            pricing_per_marketplace[col] = pricing_per_marketplace[col].round(2)
            
    cols_order = ['cod_prod', 'titulo_produto', 'marketplace'] + [c for c in pricing_per_marketplace.columns if c not in ['cod_prod', 'titulo_produto', 'marketplace']]
    pricing_per_marketplace = pricing_per_marketplace[cols_order]

    print(f"✅ Cálculo por marketplace finalizado: {len(pricing_per_marketplace)} registros.")
    
    mapa_nomes_marketplace = {
        'cod_prod': 'cod_produto',
        'titulo_produto': 'titulo_produto',
        'marketplace': 'marketplace',
        'ref_price_mean': 'preco_ref_medio',
        'ref_price_median': 'preco_ref_mediana',
        'competitors_count_original': 'n_concorrentes_original',
        'competitors_count_final': 'n_concorrentes_final',
        'min_price': 'preco_minimo',
        'max_price': 'preco_maximo',
        'std_dev': 'desvio_padrao'
    }
    pricing_per_marketplace_pt = pricing_per_marketplace.rename(columns=mapa_nomes_marketplace)
    
    output_per_marketplace_path.parent.mkdir(parents=True, exist_ok=True)
    pricing_per_marketplace_pt.to_csv(output_per_marketplace_path, index=False, sep=';', decimal=',')
    print(f"💾 Resultados salvos em '{output_per_marketplace_path}'")
    
    print("🏦 Salvando resultados por marketplace no banco de dados...")
    try:
        with get_conn() as conn:
            pricing_per_marketplace_pt.to_sql(
                'precos_por_marketplace', con=conn, if_exists='replace', index=False
            )
        print("✅ Salvo com sucesso na tabela 'precos_por_marketplace'.")
    except Exception as e:
        print(f"❌ Falha ao salvar no banco de dados: {e}")
    print("\n📋 Amostra (por marketplace):\n", pricing_per_marketplace_pt.head().to_string())
    
    if pricing_per_marketplace.empty:
        print("\n ALERTA: Nenhum dado válido para a Etapa 2. O cálculo final não será executado.")
        return

    print("\n📊 Etapa 2: Agrupando resultados por 'cod_prod' para o preço final...")
    
    final_pricing = pricing_per_marketplace.groupby('cod_prod').agg(
        final_ref_price_mean=('ref_price_mean', 'mean'),
        total_competitors=('competitors_count_original', 'sum'),
        num_marketplaces=('marketplace', 'nunique')
    ).reset_index()

    final_pricing = final_pricing.merge(product_titles, on='cod_prod', how='left')
    final_pricing['final_ref_price_mean'] = final_pricing['final_ref_price_mean'].round(2)
    
    cols_order_final = ['cod_prod', 'titulo_produto'] + [c for c in final_pricing.columns if c not in ['cod_prod', 'titulo_produto']]
    final_pricing = final_pricing[cols_order_final]
    
    mapa_nomes_final = {
        'cod_prod': 'cod_produto',
        'titulo_produto': 'titulo_produto',
        'final_ref_price_mean': 'preco_ref_final_medio',
        'total_competitors': 'total_concorrentes',
        'num_marketplaces': 'n_marketplaces'
    }
    final_pricing_pt = final_pricing.rename(columns=mapa_nomes_final)

    print(f"✅ Cálculo finalizado: {len(final_pricing_pt)} produtos.")
    output_final_path.parent.mkdir(parents=True, exist_ok=True)
    final_pricing_pt.to_csv(output_final_path, index=False, sep=';', decimal=',')
    print(f"💾 Resultados finais salvos em '{output_final_path}'")
    
    print("🏦 Salvando resultados finais no banco de dados...")
    try:
        with get_conn() as conn:
            final_pricing_pt.to_sql(
                'produtos_preco_referencia_final', con=conn, if_exists='replace', index=False
            )
        print("✅ Salvo com sucesso na tabela 'produtos_preco_referencia_final'.")
    except Exception as e:
        print(f"❌ Falha ao salvar no banco de dados: {e}")

    print("\n📋 Amostra (final):\n", final_pricing_pt.head().to_string())



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calcula preços de referência a partir de dados de mercado limpos.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_FILE)
    parser.add_argument("--output_market", type=Path, default=DEFAULT_OUTPUT_PER_MARKETPLACE)
    parser.add_argument("--output_final", type=Path, default=DEFAULT_OUTPUT_FINAL)
    args = parser.parse_args()
    
    main(args.input, args.output_market, args.output_final)
