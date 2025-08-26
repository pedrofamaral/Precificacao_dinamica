import pandas as pd
import numpy as np
from pathlib import Path
import argparse

# --- Configurações ---
BASE_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT_FILE = BASE_DIR / "data" / "processed" / "market_items_clean.parquet"
DEFAULT_OUTPUT_PER_MARKETPLACE = BASE_DIR / "data" / "processed" / "precos_referencia_por_marketplace.csv"
DEFAULT_OUTPUT_FINAL = BASE_DIR / "data" / "processed" / "precos_referencia_final.csv"

# --- Lógica de Cálculo de Preço ---

def calculate_reference_price(prices: pd.Series) -> pd.Series:
    """
    Calcula o preço de referência para uma série de preços,
    removendo outliers com base em percentis (média aparada).
    """
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
    """Função auxiliar para aplicar o cálculo de preço a um grupo de DataFrame."""
    return calculate_reference_price(group['price'])

# --- Função Principal ---

def main(input_path: Path, output_per_marketplace_path: Path, output_final_path: Path):
    """Orquestra o processo de cálculo de preços de referência em duas etapas."""
    print("🚀 Iniciando processo de precificação...")
    
    if not input_path.exists():
        print(f"❌ ERRO: Arquivo de entrada não encontrado em '{input_path}'")
        print("➡️  Execute o script 'etl_ingest.py' primeiro.")
        return

    print(f"📄 Lendo dados de '{input_path}'...")
    try:
        df = pd.read_parquet(input_path) if input_path.suffix == '.parquet' else pd.read_csv(input_path, sep=';', decimal=',')
    except Exception as e:
        print(f"❌ Falha ao ler o arquivo de entrada: {e}")
        return
        
    print(f"✅ {len(df)} registros lidos.")
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df_valid = df.dropna(subset=['price', 'cod_prod', 'marketplace', 'title'])
    
    print("\n🏷️  Identificando o título principal de cada produto...")
    product_titles = df_valid.groupby('cod_prod')['title'].agg(lambda x: x.value_counts().index[0]).rename('product_title')

    print("\n📊 Etapa 1: Agrupando por 'cod_prod' e 'marketplace'...")
    # --- CORREÇÃO: Alterada a forma de aplicar a função para maior robustez ---
    pricing_per_marketplace = df_valid.groupby(['cod_prod', 'marketplace']).apply(calculate_stats_from_group).reset_index()
    
    pricing_per_marketplace = pricing_per_marketplace.merge(product_titles, on='cod_prod', how='left')

    price_cols = ['ref_price_mean', 'ref_price_median', 'min_price', 'max_price', 'std_dev']
    for col in price_cols:
        if col in pricing_per_marketplace.columns:
            pricing_per_marketplace[col] = pricing_per_marketplace[col].round(2)
            
    cols_order = ['cod_prod', 'product_title', 'marketplace'] + [c for c in pricing_per_marketplace.columns if c not in ['cod_prod', 'product_title', 'marketplace']]
    pricing_per_marketplace = pricing_per_marketplace[cols_order]

    print(f"✅ Cálculo por marketplace finalizado: {len(pricing_per_marketplace)} registros.")
    output_per_marketplace_path.parent.mkdir(parents=True, exist_ok=True)
    pricing_per_marketplace.to_csv(output_per_marketplace_path, index=False, sep=';', decimal=',')
    print(f"💾 Resultados salvos em '{output_per_marketplace_path}'")
    print("\n📋 Amostra (por marketplace):\n", pricing_per_marketplace.head().to_string())

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
    
    cols_order_final = ['cod_prod', 'product_title'] + [c for c in final_pricing.columns if c not in ['cod_prod', 'product_title']]
    final_pricing = final_pricing[cols_order_final]

    print(f"✅ Cálculo finalizado: {len(final_pricing)} produtos.")
    output_final_path.parent.mkdir(parents=True, exist_ok=True)
    final_pricing.to_csv(output_final_path, index=False, sep=';', decimal=',')
    print(f"💾 Resultados finais salvos em '{output_final_path}'")
    print("\n📋 Amostra (final):\n", final_pricing.head().to_string())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calcula preços de referência a partir de dados de mercado limpos.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_FILE)
    parser.add_argument("--output_market", type=Path, default=DEFAULT_OUTPUT_PER_MARKETPLACE)
    parser.add_argument("--output_final", type=Path, default=DEFAULT_OUTPUT_FINAL)
    args = parser.parse_args()
    
    main(args.input, args.output_market, args.output_final)
