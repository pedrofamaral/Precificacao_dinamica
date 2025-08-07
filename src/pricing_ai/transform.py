import pandas as pd

def transform_basic(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if 'preco' in df.columns:
        df['preco'] = (
            df['preco']
            .astype(str)
            .str.replace(r'[R$\.\s]', '', regex=True)
            .str.replace(',', '.', regex=False)
        )
        df['preco'] = pd.to_numeric(df['preco'], errors='coerce')
        df['preco'] = df['preco'].fillna(df['preco'].median())

    if 'estoque' in df.columns:
        df['estoque'] = (
            df['estoque']
            .astype(str)
            .str.extract(r'(\d+)')
            .fillna(0)
            .astype(int)
        )

    if 'data_coleta' in df.columns:
        df['data_coleta'] = pd.to_datetime(df['data_coleta'], errors='coerce')
        df['dia_semana'] = df['data_coleta'].dt.weekday
        df['fim_de_semana'] = df['dia_semana'].isin([5, 6]).astype(int)

    if 'preco' in df.columns:
        low, high = df['preco'].quantile([0.01, 0.99])
        df['preco'] = df['preco'].clip(low, high)

        if 'product_id' in df.columns and 'data_coleta' in df.columns:
            df = df.sort_values(['product_id', 'data_coleta'])
            df['preco_diff'] = df.groupby('product_id')['preco'].diff()
            df['preco_ma7'] = df.groupby('product_id')['preco'].transform(lambda x: x.rolling(7, min_periods=1).mean())
            df['preco_ma30'] = df.groupby('product_id')['preco'].transform(lambda x: x.rolling(30, min_periods=1).mean())
            df['anomaly'] = (df['preco_diff'].abs() > (df['preco'] * 0.3)).astype(int)
            
    return df
