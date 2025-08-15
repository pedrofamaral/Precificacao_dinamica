import os
import sqlite3
import pandas as pd
from dotenv import load_dotenv
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error
import joblib
import numpy as np
from tensorflow import keras
from sklearn.preprocessing import StandardScaler
from xgboost import XGBRegressor

def load_data(db_path):
    conn = sqlite3.connect(db_path)
    df_int = pd.read_sql("SELECT sku_key, cost_price, sale_price, stock FROM internal_data", conn)
    df_comp = pd.read_sql("""
    SELECT sku_key, date, comp_p10, comp_p50, comp_p90, comp_min, comp_max
    FROM aggregates_daily
    """, conn)
    conn.close()
    df_comp_latest = df_comp.sort_values('date').drop_duplicates('sku_key', keep='last')
    df = pd.merge(df_int, df_comp_latest, on='sku_key', how='inner')
    return df

def preprocess(df):
    # Filtragem de outliers
    #for col in ['sale_price', 'cost_price', 'comp_p10', 'comp_p50', 'comp_p90', 'comp_min', 'comp_max']:
    #    q1 = df[col].quantile(0.01)
    #    q99 = df[col].quantile(0.99)
    #    df = df[(df[col] >= q1) & (df[col] <= q99)]
    # Imputação simples de NaN
    df.fillna(df.median(numeric_only=True), inplace=True)
    return df

def train_xgboost(X_train, y_train):
    model = XGBRegressor(n_estimators=300, random_state=42, learning_rate=0.1)
    model.fit(X_train, y_train)
    return model

def evaluate(y_true, y_pred):
    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    print(f"MAE: {mae:.2f} | RMSE: {rmse:.2f}")
    return mae, rmse

def train_keras(X_train, y_train, X_val, y_val, epochs=100, batch_size=8):
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_val_scaled = scaler.transform(X_val)

    model = keras.Sequential([
        keras.layers.Input(shape=(X_train.shape[1],)),
        keras.layers.Dense(64, activation='relu'),
        keras.layers.Dense(32, activation='relu'),
        keras.layers.Dense(1)
    ])

    model.compile(optimizer='adam', loss='mse', metrics=[keras.metrics.RootMeanSquaredError()])
    model.fit(X_train_scaled, y_train, validation_data=(X_val_scaled, y_val), epochs=epochs, batch_size=batch_size, verbose=1)

    val_loss, val_rmse = model.evaluate(X_val_scaled, y_val, verbose=0)
    print(f"Keras Val RMSE: {val_rmse:.2f}")
    model.save('models/price_suggester_keras.h5')
    joblib.dump(scaler, 'models/price_suggester_scaler.pkl')
    print("Modelo Keras e scaler salvos!")
    return model, scaler

def show_db_summary(db_path):
    import sqlite3
    import pandas as pd

    conn = sqlite3.connect(db_path)
    df_int = pd.read_sql("SELECT * FROM internal_data", conn)
    df_comp = pd.read_sql("SELECT * FROM aggregates_daily", conn)
    conn.close()

    print("\n===== RESUMO DO BANCO DE DADOS =====")
    print(f"internal_data: {df_int.shape[0]} registros, {df_int['sku_key'].nunique()} SKUs únicos")
    print(df_int.groupby('sku_key').size().rename('Exemplos por SKU'))
    if 'date' in df_int.columns:
        print("Intervalo de datas:", df_int['date'].min(), "a", df_int['date'].max())
    print("\naggregates_daily: %d registros" % df_comp.shape[0])
    print(df_comp.groupby('sku_key').size().rename('Exemplos por SKU'))
    if 'date' in df_comp.columns:
        print("Intervalo de datas (aggregates):", df_comp['date'].min(), "a", df_comp['date'].max())
    print("=====================================\n")


def main():
    load_dotenv()
    db_path = os.getenv('DB_PATH', './data/processed/pricing.db')
    print(f"Usando banco: {db_path}")
    df = load_data(db_path)
    print(f"Dados carregados: {df.shape[0]} registros")
    show_db_summary(db_path)
    df = preprocess(df)
    print(f"Dados após preprocessamento: {df.shape[0]} registros")

    features = ['cost_price', 'stock', 'comp_p10', 'comp_p50', 'comp_p90', 'comp_min', 'comp_max']
    X = df[features]
    y = df['sale_price']

    if len(df) < 3:
        print("\n⚠️ ATENÇÃO: Muito poucos dados após preprocessamento! Treinando o modelo com todos os registros (sem validação/teste split).")
        X_train = X
        y_train = y
        X_val = X
        y_val = y
    else:
        X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

    print("\nQual modelo deseja treinar?")
    print("1 - XGBoost")
    print("2 - Keras")
    print("3 - Ambos")
    escolha = input("Digite o número do modelo (1/2/3): ").strip()

    if escolha == "1":
        print("\nTreinando modelo XGBoost...")
        model_xgb = train_xgboost(X_train, y_train)
        preds_xgb = model_xgb.predict(X_val)
        print("Avaliação XGBoost:")
        evaluate(y_val, preds_xgb)
        os.makedirs('models', exist_ok=True)
        joblib.dump(model_xgb, "models/price_suggester_xgb.pkl")
        print("Modelo XGBoost salvo em models/price_suggester_xgb.pkl")
    elif escolha == "2":
        print("\nTreinando modelo Keras...")
        model_keras, scaler = train_keras(X_train, y_train, X_val, y_val)
    elif escolha == "3":
        print("\nTreinando modelo XGBoost...")
        model_xgb = train_xgboost(X_train, y_train)
        preds_xgb = model_xgb.predict(X_val)
        print("Avaliação XGBoost:")
        evaluate(y_val, preds_xgb)
        os.makedirs('models', exist_ok=True)
        joblib.dump(model_xgb, "models/price_suggester_xgb.pkl")
        print("Modelo XGBoost salvo em models/price_suggester_xgb.pkl")

        print("\nTreinando modelo Keras...")
        model_keras, scaler = train_keras(X_train, y_train, X_val, y_val)
    else:
        print("Opção inválida. Saindo.")

if __name__ == "__main__":
    main()



#Integração dinamica com LLMs RAG/ formar endpoint para sugerir preços
#Possibilidade de usar o modelo treinado para sugerir preços em tempo real via API
## Sugestão de melhorias:
# - Implementar validação cruzada para melhor avaliação do modelo
# - Adicionar logging para monitorar o processo de treinamento
# - Fazer interface com o FastAPI para expor o modelo como um endpoint
# - Considerar usar GridSearchCV para otimizar hiperparâmetros do XGBoost
# - Integrar com o Discord Bot para sugestões de preços via chat