from fastapi import FastAPI, Query
from pydantic import BaseModel
import pandas as pd
import joblib
import numpy as np
from tensorflow import keras
import sqlite3
import datetime

# =========== MODELOS ML ===========
XGB_MODEL_PATH = "models/price_suggester_xgb.pkl"
KERAS_MODEL_PATH = "models/price_suggester_keras.h5"
KERAS_SCALER_PATH = "models/price_suggester_scaler.pkl"

try:
    xgb_model = joblib.load(XGB_MODEL_PATH)
except:
    xgb_model = None
try:
    keras_model = keras.models.load_model(KERAS_MODEL_PATH)
    keras_scaler = joblib.load(KERAS_SCALER_PATH)
except:
    keras_model = None
    keras_scaler = None

FEATURES = ['cost_price', 'stock', 'comp_p10', 'comp_p50', 'comp_p90', 'comp_min', 'comp_max']

# =========== CARREGAMENTO DO CSV ===========
FEATURES_CSV = "data/processed/Features_locais.csv"
try:
    df_features = pd.read_csv(FEATURES_CSV)
except Exception as e:
    df_features = None
    print(f"Erro ao carregar {FEATURES_CSV}: {e}")

# =========== FASTAPI ===========
app = FastAPI(title="API de Precificação Dinâmica", description="Sugestão de preço baseada em regras e ML com dados internos e de concorrentes.")

# --------- ENDPOINT: Listar SKUs disponíveis ----------
@app.get("/list_skus")
def list_skus():
    if df_features is None:
        return {"error": "Features_locais.csv não carregado"}
    return {"skus": df_features['sku_key'].unique().tolist()}

# --------- ENDPOINT: Sugestão de preço por SKU com CSV ----------
class PredictInput(BaseModel):
    sku_key: str
    model: str = "xgboost"  # ou keras

@app.post("/suggest_price_from_csv")
def suggest_price_from_csv(data: PredictInput):
    if df_features is None:
        return {"error": "Arquivo Features_locais.csv não carregado"}
    linha = df_features[df_features['sku_key'] == data.sku_key]
    if linha.empty:
        return {"error": f"SKU '{data.sku_key}' não encontrado no CSV"}

    features = linha[FEATURES].values
    entrada = {col: float(linha[col].iloc[0]) for col in FEATURES}

    if data.model.lower() == "xgboost":
        if xgb_model is None:
            return {"error": "Modelo XGBoost não carregado."}
        price = xgb_model.predict(features)[0]
        return {
            "sku_key": data.sku_key,
            "suggested_price": round(float(price), 2),
            "model": "xgboost",
            "input_features": entrada
        }
    elif data.model.lower() == "keras":
        if keras_model is None or keras_scaler is None:
            return {"error": "Modelo Keras não carregado."}
        features_scaled = keras_scaler.transform(features)
        price = keras_model.predict(features_scaled)[0][0]
        return {
            "sku_key": data.sku_key,
            "suggested_price": round(float(price), 2),
            "model": "keras",
            "input_features": entrada
        }
    else:
        return {"error": "Modelo não reconhecido. Use 'xgboost' ou 'keras'."}

# --------- ENDPOINT: Sugestão de preço para TODOS os SKUs ----------
@app.get("/suggest_all_prices")
def suggest_all_prices(model: str = Query("xgboost", enum=["xgboost", "keras"])):
    if df_features is None:
        return {"error": "Arquivo Features_locais.csv não carregado"}

    resultados = []
    for _, linha in df_features.iterrows():
        features = linha[FEATURES].values.reshape(1, -1)
        entrada = {col: float(linha[col]) for col in FEATURES}
        sku = linha['sku_key']

        if model == "xgboost" and xgb_model is not None:
            price = xgb_model.predict(features)[0]
        elif model == "keras" and keras_model is not None and keras_scaler is not None:
            features_scaled = keras_scaler.transform(features)
            price = keras_model.predict(features_scaled)[0][0]
        else:
            price = None

        resultados.append({
            "sku_key": sku,
            "suggested_price": round(float(price), 2) if price is not None else None,
            "model": model,
            "input_features": entrada
        })
    return {"results": resultados}

# --------- ENDPOINT: Sugestão de preço por REGRAS ----------
class RuleInput(BaseModel):
    sku_key: str
    cost_price: float
    sale_price: float
    comp_p50: float
    comp_max: float = None
    min_margin: float = 0.35
    map_price: float = None
    comp_p10: float = None
    comp_min: float = None

@app.post("/suggest_price")
def suggest_price_rule(data: RuleInput):
    custo = data.cost_price
    mediana = data.comp_p50
    min_margin = data.min_margin if data.min_margin else 0.12
    map_price = data.map_price
    p10 = data.comp_p10 if data.comp_p10 is not None else mediana
    comp_min = data.comp_min if data.comp_min is not None else mediana
    comp_max = data.comp_max if data.comp_max is not None else None

    min_price_rule = custo * (1 + min_margin)
    sugestao = mediana

    if sugestao < min_price_rule:
        sugestao = min_price_rule
    if map_price and sugestao < map_price:
        sugestao = map_price
    if sugestao < p10:
        sugestao = p10
    if sugestao < comp_min:
        sugestao = comp_min
    if comp_max is not None and sugestao > comp_max:
        sugestao = comp_max

    rationale = f"Sugestão baseada na mediana do mercado ({mediana:.2f}), respeitando margem mínima ({min_margin:.0%}), MAP ({map_price}), e não excedendo o preço máximo do mercado ({comp_max})."
    evidence = {
        "cost_price": custo,
        "comp_p50": mediana,
        "min_margin": min_margin,
        "map_price": map_price,
        "comp_p10": p10,
        "comp_min": comp_min,
        "comp_max": comp_max
    }

    return {
        "suggested_price": round(float(sugestao), 2),
        "rationale": rationale,
        "evidence": evidence
    }

# --------- ENDPOINT: Sugestão de preço por ML manual ----------
class MLPriceInput(BaseModel):
    cost_price: float
    stock: int
    comp_p10: float
    comp_p50: float
    comp_p90: float
    comp_min: float
    comp_max: float
    model: str = "xgboost"

@app.post("/suggest_price_ml")
def suggest_price_ml(data: MLPriceInput):
    features = np.array([[getattr(data, f) for f in FEATURES]])

    if data.model.lower() == "xgboost":
        if xgb_model is None:
            return {"error": "Modelo XGBoost não carregado."}
        price = xgb_model.predict(features)[0]
        return {"suggested_price": float(price), "model": "xgboost"}
    elif data.model.lower() == "keras":
        if keras_model is None or keras_scaler is None:
            return {"error": "Modelo Keras não carregado."}
        features_scaled = keras_scaler.transform(features)
        price = keras_model.predict(features_scaled)[0][0]
        return {"suggested_price": float(price), "model": "keras"}
    else:
        return {"error": "Modelo não reconhecido. Use 'xgboost' ou 'keras'."}

