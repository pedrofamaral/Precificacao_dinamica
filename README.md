# Precificação — MVP (SQLite + ETL + Agregados + API de Sugestão)

## 1) Preparar ambiente
```bash
python -m venv .venv
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

## 2) Rodar o scraping
'''bash
cd PriceMonitor mercadolivre =>python scraper2.0.py --termo "pneu 175 70 r13 goodyear kelly edge" --max 70 --modo click --dump-html --debug --detalhes
'''
##Tratar os dados
'''bash
cd pricing_mvp
python .\load_market_JSON.py data/raw
python .\build_aggregates.py
python .\merge_features.py
python .\load_internal_data.py

ou

scripts/etl
'''

##Treinar o modelo e abrir endpoint

python train_model.py
python api.py