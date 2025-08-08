# Precificação Dinâmica 📈

> MVP end-to-end de **scraping → ETL → banco SQLite → ML → API FastAPI**  
> Otimize preços em tempo real a partir de dados de mercado, custos internos e regras comerciais.

![Python](https://img.shields.io/badge/Python-3.11-blue.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.111.0-009688?logo=fastapi&logoColor=white)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)
<!-- Quando configurar CI, troque o link abaixo pelo badge real -->
![Build](https://img.shields.io/badge/build-passing-brightgreen.svg)

---

## Índice <!-- omit in toc -->
- [✨ Principais funcionalidades](#✨-principais-funcionalidades)
- [🗂️ Estrutura do projeto](#🗂️-estrutura-do-projeto)
- [⚙️ Instalação rápida](#⚙️-instalação-rápida)
- [🚀 Guia de uso](#🚀-guia-de-uso)
- [🔍 Modelagem & métricas](#🔍-modelagem--métricas)
- [📡 Deploy & escalabilidade](#📡-deploy--escalabilidade)
- [🗺️ Roadmap](#🗺️-roadmap)
- [🤝 Contribuindo](#🤝-contribuindo)
- [📄 Licença](#📄-licença)
- [🙋 Autor](#🙋-autor)

---

## ✨ Principais funcionalidades
| Módulo | Descrição |
| ------ | --------- |
| **PriceMonitor** | Scraper (Selenium/Playwright) que coleta preços no Mercado Livre; HTML bruto salvo em `data/raw/`. |
| **pricing_mvp** | Pipeline ETL → features → modelos → **API**; inclui scripts de agregação e merge de bases internas. |
| **src/pricing_ai** | Pacote reutilizável com loaders, transformações e métricas customizadas. |
| **API FastAPI** | `POST /suggest-price` retorna preço ótimo, demanda esperada, elasticidade e *confidence interval*. |
| **Discord bot** (opcional) | Envia alertas de pricing em tempo real para um canal do Discord. |

---

## 🗂️ Estrutura do projeto
```text
Precificacao_dinamica/
├── PriceMonitor/          # Scraper & parsers
├── pricing_mvp/           # Pipeline e API
│   ├── load_market_JSON.py
│   ├── build_aggregates.py
│   ├── merge_features.py
│   ├── train_model.py
│   └── api.py
├── scripts/               # Atalhos de ETL
├── src/pricing_ai/        # Pacote reutilizável
├── data/
│   ├── raw/               # HTML + JSON bruto do scraping
│   └── processed/         # CSV/SQLite pós-ETL
├── requirements.txt
└── README.md              # Este arquivo

⚙️ Instalação rápida
# 1. Clone
git clone https://github.com/pedrofamaral/Precificacao_dinamica.git
cd Precificacao_dinamica

# 2. Ambiente virtual
python -m venv .venv
source .venv/bin/activate        # Linux/macOS
# .venv\Scripts\Activate.ps1     # Windows

# 3. Dependências
pip install -r requirements.txt

🚀 Guia de uso
python PriceMonitor/scraper2.0.py \
  --termo "pneu 175 70 r13 goodyear kelly edge" \
  --max 70 --modo click --dump-html --debug --detalhes
2️⃣ ETL + features
bash
Copiar
Editar
cd pricing_mvp
python load_market_JSON.py ../data/raw
python build_aggregates.py
python merge_features.py
python load_internal_data.py      # importa custos internos
# ou simplesmente:
scripts/etl                       # atalho completo
3️⃣ Treino do modelo
bash
Copiar
Editar
python train_model.py             # gera modelo.pkl e métricas
4️⃣ API local
bash
Copiar
Editar
uvicorn api:app --reload --port 8000
Exemplo de chamada
bash
Copiar
Editar
curl -X POST http://localhost:8000/suggest-price \
     -H "Content-Type: application/json" \
     -d '{
           "sku": "ABC123",
           "cost": 98.50,
           "competitors": [
             {"seller": "LojaX", "price": 119.90},
             {"seller": "LojaY", "price": 115.40}
           ],
           "stock": 42
         }'
Resposta

json
Copiar
Editar
{
  "recommended_price": 124.90,
  "expected_demand": 37,
  "elasticity": -1.23,
  "confidence": 0.82
}

