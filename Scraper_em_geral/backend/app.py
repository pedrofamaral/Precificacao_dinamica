# app.py
import os
from flask import Flask, request, redirect, session, jsonify
import requests
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.secret_key = os.urandom(24)

CLIENT_ID     = os.getenv("ML_CLIENT_ID")
CLIENT_SECRET = os.getenv("ML_CLIENT_SECRET")
REDIRECT_URI  = os.getenv("ML_REDIRECT_URI")


@app.route("/")
def index():
    return """
    <h1>Mercado Livre API - OAuth</h1>
    <p><a href="/login">Fazer login no Mercado Livre</a></p>
    <p>Depois de autorizar, use: <code>/search?q=TERMO_BUSCA</code></p>
    """

@app.route("/login")
def login():
    auth_url = (
        "https://auth.mercadolivre.com.br/authorization?"
        f"response_type=code&client_id={CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}"
    )
    return redirect(auth_url)

@app.route("/oauth/mercadolivre/callback")
def ml_callback():
    code = request.args.get("code")
    if not code:
        return "Erro: Código de autorização não recebido", 400
    
    try:
        token_resp = requests.post(
            "https://api.mercadolibre.com/oauth/token",
            data={
                "grant_type": "authorization_code",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "code": code,
                "redirect_uri": REDIRECT_URI,
            },
            timeout=10
        )
        
        token_data = token_resp.json()

        if "access_token" in token_data:
            session["ml_access_token"] = token_data.get("access_token")
            session["ml_refresh_token"] = token_data.get("refresh_token")
            return """
            <h1>Autorizado com sucesso!</h1>
            <p>Agora você pode fazer buscas:</p>
            <ul>
                <li><a href="/search?q=smartphone">Buscar smartphones</a></li>
                <li><a href="/search?q=notebook">Buscar notebooks</a></li>
                <li><a href="/user_info">Ver informações do usuário</a></li>
            </ul>
            """
        else:
            return f"Erro na autorização: {token_data}", 400
            
    except requests.RequestException as e:
        return f"Erro na requisição: {str(e)}", 500

@app.route("/search")
def market_search():
    if "ml_access_token" not in session:
        return "Erro: Você precisa fazer login primeiro. <a href='/login'>Fazer login</a>", 401
    
    term = request.args.get("q", "")
    if not term:
        return "Erro: Parâmetro 'q' é obrigatório", 400
    
    try:
        url = f"https://api.mercadolibre.com/sites/MLB/search?q={term}"
        resp = requests.get(url, timeout=10)
        
        if resp.status_code == 200:
            data = resp.json()
            # Filtrar apenas os dados mais relevantes
            results = []
            for item in data.get("results", [])[:10]:  # Limitar a 10 resultados
                results.append({
                    "id": item.get("id"),
                    "title": item.get("title"),
                    "price": item.get("price"),
                    "currency_id": item.get("currency_id"),
                    "permalink": item.get("permalink"),
                    "thumbnail": item.get("thumbnail"),
                    "condition": item.get("condition"),
                    "seller": item.get("seller", {}).get("nickname")
                })
            
            return jsonify({
                "query": term,
                "results_count": len(results),
                "results": results
            })
        else:
            return f"Erro na busca: {resp.status_code}", resp.status_code
            
    except requests.RequestException as e:
        return f"Erro na requisição: {str(e)}", 500

@app.route("/user_info")
def user_info():
    if "ml_access_token" not in session:
        return "Erro: Você precisa fazer login primeiro. <a href='/login'>Fazer login</a>", 401
    
    try:
        headers = {"Authorization": f"Bearer {session['ml_access_token']}"}
        resp = requests.get("https://api.mercadolibre.com/users/me", headers=headers, timeout=10)
        
        if resp.status_code == 200:
            return jsonify(resp.json())
        else:
            return f"Erro ao obter informações do usuário: {resp.status_code}", resp.status_code
            
    except requests.RequestException as e:
        return f"Erro na requisição: {str(e)}", 500

@app.route("/logout")
def logout():
    session.clear()
    return "Logout realizado com sucesso! <a href='/'>Voltar ao início</a>"

if __name__ == "__main__":
    if not CLIENT_ID or not CLIENT_SECRET or not REDIRECT_URI:
        print("ERRO: Defina as variáveis ML_CLIENT_ID e ML_CLIENT_SECRET e ML_REDIRECT_URI no arquivo .env")
        exit(1)
    
    print("Servidor rodando em http://localhost:8000")
    app.run(host="0.0.0.0", port=8000, debug=True)