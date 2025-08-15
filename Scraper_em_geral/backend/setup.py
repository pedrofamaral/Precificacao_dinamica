# setup.py
import os
import subprocess
import sys

def install_requirements():
    """Instala as dependências necessárias"""
    requirements = [
        "flask",
        "requests",
        "python-dotenv"
    ]
    
    for req in requirements:
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", req])
            print(f"✓ {req} instalado com sucesso")
        except subprocess.CalledProcessError:
            print(f"✗ Erro ao instalar {req}")

def setup_env():
    """Configura o arquivo .env"""
    env_content = """# .env
# Substitua pelos seus valores reais do Mercado Livre
ML_CLIENT_ID=seu_client_id_aqui
ML_CLIENT_SECRET=seu_client_secret_aqui
"""
    
    if not os.path.exists(".env"):
        with open(".env", "w") as f:
            f.write(env_content)
        print("✓ Arquivo .env criado")
    else:
        print("✓ Arquivo .env já existe")

def main():
    print("🔧 Configurando ambiente para Mercado Livre API...")
    print()
    
    install_requirements()
    setup_env()
    
    print()
    print("📋 Próximos passos:")
    print("1. Edite o arquivo .env com suas credenciais do Mercado Livre")
    print("2. Para desenvolvimento local: python app.py")
    print("3. Para usar com ngrok:")
    print("   - Instale ngrok: https://ngrok.com/download")
    print("   - Execute: ngrok http 8000")
    print("   - Copie a URL https://xxx.ngrok.io")
    print("   - Atualize REDIRECT_URI no app.py")
    print("   - Configure a URL no painel do Mercado Livre")
    print()
    print("📱 Acesse: http://localhost:8000")

if __name__ == "__main__":
    main()