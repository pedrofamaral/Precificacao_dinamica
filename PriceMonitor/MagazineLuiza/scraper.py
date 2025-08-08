import os
import re
import time
import json
import random
import logging
import unicodedata
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Any
from urllib.parse import urljoin, urlparse
import argparse
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
import sqlite3

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import (
    TimeoutException, 
    NoSuchElementException, 
    WebDriverException,
    StaleElementReferenceException
)

    
MARCAS = [
    "goodyear", "pirelli", "michelin", "dunlop", "bridgestone", "continental",
    "hankook", "bfgoodrich", "firestone", "kumho", "atras", "maxxis", "formula",
    "yokohama", "toyo", "nitto", "general", "cooper", "falken", "nexen", "sumitomo"
]
MODELOS = [
    "kelly edge", "formula evo", "sp touring",
    "assurance", "maxlife", "efficientgrip",
    "wrangler", "eagle", "energy", "direction","kelly", "p400 EVO", "bc20", "lm704", "enasave ec300"
    "xl tl primacy", "primacy 4", "f700", "sp sport", "FM800","eagle sport", "p400"
    "energy xm2"
]
def normalizar_str(s):
    s = unicodedata.normalize('NFD', s)
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return s.lower()

def _extrair_marca_titulo(titulo: str) -> str:
    if not titulo:
        return ""
    titulo_clean = titulo.lower().strip()
    for marca in MARCAS:
        if marca in titulo_clean:
            return marca.upper()
    return ""

def extrair_filtros_busca(termo: str):
    termo_low = termo.lower()
    medida = extrair_medida(termo_low)
    tokens = re.findall(r'\w+', termo_low)

    marca = None
    for m in MARCAS:
        if m in tokens:
            marca = m
            break

    modelo = None
    for mod in MODELOS:
        if mod in termo_low:
            modelo = mod
            break
    return medida, marca, modelo

def eh_kit_ou_multiplos_pneus(texto: str) -> bool:
    if not texto:
        return False
    texto_normalizado = unicodedata.normalize("NFD", texto)
    texto_limpo = "".join(c for c in texto_normalizado if unicodedata.category(c) != "Mn").lower()
    texto_sem_pontuacao = ''.join(char if char.isalnum() or char.isspace() else ' ' for char in texto_limpo)
    palavras = texto_sem_pontuacao.split()
    PALAVRAS_KIT = [
        "kit", "kits", "conjunto", "conjuntos", "par", "pares",
        "04", "duas", "dois", "quatro", "dupla",
        "duplas", "combo", "combos", "pack", "packs", "promoção",
        "promocao", "jogo", "oferta", "pacote", "pacotes", "lote", "lotes","casal", "pneus", 
        "unidades", "K2", "K4", "K6", "KIT2"
    ]
    print(f"DEBUG: Palavras do titulo '{palavras}'")
    for palavra in palavras:
        if palavra in PALAVRAS_KIT:
            print(f"DEBUG: Palavra '{palavra}' encontrada em '{texto_limpo}'")
            return True
    padroes_kit = [
        r'\b(kit|conjunto|par|pack|combo|lote|jogo|casal)\b',
        r'\b(kit|conjunto|par|pack|combo|lote|jogo)\s*(de|com)?\s*(\d+)\s*(pneu|pneus|unidade|unidades)\b',
        r'\b(dois|duas|quatro)\s*(pneu|pneus)\b',
        r'\b(dupla|duplas)\s*(de\s*)?(pneu|pneus)\b',
        r'\b(promoção|promocao|oferta)\s*(kit|conjunto|par|KIT2)\b',
        r'\b(kit|conjunto)\s*(com|de)\s*(\d+)\b', 
        r'\bk\d\b',      
        r'\bk\d{1,2}\b',
        r'\bkit\s*\d{1,2}\b', 
        r'\bpar\s*\d{1,2}\b'
    ]
    return any(re.search(p, texto_limpo) for p in padroes_kit)

CONFIG = {
    'USER_AGENTS': [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    ],
    'RETRY_ATTEMPTS': 3,
    'TIMEOUT': 30,
    'SCROLL_PAUSE_TIME': 2,
    'MAX_PRODUCTS_PER_PAGE': 50,
    'OUTPUT_FORMATS': ['json', 'csv', 'sqlite']
}

VENDEDORES_PALAVRAS_INVALIDAS = [
    "imperador", "imperatriz", "carli", "imperiodospneuspecas"
]

RE_PRECO = re.compile(r'[\d.,]+')

def delay_humano(min_delay=2.5, max_delay=5.5):
    delay = random.uniform(min_delay, max_delay)
    time.sleep(delay)
    return delay

def slugify(text: str) -> str:
    if not text:
        return "sem-nome"
    
    text = str(text).lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s/]+", "-", text)
    text = text.strip("-")
    return text[:100] if text else "produto"

def normalizar_termo(termo: str) -> str:
    termo = termo.replace("/", " ")
    termo = termo.replace("-", " ")
    termo = termo.replace("  ", " ")
    termo = termo.replace(" r ", " r")
    return termo.strip()

def extrair_medida(termo: str) -> str:
    termo = re.sub(r"[-_/]", " ", termo.lower())
    padrao = r'(\d{3})\s*(\d{2})\s*r?\s*(\d{2})'
    m = re.search(padrao, termo)
    if m:
        return f"{m.group(1)}-{m.group(2)}-r{m.group(3)}"
    return slugify(termo[:30])

@dataclass
class ProdutoMagalu:
    titulo: str
    preco: float
    link: str
    data_coleta: str
    preco_original: Optional[float] = None
    promocao: bool = False
    imagem: str = ""
    marketplace: str = "magazineluiza"
    categoria: str = ""
    disponivel: bool = True
    avaliacoes: int = 0
    nota_media: float = 0.0
    vendedor: str = ""
    frete_gratis: bool = False
    parcelamento: str = ""
    descricao: str = ""
    frete: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    def is_valid(self) -> bool:
        return bool(self.titulo and self.preco and self.link)

def parse_preco(preco_str: str) -> Optional[float]:
    if not preco_str:
        return None
    
    try:
        preco_clean = re.sub(r'[^\d,.]', '', preco_str)
        
        if ',' in preco_clean and '.' in preco_clean:
            preco_clean = preco_clean.replace('.', '').replace(',', '.')
        elif ',' in preco_clean:
            preco_clean = preco_clean.replace(',', '.')
        
        return float(preco_clean)
    except (ValueError, AttributeError):
        return None

def parse_avaliacoes(avaliacoes_str: str) -> int:
    if not avaliacoes_str:
        return 0
    
    numeros = re.findall(r'\d+', avaliacoes_str.replace('.', '').replace(',', ''))
    return int(numeros[0]) if numeros else 0

def parse_nota(nota_str: str) -> float:
    if not nota_str:
        return 0.0
    
    try:
        nota = re.search(r'(\d+[,.]?\d*)', nota_str)
        if nota:
            return float(nota.group(1).replace(',', '.'))
    except (ValueError, AttributeError):
        pass
    
    return 0.0

class DatabaseManager:
    
    def __init__(self, db_path: str = "data/magalu_products.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.init_database()
    
    def init_database(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS produtos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    titulo TEXT NOT NULL,
                    preco REAL NOT NULL,
                    link TEXT UNIQUE NOT NULL,
                    data_coleta TEXT NOT NULL,
                    preco_original REAL,
                    promocao BOOLEAN,
                    imagem TEXT,
                    marketplace TEXT,
                    categoria TEXT,
                    disponivel BOOLEAN,
                    avaliacoes INTEGER,
                    nota_media REAL,
                    vendedor TEXT,
                    frete_gratis BOOLEAN,
                    parcelamento TEXT,
                    descricao TEXT
                )
            ''')
            conn.commit()
    
    def salvar_produtos(self, produtos: List[ProdutoMagalu]):
        with sqlite3.connect(self.db_path) as conn:
            for produto in produtos:
                conn.execute('''
                    INSERT OR REPLACE INTO produtos 
                    VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    produto.titulo, produto.preco, produto.link, 
                    produto.data_coleta, produto.preco_original, produto.promocao,
                    produto.imagem, produto.marketplace, produto.categoria,
                    produto.disponivel, produto.avaliacoes, produto.nota_media,
                    produto.vendedor, produto.frete_gratis, produto.parcelamento,
                    produto.descricao
                ))
            conn.commit()

class ScraperMagalu:    
    base_url = "https://www.magazineluiza.com.br"
    marketplace = "magazineluiza"

    def __init__(self, headless: bool = True, delay_scroll: float = 1.0,  termo_busca: str = None,
                 max_workers: int = 1, output_dir: str = "data"):
        self.headless = headless
        self.delay_scroll = delay_scroll
        self.max_workers = max_workers
        self.output_dir = Path(output_dir)
        self.termo_busca = termo_busca
        self.filtro_medida, self.filtro_marca, self.filtro_modelo = extrair_filtros_busca(termo_busca) if termo_busca else (None, None, None)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.driver = None
        self.logger = self._setup_logger()
        self.db_manager = DatabaseManager()
        
    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger(self.marketplace)
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            log_dir = Path("logs")
            log_dir.mkdir(parents=True, exist_ok=True)
            log_file = log_dir / "scraper.log"

            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            
            formatter = logging.Formatter(
                '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)
            
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
        
        return logger

    def _init_driver(self) -> webdriver.Chrome:
        options = Options()
        
        if self.headless:
            options.add_argument("--headless=new")
        
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-plugins")
        options.add_argument("--disable-images")  
        
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--start-maximized")
        
        user_agent = random.choice(CONFIG['USER_AGENTS'])
        options.add_argument(f"--user-agent={user_agent}")
        
        options.add_argument("--lang=pt-BR")
        options.add_experimental_option('prefs', {
            'intl.accept_languages': 'pt-BR,pt,en-US,en'
        })
        
        try:
            driver = webdriver.Chrome(options=options)
            driver.set_page_load_timeout(CONFIG['TIMEOUT'])
            return driver
        except Exception as e:
            self.logger.error(f"Erro ao inicializar driver: {e}")
            raise

    def construir_url_busca(self, termo: str, pagina: int = 1, filtros: Optional[Dict] = None) -> str:
        termo = normalizar_termo(termo)
        termo_url = termo.strip().replace(" ", "+")
        url = f"{self.base_url}/busca/{termo_url}/?page={pagina}"
        
        if filtros:
            params = []
            for key, value in filtros.items():
                if value:
                    params.append(f"{key}={value}")
            
            if params:
                url += "&" + "&".join(params)
        
        return url

    def scroll_page(self, max_scrolls: int = 5):
        for i in range(max_scrolls):
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(self.delay_scroll)
            
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if i > 0 and new_height == getattr(self, '_last_height', 0):
                break
            self._last_height = new_height


    def extrair_produto_detalhado(self, card) -> Optional[ProdutoMagalu]:
        try:
            linhas = [l.strip() for l in card.text.split('\n') if l.strip()]
            
            def linha_titulo(linhas):
                for l in linhas:
                    if (
                        "pneu" in l.lower()
                        and not any(x in l.lower() for x in ["full", "patrocinado", "anúncio", "compre junto"])
                        and len(l) > 8
                    ):
                        return l
                return card.text.strip()
            titulo = linha_titulo(linhas)

            if not titulo or eh_kit_ou_multiplos_pneus(titulo):
                self.logger.info(f"Produto ignorado: {titulo} (kit, múltiplos ou casal)")
                return None

            if hasattr(self, 'filtro_medida') and self.filtro_medida:
                medida_produto = extrair_medida(titulo)
                if not medida_produto or medida_produto != self.filtro_medida:
                    self.logger.info(f"Produto ignorado: {titulo} (medida não bate)")
                    return None

            if hasattr(self, 'filtro_marca') and self.filtro_marca:
                marca_produto = _extrair_marca_titulo(titulo)
                if not marca_produto or marca_produto.lower() != self.filtro_marca.lower():
                    self.logger.info(f"Produto ignorado: {titulo} (marca não bate)")
                    return None

            if hasattr(self, 'filtro_modelo') and self.filtro_modelo:
                if self.filtro_modelo.lower() not in titulo.lower():
                    self.logger.info(f"Produto ignorado: {titulo} (modelo não bate)")
                    return None

            link = card.get_attribute('href')
            vendedor = ""

            aba_atual = self.driver.current_window_handle

            self.driver.execute_script("window.open(arguments[0], '_blank');", link)
            self.driver.switch_to.window(self.driver.window_handles[-1])

            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-testid='mod-sellerdetails'] label[data-testid='link']"))
                )
                vendedor_element = self.driver.find_element(By.CSS_SELECTOR, "div[data-testid='mod-sellerdetails'] label[data-testid='link']")
                vendedor = vendedor_element.text.strip()
                self.logger.info(f"Vendedor encontrado: {vendedor}")
                if any(invalido in vendedor.lower() for invalido in VENDEDORES_PALAVRAS_INVALIDAS):
                    self.logger.info(f"Produto ignorado: {titulo} (vendedor inválido: {vendedor})")
                    return None
                    
            except Exception as e:
                vendedor = "f{self.marketplace}"
            finally:
                self.driver.close()  
                self.driver.switch_to.window(aba_atual)  


            precos_no_texto = [parse_preco(l) for l in linhas if "R$" in l and parse_preco(l)]
            precos_validos = [p for p in precos_no_texto if p >= 100]
            preco = min(precos_validos) if precos_validos else None

            preco_original = None
            promocao = False
            if len(precos_no_texto) >= 2 and precos_no_texto[0] > precos_no_texto[-1]:
                preco_original = precos_no_texto[0]
                promocao = True

            if not preco:
                return None

            avaliacoes = 0
            nota_media = 0.0
            for l in linhas:
                m = re.search(r'(\d+(?:,\d+)?)\s*\((\d+)\)', l)
                if m:
                    nota_media = float(m.group(1).replace(',', '.'))
                    avaliacoes = int(m.group(2))
                    break

            frete_gratis = any('grátis' in l.lower() for l in linhas)

            if not vendedor:
                for marca in MARCAS:
                    if marca in titulo.lower():
                        vendedor = marca
                        break

            imagem = ""
            try:
                img = card.find_element(By.TAG_NAME, "img")
                imagem = img.get_attribute("src")
            except Exception:
                pass

            produto = ProdutoMagalu(
                titulo=titulo,
                preco=preco,
                link=link,
                data_coleta=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                preco_original=preco_original,
                promocao=promocao,
                imagem=imagem,
                avaliacoes=avaliacoes,
                nota_media=nota_media,
                frete_gratis=frete_gratis,
                vendedor=vendedor
            )

            return produto if produto.is_valid() else None

        except Exception as e:
            self.logger.warning(f"Erro ao extrair produto: {e}")
            return None

    def buscar_produtos(self, termo: str, pagina: int = 1, 
                    max_resultados: int = 20, filtros: Optional[Dict] = None,
                    scroll_pages: bool = True) -> List[ProdutoMagalu]:
        produtos = []
        
        for tentativa in range(CONFIG['RETRY_ATTEMPTS']):
            try:
                if not self.driver:
                    self.driver = self._init_driver()
                
                url = self.construir_url_busca(termo, pagina, filtros)
                self.logger.info(f"Tentativa {tentativa + 1}: Carregando {url}")
                self.driver.get(url)
                
                WebDriverWait(self.driver, CONFIG['TIMEOUT']).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[data-testid='product-card-container']"))
                )

                delay_humano(3, 6)
                if scroll_pages:
                    self.scroll_page()
                delay_humano(2, 3)

                cards = self.driver.find_elements(By.CSS_SELECTOR, "a[data-testid='product-card-container']")

                self.logger.info(f"Encontrados {len(cards)} produtos na página")
                if not cards:
                    screenshot_path = Path("screenshot-falha.png")
                    self.driver.save_screenshot(str(screenshot_path))
                    self.logger.warning(f"Nenhum card! Screenshot salvo em {screenshot_path}")
                else:
                    for idx, card in enumerate(cards[:5]):
                        try:
                            print(f"Card #{idx} HREF: {card.get_attribute('href')}")
                            print(f"Card #{idx} TEXT: {card.text[:100]}")
                        except Exception:
                            continue

                for i, card in enumerate(cards):
                    if len(produtos) >= max_resultados:
                        break
                    try:
                        produto = self.extrair_produto_detalhado(card)
                        if produto:
                            produtos.append(produto)
                            self.logger.debug(f"Produto {len(produtos)}: {produto.titulo}")
                        if i % 5 == 0:
                            delay_humano(1, 2)
                    except StaleElementReferenceException:
                        self.logger.warning(f"Elemento obsoleto no produto {i}")
                        continue
                    except Exception as e:
                        self.logger.warning(f"Erro ao processar produto {i}: {e}")
                        continue

                self.logger.info(f"Coletados {len(produtos)} produtos válidos")
                break
            
            except TimeoutException:
                self.logger.error(f"Timeout na tentativa {tentativa + 1}")
                if tentativa < CONFIG['RETRY_ATTEMPTS'] - 1:
                    delay_humano(5, 10)
            except Exception as e:
                self.logger.error(f"Erro na tentativa {tentativa + 1}: {e}")
                if tentativa < CONFIG['RETRY_ATTEMPTS'] - 1:
                    delay_humano(5, 10)
                    if self.driver:
                        self.driver.quit()
                        self.driver = None
        return produtos

    def buscar_multiplas_paginas(self, termo: str, max_paginas: int = 3, 
                                max_resultados_total: int = 100) -> List[ProdutoMagalu]:
        todos_produtos = []
        
        for pagina in range(1, max_paginas + 1):
            if len(todos_produtos) >= max_resultados_total:
                break
                
            self.logger.info(f"Processando página {pagina}...")
            
            produtos_pagina = self.buscar_produtos(
                termo, 
                pagina=pagina, 
                max_resultados=max_resultados_total - len(todos_produtos)
            )
            
            if not produtos_pagina:
                self.logger.info(f"Nenhum produto encontrado na página {pagina}. Parando busca.")
                break
            
            todos_produtos.extend(produtos_pagina)
            self.logger.info(f"Total coletado até agora: {len(todos_produtos)} produtos")
            
            delay_humano(3, 8)
        
        return todos_produtos  

    def salvar_resultados(self, produtos: List[ProdutoMagalu], termo: str, formatos: List[str] = None) -> Dict[str, str]:
        if not formatos:
            formatos = ['json']

        arquivos_salvos = {}
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        medida = extrair_medida(termo)
        output_dir = self.output_dir / "raw" / medida
        output_dir.mkdir(parents=True, exist_ok=True)
        slug = slugify(termo)

        if 'json' in formatos:
            arquivo_json = output_dir / f"{slug}_{timestamp}.json"
            with open(arquivo_json, "w", encoding="utf-8") as f:
                json.dump([produto.to_dict() for produto in produtos], f, ensure_ascii=False, indent=2)
            arquivos_salvos['json'] = str(arquivo_json)
            self.logger.info(f"JSON salvo: {arquivo_json}")

        if 'csv' in formatos and produtos:
            arquivo_csv = output_dir / f"{slug}_{timestamp}.csv"
            with open(arquivo_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=produtos[0].to_dict().keys())
                writer.writeheader()
                for produto in produtos:
                    writer.writerow(produto.to_dict())
            arquivos_salvos['csv'] = str(arquivo_csv)
            self.logger.info(f"CSV salvo: {arquivo_csv}")

        if 'sqlite' in formatos:
            self.db_manager.salvar_produtos(produtos)
            arquivos_salvos['sqlite'] = str(self.db_manager.db_path)
            self.logger.info(f"Dados salvos no banco: {self.db_manager.db_path}")

        return arquivos_salvos

    def buscar_varias_paginas(self, termo: str, max_paginas: int = 5, max_resultados: int = 100, filtros: Optional[dict] = None):
            todos_produtos = []
            pagina = 1
            while len(todos_produtos) < max_resultados and pagina <= max_paginas:
                self.logger.info(f"--- Buscando página {pagina} ---")
                produtos = self.buscar_produtos(
                    termo=termo,
                    pagina=pagina,
                    max_resultados=max_resultados - len(todos_produtos),
                    filtros=filtros,
                    scroll_pages=True
                )
                if not produtos:
                    self.logger.info(f"Nenhum produto encontrado na página {pagina}. Parando busca.")
                    break
                todos_produtos.extend(produtos)
                self.logger.info(f"Coletados {len(produtos)} produtos na página {pagina}. Total acumulado: {len(todos_produtos)}")
                pagina += 1
                delay_humano(2, 4)  
            return todos_produtos

    def executar_busca_completa(self, termo: str, max_paginas: int = 5, 
                               max_resultados: int = 100, 
                               formatos: List[str] = None) -> Dict[str, Any]:
        inicio = time.time()
        
        self.logger.info(f"Iniciando busca completa para: '{termo}'")
        self.logger.info(f"Parâmetros: {max_paginas} páginas, {max_resultados} produtos máx")
        
        try:
            produtos = self.buscar_varias_paginas(
                termo=termo,
                max_paginas=max_paginas,
                max_resultados=max_resultados
            )

            if not produtos:
                self.logger.warning("Nenhum produto encontrado!")
                return {
                    'termo': termo,
                    'produtos_encontrados': 0,
                    'promocoes': 0,
                    'preco_medio': 0.0,
                    'tempo_execucao': round(time.time() - inicio, 2),
                    'arquivos': {}
                }

            promocoes = sum(1 for p in produtos if getattr(p, "promocao", False))
            preco_medio = sum(p.preco for p in produtos) / len(produtos)
            
            self.logger.info(f"Busca concluída: {len(produtos)} produtos")
            self.logger.info(f"Produtos em promoção: {promocoes}")
            self.logger.info(f"Preço médio: R$ {preco_medio:.2f}")
            
            arquivos = self.salvar_resultados(produtos, termo, formatos)
            
            tempo_total = time.time() - inicio
            relatorio = {
                'termo': termo,
                'produtos_encontrados': len(produtos),
                'promocoes': promocoes,
                'preco_medio': round(preco_medio, 2),
                'tempo_execucao': round(tempo_total, 2),
                'arquivos': arquivos
            }
            
            self.logger.info(f"Execução concluída em {tempo_total:.2f}s")
            return relatorio
            
        except Exception as e:
            self.logger.error(f"Erro na execução completa: {e}")
            raise
        finally:
            self.fechar()


    def fechar(self):
        if self.driver:
            try:
                self.driver.quit()
                self.logger.info("Driver fechado com sucesso")
            except Exception as e:
                self.logger.warning(f"Erro ao fechar driver: {e}")
            finally:
                self.driver = None

def main():
    parser = argparse.ArgumentParser(
        description="Scraper Completo do Magazine Luiza",
        formatter_class=argparse.RawDescriptionHelpFormatter,)

    parser.add_argument("--termo", type=str, help="Termo de busca")
    parser.add_argument("--paginas", type=int, default=3, 
                       help="Número máximo de páginas (padrão: 3)")
    parser.add_argument("--max", type=int, default=50, 
                       help="Número máximo de produtos (padrão: 50)")
    parser.add_argument("--formatos", nargs='+', 
                       choices=['json', 'csv', 'sqlite'],
                       default=['json'], 
                       help="Formatos de saída (padrão: json)")
    parser.add_argument("--headless", type=str, choices=['true', 'false'], 
                       default='true', help="Executar em modo headless (padrão: true)")
    parser.add_argument("--output", default="data", 
                       help="Diretório de saída (padrão: data)")
    parser.add_argument("--delay", type=float, default=1.0, 
                       help="Delay de scroll em segundos (padrão: 1.0)")
    parser.add_argument("--verbose", action='store_true', 
                       help="Modo verbose (mais logs)")
    parser.add_argument("--lote-json", type=str, 
                       help="Arquivo JSON com termos de busca em lote (opcional)")
    parser.add_argument("--idx-from",type=int, default=0,
                       help="Índice inicial para busca em lote (padrão: 0)")
    parser.add_argument("--idx-to", type=int, help="Índice final para busca em lote (opcional, padrão: até o final)")

    args = parser.parse_args()

    if args.lote_json and not os.path.isfile(args.lote_json):
        root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        alt_path = os.path.join(root_path, args.lote_json)
        if os.path.isfile(alt_path):
            args.lote_json = alt_path

    if args.lote_json:
        with open(args.lote_json, "r", encoding="utf-8") as f:
            queries = json.load(f)
        idx_to = args.idx_to if args.idx_to is not None else len(queries)
        for idx, item in enumerate(queries[args.idx_from:idx_to], start=args.idx_from):
            print(f"\n==== Buscando produto {idx}: {item.get('brand')} {item.get('line_model')} {item.get('width')}/{item.get('aspect')}R{item.get('rim')} ====")
            termo = item.get("query_flex") or item.get("query_strict")
            scraper = ScraperMagalu(
                headless=args.headless.lower() == 'true',
                delay_scroll=args.delay,
                output_dir=args.output, 
                termo_busca=termo
            )
            try:
                relatorio = scraper.executar_busca_completa(
                    termo=termo,
                    max_paginas=args.paginas,
                    max_resultados=args.max,
                    formatos=args.formatos
                )

                medida = f"{item.get('width', '')}_{item.get('aspect', '')}_r{item.get('rim', '')}"
                marca = item.get('brand', '').replace(" ", "_")
                modelo = item.get('line_model', '').replace(" ", "_")
                nome_produto = f"{marca}_{modelo}".strip("_")
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                output_dir = os.path.join(args.output, "raw", medida)
                os.makedirs(output_dir, exist_ok=True)

                for formato in args.formatos:
                    nome_arquivo = f"{timestamp}_{medida}_{nome_produto}.{formato}"
                    caminho_arquivo = os.path.join(output_dir, nome_arquivo)

                    if formato == "json":
                        with open(caminho_arquivo, "w", encoding="utf-8") as fjson:
                            json.dump(relatorio, fjson, ensure_ascii=False, indent=2)
                    elif formato == "csv":
                        import csv
                        produtos = relatorio.get("produtos", [])
                        if produtos:
                            with open(caminho_arquivo, "w", encoding="utf-8", newline="") as fcsv:
                                writer = csv.DictWriter(fcsv, fieldnames=produtos[0].keys())
                                writer.writeheader()
                                writer.writerows(produtos)
                    elif formato == "sqlite":
                        import sqlite3
                        produtos = relatorio.get("produtos", [])
                        if produtos:
                            conn = sqlite3.connect(caminho_arquivo)
                            cur = conn.cursor()
                            keys = produtos[0].keys()
                            columns = ', '.join([f"{k} TEXT" for k in keys])
                            cur.execute(f"CREATE TABLE IF NOT EXISTS produtos ({columns})")
                            for prod in produtos:
                                values = tuple(str(prod[k]) for k in keys)
                                placeholders = ', '.join('?' for _ in keys)
                                cur.execute(f"INSERT INTO produtos VALUES ({placeholders})", values)
                            conn.commit()
                            conn.close()
                    print(f"Arquivo salvo: {caminho_arquivo}")

            except Exception as e:
                print(f"Erro no produto {idx}: {e}")
            finally:
                scraper.fechar()
        exit(0)

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    scraper = ScraperMagalu(
        headless=args.headless.lower() == 'true',
        delay_scroll=args.delay,
        output_dir=args.output, 
        termo_busca=args.termo
    )

    
    try:
        relatorio = scraper.executar_busca_completa(
            termo=args.termo,
            max_paginas=args.paginas,
            max_resultados=args.max,
            formatos=args.formatos
        )
        
        print("\n" + "="*60)
        print("RELATÓRIO FINAL")
        print("="*60)
        print(f"Termo buscado: {relatorio['termo']}")
        print(f"Produtos encontrados: {relatorio['produtos_encontrados']}")
        print(f"Produtos em promoção: {relatorio['promocoes']}")
        print(f"Preço médio: R$ {relatorio['preco_medio']:.2f}")
        print(f"Tempo de execução: {relatorio['tempo_execucao']:.2f}s")
        print("\nArquivos gerados:")
        for formato, arquivo in relatorio['arquivos'].items():
            print(f"  {formato.upper()}: {arquivo}")
        print("="*60)
        
    except KeyboardInterrupt:
        print("\nOperação cancelada pelo usuário")
    except Exception as e:
        print(f"Erro na execução: {e}")
        logging.exception("Erro detalhado:")
    finally:
        scraper.fechar()

if __name__ == "__main__":
    main()

