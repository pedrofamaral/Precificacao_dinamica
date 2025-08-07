from __future__ import annotations

import abc
import logging
import random
import re
import time
from dataclasses import dataclass
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Set

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# ─────────────────────────────── Dataclass Produto ──────────────────────────────

@dataclass(slots=True)
class Product:
    titulo: str
    preco: float | None
    link: str
    marketplace: str
    categoria: str = ""
    marca: str = ""
    local: str = ""
    vendedor: str = ""
    condicao: str = ""  # novo, usado, etc.
    frete_gratis: bool = False
    data_coleta: str = ""  # YYYY‑MM‑DD HH:MM:SS

    def to_dict(self) -> dict:
        return asdict(self)

    def __repr__(self) -> str:  # compacto para debug
        p = f"R$ {self.preco:,.2f}" if self.preco is not None else "–"
        return f"<{self.marketplace}:{self.titulo[:40]}… | {p}>"

# ─────────────────────────────── Classe Base ────────────────────────────────────

class ScraperBase(abc.ABC):
    """Infra‑estrutura compartilhada para scrapers Selenium‑based."""

    # Deve ser sobrescrito pela subclasse
    marketplace: str = "base"

    # --------------------------- Métodos que subclasses DEVEM implementar ------

    @abc.abstractmethod
    def _build_search_url(self, termo: str, page: int = 1) -> str: ...

    @abc.abstractmethod
    def _coletar_produtos_pagina(self, links_vistos: Set[str]) -> List[Product]: ...

    @abc.abstractmethod
    def _ir_proxima_pagina(self) -> bool: ...

    # Sobrescreva se o site exigir clique em banner de cookies
    def _aceitar_cookies(self) -> None:  # noqa: D401
        pass

    # --------------------------- Construtor ------------------------------------

    def __init__(
        self,
        *,
        headless: bool = True,
        timeout: int = 15,
        delay_scroll: float = 0.8,
        max_scrolls: int = 8,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.headless = headless
        self.timeout = timeout
        self.delay_scroll = delay_scroll
        self.max_scrolls = max_scrolls
        self.driver: Optional[webdriver.Chrome] = None
        
        # Configurar logger - prioridade: parâmetro > classe específica > padrão
        if logger:
            self.logger = logger
        else:
            self.logger = self._setup_logger()

    # --------------------------- API pública -----------------------------------

    def buscar(
        self,
        termo: str,
        *,
        max_resultados: int = 100,
        max_paginas: int = 10,
    ) -> List[Product]:
        """Raspa resultados para *termo* retornando até *max_resultados* itens."""

        url = self._build_search_url(termo, page=1)
        self.driver = self._configurar_driver()
        produtos: List[Product] = []
        links_vistos: Set[str] = set()

        try:
            self.logger.info("🔍 Buscando '%s' em %s", termo, self.marketplace)
            self.logger.debug("📍 URL: %s", url)
            
            self.driver.get(url)
            self._aceitar_cookies()

            pagina = 1
            sem_novos = 0
            
            while len(produtos) < max_resultados and pagina <= max_paginas:
                self.logger.debug("📄 Processando página %s", pagina)
                self._rolar_pagina()

                novos = self._coletar_produtos_pagina(links_vistos)
                produtos.extend(novos)
                
                self.logger.info(
                    "📦 Página %s: +%s produtos (Total: %s/%s)", 
                    pagina, len(novos), len(produtos), max_resultados
                )

                if novos:
                    sem_novos = 0
                else:
                    sem_novos += 1
                    self.logger.warning("⚠️ Página %s sem produtos novos (%s/3)", pagina, sem_novos)
                    if sem_novos >= 3:
                        self.logger.warning("🛑 3 páginas sem novos produtos - encerrando")
                        break

                if len(produtos) >= max_resultados:
                    self.logger.info("🎯 Meta de %s produtos atingida", max_resultados)
                    break

                if not self._ir_proxima_pagina():
                    self.logger.info("🔚 Não há mais páginas disponíveis")
                    break

                pagina += 1
                sleep_time = self._rand()
                self.logger.debug("⏱️ Pausa entre páginas: %.2fs", sleep_time)
                time.sleep(sleep_time)

        except Exception as e:
            self.logger.error("❌ Erro durante a busca: %s", str(e))
            self.logger.debug("📋 Traceback:", exc_info=True)
            raise
        finally:
            if self.driver is not None:
                self.logger.debug("🔧 Fechando navegador")
                self.driver.quit()

        self.logger.info("✅ Busca finalizada: %s produtos coletados", len(produtos))
        return produtos[:max_resultados]

    # Alias de compatibilidade com versão antiga
    buscar_produtos = buscar

    # --------------------------- Helpers genéricos -----------------------------

    @staticmethod
    def _rand(a: float = 0.6, b: float = 1.4) -> float:
        """Retorna intervalo pseudo‑aleatório para pausas humanas."""
        return random.uniform(a, b)

    def _rolar_pagina(self) -> None:
        """Rola a página incrementalmente para disparar lazy‑load."""
        try:
            body_height = self.driver.execute_script("return document.body.scrollHeight")
            step = max(body_height // self.max_scrolls, 700)
            pos = 0
            
            self.logger.debug("📜 Rolando página (altura: %spx, step: %spx)", body_height, step)
            
            for i in range(self.max_scrolls):
                pos += step
                self.driver.execute_script("window.scrollTo(0, arguments[0]);", pos)
                time.sleep(self.delay_scroll)
                
        except Exception as e:
            self.logger.debug("⚠️ Erro ao rolar página: %s", str(e))

    def _configurar_driver(self) -> webdriver.Chrome:  # noqa: D401
        """Configura e retorna instância do ChromeDriver"""
        self.logger.debug("🔧 Configurando ChromeDriver (headless=%s)", self.headless)
        
        opts = Options()
        if self.headless:
            opts.add_argument("--headless=new")
            opts.add_argument("--window-size=1920,1080")
            opts.add_argument("--disable-blink-features=AutomationControlled")
            opts.add_experimental_option("excludeSwitches", ["enable-automation"])
            opts.add_argument("--disable-gpu")
            opts.add_argument("--no-sandbox")
            opts.add_argument("--disable-dev-shm-usage")
            opts.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
            )
        
        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=opts)
            driver.execute_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
            driver.set_page_load_timeout(self.timeout)
            self.logger.debug("✅ ChromeDriver configurado com sucesso")
            return driver
        except Exception as e:
            self.logger.error("❌ Erro ao configurar ChromeDriver: %s", str(e))
            raise

    @staticmethod
    def _limpar_preco(txt: str | None) -> float | None:  # noqa: D401
        """Extrai valor numérico de texto de preço"""
        if not txt:
            return None
        txt = txt.replace("\u00a0", " ")  # NBSP
        nums = re.findall(r"\d+[.,]?\d*", txt)
        if not nums:
            return None
        try:
            # Pega o primeiro número encontrado e converte
            num_str = nums[0].replace(".", "").replace(",", ".")
            return float(num_str)
        except (ValueError, IndexError):
            return None

    # --------------------------- Logging --------------------------------------

    def _setup_logger(self) -> logging.Logger:  # noqa: D401
        """Configura sistema de logging padrão"""
        # Criar diretório de logs
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)

        # Nome único do logger baseado na classe
        logger_name = f"{self.marketplace}_scraper"
        logger = logging.getLogger(logger_name)
        
        # Evitar handlers duplicados
        if logger.handlers:
            return logger
            
        logger.setLevel(logging.DEBUG)

        # Formatter
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)8s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        # Handler para arquivo
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"{self.marketplace}_{timestamp}.log"
        
        try:
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            print(f"⚠️ Erro ao criar arquivo de log: {e}")

        # Handler para console
        try:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            
            # Formatter simplificado para console
            console_formatter = logging.Formatter(
                "%(asctime)s [%(levelname)s] %(message)s",
                datefmt="%H:%M:%S"
            )
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)
        except Exception as e:
            print(f"⚠️ Erro ao configurar log do console: {e}")

        logger.info("📝 Sistema de logging inicializado")
        logger.debug("📁 Arquivo de log: %s", log_file)
        
        return logger