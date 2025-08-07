#!/usr/bin/env python3
"""Amazon scraper — versão 2025-07 MELHORADA
Inclui:
• logging + RotatingFileHandler
• seletores corrigidos baseados no HTML real
• delays aumentados e mais variáveis
• dump de HTML/screenshot em modo DEBUG
• detecção de CAPTCHA (validateCaptcha)
• melhor extração de dados dos produtos
"""

from pathlib import Path
import argparse, json, re, sys, time, random, logging
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass, asdict
from typing import List, Optional
from datetime import datetime
import logging
import requests
from selenium.webdriver import Chrome, ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    ElementClickInterceptedException,
    StaleElementReferenceException,
    WebDriverException,
)

# ──────────────────────────── logging ────────────────────────────── #
LOG_DIR = Path(__file__).with_suffix("").parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)


def _setup_logger(debug: bool = False) -> logging.Logger:
    logger = logging.getLogger("amazon.scraper")
    logger.setLevel(logging.DEBUG if debug else logging.INFO)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    )

    logger.handlers.clear()

    file_h = RotatingFileHandler(
        LOG_DIR / "scraper.log",
        maxBytes=2_000_000,
        backupCount=5,
        encoding="utf-8",
    )
    file_h.setFormatter(fmt)
    file_h.setLevel(logging.DEBUG)

    console_h = logging.StreamHandler(sys.stdout)
    console_h.setFormatter(fmt)
    console_h.setLevel(logging.DEBUG if debug else logging.INFO)

    logger.addHandler(file_h)
    logger.addHandler(console_h)
    return logger


logger = _setup_logger()

_MONEY_RE = re.compile(r"R\$[\s\xa0]*([\d\.\,]+)")
_FREE_SHIPPING = re.compile(r"frete\s+gr[aá]tis", re.I)
_FRETE_RE = re.compile(r"R\$[\s\xa0]*([\d\.\,]+)\s*de\s*frete", re.I)

_SLEEP_MIN, _SLEEP_MAX = 4.0, 8.0  
_PAGE_LOAD_DELAY = (2.0, 4.0)  
_HEADLESS_DEF = True

CARD_SEL = ("div[data-component-type='s-search-result'][data-asin]",)
TITLE_SEL = "h2 a.a-link-normal, h2 .a-link-normal"  
PRICE_SEL = "span.a-price span.a-offscreen"
PRICE_WHOLE_SEL    = "span.a-price-whole"
PRICE_FRACTION_SEL = "span.a-price-fraction"
PRICE_OFFSCREEN    = "span.a-price .a-offscreen"     
ALT_PRICE_SEL = "span.a-price-whole, .a-price .a-offscreen"  
RATING_SEL = "span.a-icon-alt"
SHIPPING_SEL = "span[aria-label*='frete'], .a-row:contains('frete')"


@dataclass
class Produto:
    termo_busca: str
    titulo: str
    preco: float
    url: str
    asin: str
    frete_gratis: bool
    valor_frete: float | None
    rating: str | None = None
    num_reviews: str | None = None

    def to_dict(self) -> dict:
        d = asdict(self)
        d["preco"] = f"{self.preco:.2f}"
        if self.valor_frete is not None:
            d["valor_frete"] = f"{self.valor_frete:.2f}"
        return d

_BRANDS = (
    "goodyear", "pirelli", "bridgestone", "continental", "michelin",
    "dunlop", "firestone", "kumho", "hankook", "maxxis", "linglong",
)

def _detectar_marca(texto: str) -> str | None:
    t = texto.lower()
    for m in _BRANDS:
        if m in t:
            return m
    return None

_KIT_RE = re.compile(r"\b(kit|jogo|par|unidades?|[2-9]x)\b", re.I)
def _eh_kit_ou_multiplos(titulo: str) -> bool:
    return bool(_KIT_RE.search(titulo))

def _construir_dim_pattern(termo: str) -> re.Pattern:
    digs = re.findall(r"\d+", termo)
    if len(digs) < 2:
        return re.compile(".*")      
    width, *_, rim = digs
    regex = rf"{width}\D{{0,4}}\d{{1,3}}\D{{0,4}}{rim}"
    return re.compile(regex, re.I)


class AmazonScraper:
    BASE = "https://www.amazon.com.br"

    def __init__(self, headless: bool = _HEADLESS_DEF, proxy: str | None = None):
        self.driver = self._make_driver(headless, proxy)
        logger.info("Chrome iniciado (headless=%s, proxy=%s)", headless, proxy)

    @staticmethod
    def _make_driver(headless: bool, proxy: str | None) -> Chrome:
        opts = ChromeOptions()
        if headless:
            opts.add_argument("--headless=new")
        
        opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        opts.add_argument("--window-size=1920,1080")  
        opts.add_argument("--lang=pt-BR")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option('useAutomationExtension', False)
        
        if proxy:
            opts.add_argument(f"--proxy-server={proxy}")

        try:
            driver = Chrome(options=opts)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            return driver
        except WebDriverException as e:
            logger.error("Falha ao iniciar Chrome: %s", e)
            raise

    # ---------------------------------------------------------------- #
    def _build_search_url(self, termo: str, page: int = 1) -> str:
        url = f"{self.BASE}/s?k={termo.replace(' ', '+')}&s=price-asc-rank&page={page}&ref=sr_pg_{page}"
        logger.debug("URL criada: %s", url)
        return url

    @staticmethod
    def _rand_sleep():
        """Sleep com tempo mais variável e humano"""
        time.sleep(random.uniform(_SLEEP_MIN, _SLEEP_MAX))

    @staticmethod
    def _page_load_delay():
        """Delay extra após carregamento da página"""
        time.sleep(random.uniform(*_PAGE_LOAD_DELAY))

    def _wait_cards(self):
        selector = ", ".join(CARD_SEL)
        logger.debug("Esperando por cards com: %r", selector)
        WebDriverWait(self.driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
        )


    @staticmethod
    def _clean_price(raw: str | None) -> Optional[float]:
        """Limpeza melhorada de preços"""
        if not raw:
            return None
        
        # Remove tags HTML se houver
        import re
        raw = re.sub(r'<[^>]+>', '', raw)
        
        m = _MONEY_RE.search(raw)
        if not m:
            return None
        try:
            price_str = m.group(1).replace(".", "").replace(",", ".")
            return float(price_str)
        except ValueError:
            logger.debug("Erro ao converter preço: %s", raw)
            return None

    NEXT_BTN_SELS = (
        "ul.a-pagination li.a-last a",
    )

    def _ir_proxima_pagina(self) -> bool:
        for sel in self.NEXT_BTN_SELS:
            try:
                btns = self.driver.find_elements(By.CSS_SELECTOR, sel)
                if not btns:
                    continue
                btn = btns[0]
                if not (btn.is_displayed() and btn.is_enabled()):
                    continue
                self.driver.execute_script("arguments[0].scrollIntoView(true);", btn)
                time.sleep(random.uniform(0.3, 0.6))
                href = btn.get_attribute("href") or ""
                if not href:
                    return False
                try:
                    r = requests.head(href, allow_redirects=True, timeout=5)
                    if r.status_code != 200:
                        continue
                except Exception:
                    pass
                btn.click()
                logger.info("Clique em 'Próxima página' com seletor %r", sel)
                WebDriverWait(self.driver, 10).until(EC.staleness_of(btn))
                self._delay_after_page()
                logger.info("Navegado para próxima página")
                return True
            except (ElementClickInterceptedException, StaleElementReferenceException, TimeoutException) as e:
                logger.debug("Tentativa de next-page falhou para %r: %s", sel, e)
                continue
        logger.info("Não foi possível avançar para próxima página")
        return False



    def _extrair_valor_frete(self, card):
        try:
            frete_elements = card.find_elements(By.CSS_SELECTOR, 
                "span[aria-label*='frete'], .a-row, .a-size-base")
            
            for el in frete_elements:
                txt = el.text.lower() if el.text else ""
                if not txt:
                    continue
                    
                if _FREE_SHIPPING.search(txt):
                    return True, 0.0
                    
                m = _FRETE_RE.search(txt)
                if m:
                    try:
                        frete_val = float(m.group(1).replace(".", "").replace(",", "."))
                        return False, frete_val
                    except ValueError:
                        continue
                        
        except Exception as e:
            logger.debug("Erro ao extrair frete: %s", e)
            
        return False, None

    def _extrair_rating(self, card) -> tuple[str | None, str | None]:
        try:
            rating_el = card.find_element(By.CSS_SELECTOR, RATING_SEL)
            rating_text = rating_el.get_attribute("innerHTML") or ""
            
            rating_match = re.search(r'(\d+,?\d*)\s*de\s*5', rating_text)
            rating = rating_match.group(1) if rating_match else None
            
            try:
                reviews_el = card.find_element(By.CSS_SELECTOR, "a span.a-size-base")
                num_reviews = reviews_el.text.strip()
            except:
                num_reviews = None
                
            return rating, num_reviews
            
        except NoSuchElementException:
            return None, None

    def _extrair_preco(self, card) -> Optional[float]:
        try:
            whole = card.find_element(By.CSS_SELECTOR, PRICE_WHOLE_SEL).text
            frac  = card.find_element(By.CSS_SELECTOR, PRICE_FRACTION_SEL).text
            whole = whole.replace(".", "").replace("\xa0", "").strip()
            if whole and frac:
                return float(f"{whole}.{frac}")
        except NoSuchElementException:
            pass

        try:
            txt = card.find_element(By.CSS_SELECTOR, PRICE_OFFSCREEN).text
            num = re.sub(r"[^\d,]", "", txt)            # deixa só 1.234,56
            num = num.replace(".", "").replace(",", ".")
            return float(num)
        except (NoSuchElementException, ValueError):
            return None

    # ─── Extração melhorada de produtos ──────────────────────────── #
    def _extrair_produto(
    self,
    card,
    termo: str,
    dim_pat: re.Pattern,
    marca_desejada: str | None
    ) -> Optional[Produto]:
        try:
            try:
                title_el = card.find_element(By.CSS_SELECTOR, "[data-cy='title-recipe'] span")
                is_recipe = True
            except NoSuchElementException:
                title_el = card.find_element(By.CSS_SELECTOR, "h2 span")
                is_recipe = False

            titulo = title_el.text.strip()
            logger.debug("→ Título bruto: %r", titulo)

            if self._eh_kit_ou_multiplos(titulo):
                logger.debug("   ❌ descartado POR KIT: %r", titulo)
                return None

            if not dim_pat.search(titulo):
                logger.debug("   ❌ descartado POR DIM (pattern=%r): %r", dim_pat.pattern, titulo)
                return None

            detected_brand = self._detectar_marca(titulo)
            logger.debug("   Marca detectada: %r – Marca desejada: %r", detected_brand, marca_desejada)
            if marca_desejada and detected_brand != marca_desejada:
                logger.debug("   ❌ descartado POR MARCA: %r", titulo)
                return None

            sel_link = "[data-cy='title-recipe'] a.a-link-normal" if is_recipe else "h2 a.a-link-normal"
            link_el = card.find_element(By.CSS_SELECTOR, sel_link)
            href   = (link_el.get_attribute("href") or "").strip()
            url    = href.split("?")[0]
            m      = re.search(r"/dp/([A-Z0-9]{10})", href)
            asin   = m.group(1) if m else ""
            logger.debug("   ASIN extraído: %r", asin)

            if is_recipe:
                price_txt = card.find_element(
                    By.CSS_SELECTOR,
                    "[data-cy='secondary-offer-recipe'] span.a-color-base"
                ).text
            else:
                price_txt = self._extrair_preco_texto(card)

            preco = self._clean_price(price_txt)
            if preco is None:
                logger.debug("   ❌ descartado POR PREÇO inválido: %r", price_txt)
                return None

            frete_gratis, valor_frete = self._extrair_valor_frete(card)
            rating, num_reviews       = self._extrair_rating(card)

            logger.debug("   ✔ PASSOU: %r – R$ %.2f", titulo, preco)
            return Produto(
                termo_busca  = termo,
                titulo       = titulo,
                preco        = preco,
                url          = url,
                asin         = asin,
                frete_gratis = frete_gratis,
                valor_frete  = valor_frete,
                rating       = rating,
                num_reviews  = num_reviews,
            )

        except Exception as e:
            logger.error("Erro ao extrair produto (%r): %s", titulo if 'titulo' in locals() else None, e)
            return None

    def buscar_produtos(
        self,
        termo: str,
        max_resultados: int | None = None
    ) -> List[Produto]:
        produtos, vistos = [], set()
        page = 1
        consecutive_failures = 0
        max_failures = 3

        dim_pat = self._construir_dim_pattern(termo)
        marca_desejada = self._detectar_marca(termo or "")

        while consecutive_failures < max_failures:
            url = self._build_search_url(termo, page)
            logger.info("🔎 %r - página %d", termo, page)

            try:
                self.driver.get(url)
                self._page_load_delay()

                # ── CAPTCHA/bloqueio ──────────────────────────────
                if "validateCaptcha" in self.driver.current_url or "robot_check" in self.driver.current_url:
                    logger.warning("CAPTCHA/bloqueio detectado na página %d", page)
                    Path("debug").mkdir(exist_ok=True)
                    Path(f"debug/captcha_page_{page}.html") \
                        .write_text(self.driver.page_source, encoding="utf-8")
                    self.driver.save_screenshot(f"debug/captcha_page_{page}.png")
                    break
                # ───────────────────────────────────────────────────

                try:
                    self._wait_cards()
                except TimeoutException:
                    logger.warning("Timeout esperando cards na página %d", page)
                    consecutive_failures += 1
                    continue

                if logger.isEnabledFor(logging.DEBUG):
                    Path("debug").mkdir(exist_ok=True)
                    Path(f"debug/page_{page}.html") \
                        .write_text(self.driver.page_source, encoding="utf-8")
                    self.driver.save_screenshot(f"debug/page_{page}.png")

                cards = self.driver.find_elements(
                    By.CSS_SELECTOR, ", ".join(CARD_SEL)
                )
                print(f"🔍 {len(cards)} cards encontrados na página {page}")
                logger.info("%d cards encontrados na página %d", len(cards), page)

                if not cards:
                    consecutive_failures += 1
                    logger.warning("Nenhum card encontrado na página %d", page)
                    continue

                consecutive_failures = 0
                produtos_pagina = 0

                for i, card in enumerate(cards):
                    try:
                        prod = self._extrair_produto(card, termo, dim_pat, marca_desejada)
                        if not prod:
                            logger.debug("Card %d: filtrado/sem dados", i)
                            continue

                        if prod.url in vistos:
                            logger.debug("Duplicado ignorado: %s", prod.titulo[:50])
                            continue

                        produtos.append(prod)
                        vistos.add(prod.url)
                        produtos_pagina += 1

                        logger.info(
                            "✓ Produto %d: %s – R$ %.2f",
                            len(produtos), prod.titulo[:60], prod.preco
                        )

                        if max_resultados and len(produtos) >= max_resultados:
                            logger.info("Limite de %d produtos alcançado", max_resultados)
                            return produtos

                    except Exception as e:
                        logger.error("Erro processando card %d: %s", i, e)
                        continue

                logger.info("Página %d: %d produtos coletados", page, produtos_pagina)

                if not self._ir_proxima_pagina():
                    break

                page += 1
                self._rand_sleep()
                continue

            except Exception as e:
                logger.error("Erro na página %d: %s", page, e)
                consecutive_failures += 1
                continue

        logger.info("Total coletado para %r: %d produtos", termo, len(produtos))
        return produtos


    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            self.driver.quit()
        except Exception:
            pass
    
    def _construir_dim_pattern(self, termo: str) -> re.Pattern:
        return _construir_dim_pattern(termo)

    def _detectar_marca(self, texto: str) -> str | None:
        return _detectar_marca(texto)

    def _eh_kit_ou_multiplos(self, titulo: str) -> bool:
        return _eh_kit_ou_multiplos(titulo)



# ───────────────────────── util extras / main ─────────────────────── #
def _slugify(text: str) -> str:
    return re.sub(r"[^\w\-]+", "_", text.lower()).strip("_")


def main():
    parser = argparse.ArgumentParser(description="Amazon Product Scraper")
    parser.add_argument("--termos", nargs="+", required=True, help="Termos de busca")
    parser.add_argument("--headless", action="store_true", default=_HEADLESS_DEF, help="Executar em modo headless")
    parser.add_argument("--proxy", help="Proxy server (formato: http://host:port)")
    parser.add_argument("--max", type=int, help="Máximo de produtos por termo")
    parser.add_argument("--show", action="store_true", help="Exibir resultados no console")
    parser.add_argument("--debug", action="store_true", help="Logs DEBUG + grava HTML/screenshots")
    args = parser.parse_args()

    global logger
    logger = _setup_logger(debug=args.debug)

    resultados: List[Produto] = []
    
    try:
        with AmazonScraper(headless=args.headless, proxy=args.proxy) as bot:
            for termo in args.termos:
                logger.info("🚀 Iniciando busca por: %r", termo)
                produtos_termo = bot.buscar_produtos(termo, args.max)
                resultados.extend(produtos_termo)
                
                if len(args.termos) > 1:
                    logger.info("⏳ Aguardando antes do próximo termo...")
                    time.sleep(random.uniform(10, 20))

        if args.show:
            for p in resultados:
                print(json.dumps(p.to_dict(), ensure_ascii=False, indent=2))
        else:
            now = datetime.now().strftime("%Y%m%d_%H%M%S")
            fname = f"amazon_{_slugify('_'.join(args.termos))}_{now}.json"

            out_dir = Path(__file__).resolve().parent / "data" / "processed" / "amazon"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / fname
            
            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump([p.to_dict() for p in resultados], f, ensure_ascii=False, indent=2)

            logger.info("✅ Arquivo salvo em %s", out_path)
            print(f"✅ Arquivo salvo em {out_path}")
            print(f"📊 Total de produtos coletados: {len(resultados)}")
            
    except KeyboardInterrupt:
        logger.info("❌ Execução interrompida pelo usuário")
    except Exception as e:
        logger.error("❌ Erro fatal: %s", e)
        raise


if __name__ == "__main__":
    main()