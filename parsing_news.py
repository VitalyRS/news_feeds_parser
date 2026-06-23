#!/usr/bin/env python3
"""
Парсер новостных сайтов из dead_feeds.txt.
Скрапит заголовки и ссылки статей и сохраняет в SQLite.
Запуск: python3 parsing_news.py
"""

import sqlite3
import requests
import logging
import sys
import os
import time
import concurrent.futures
from datetime import datetime
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

# =================== КОНФИГУРАЦИЯ ===================
DB_FILE = os.path.join(os.path.dirname(__file__), 'news.db')
DEAD_FEEDS_FILE = os.path.join(os.path.dirname(__file__), 'dead_feeds.txt')
REQUEST_TIMEOUT = 15
MAX_WORKERS = 5          # Сколько сайтов парсить параллельно
MAX_ARTICLES_PER_SITE = 50  # Максимум статей с одного сайта

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (X11; Linux x86_64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

# =================== ЛОГИРОВАНИЕ ===================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('parsing_news.log', encoding='utf-8'),
    ]
)
log = logging.getLogger(__name__)


# =================== БАЗА ДАННЫХ ===================
def init_db(conn: sqlite3.Connection):
    """Создаём таблицу scraped_news если не существует."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scraped_news (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            source_url  TEXT NOT NULL,
            title       TEXT NOT NULL,
            link        TEXT NOT NULL UNIQUE,
            scraped_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_link ON scraped_news(link)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_source ON scraped_news(source_url)")
    conn.commit()
    log.info("✅ БД инициализирована: %s", DB_FILE)


def save_articles(conn: sqlite3.Connection, source_url: str, articles: list[dict]) -> int:
    """Сохраняет список {title, link} в БД. Возвращает количество новых записей."""
    new_count = 0
    for art in articles:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO scraped_news (source_url, title, link) VALUES (?, ?, ?)",
                (source_url, art['title'], art['link'])
            )
            if conn.total_changes > 0:
                new_count += 1
        except sqlite3.Error as e:
            log.warning("Ошибка вставки [%s]: %s", art.get('link'), e)
    conn.commit()
    return new_count


# =================== УТИЛИТЫ ===================
def normalize_url(base_url: str, href: str) -> str:
    """Преобразует относительные ссылки в абсолютные."""
    if not href:
        return ''
    href = href.strip()
    if href.startswith('//'):
        scheme = urlparse(base_url).scheme
        return f"{scheme}:{href}"
    if href.startswith('http'):
        return href
    return urljoin(base_url, href)


def fetch_page(url: str) -> BeautifulSoup | None:
    """Загружает страницу и возвращает BeautifulSoup объект."""
    try:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT, headers=HEADERS)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, 'html.parser')
    except requests.HTTPError as e:
        log.error("HTTP ошибка %s: %s", url, e)
    except requests.RequestException as e:
        log.error("Ошибка соединения %s: %s", url, e)
    return None


def extract_from_articles(soup: BeautifulSoup, base_url: str,
                           heading_tags=('h2', 'h3'),
                           article_selector='article') -> list[dict]:
    """
    Универсальный экстрактор: ищет <article> → <h2/h3> → <a>.
    Возвращает список {'title': ..., 'link': ...}.
    """
    results = []
    articles = soup.select(article_selector)
    for art in articles:
        heading = art.find(heading_tags)
        if not heading:
            continue
        link_tag = heading.find('a', href=True) or art.find('a', href=True)
        if not link_tag:
            continue
        title = heading.get_text(strip=True)
        link = normalize_url(base_url, link_tag['href'])
        if title and link and link.startswith('http'):
            results.append({'title': title, 'link': link})
    return results


def extract_from_headings(soup: BeautifulSoup, base_url: str,
                           selector='h2 a') -> list[dict]:
    """
    Запасной экстрактор через CSS-селектор типа 'h2 a'.
    """
    results = []
    for tag in soup.select(selector):
        title = tag.get_text(strip=True)
        link = normalize_url(base_url, tag.get('href', ''))
        if title and link and link.startswith('http'):
            results.append({'title': title, 'link': link})
    return results


def deduplicate(articles: list[dict]) -> list[dict]:
    """Убирает дубликаты по ссылке."""
    seen = set()
    unique = []
    for art in articles:
        if art['link'] not in seen:
            seen.add(art['link'])
            unique.append(art)
    return unique


# =================== ПАРСЕРЫ ПО ГРУППАМ САЙТОВ ===================

# --- ГРУППА 1: Prensa Ibérica ---
# Сайты: lne.es, diariocordoba.com, farodevigo.es, elperiodicodearagon.com,
#         diariodemallorca.es, levante-emv.com, laopiniondemurcia.es,
#         diariodeibiza.es, eldia.es, laprovincia.es
# Структура: <article> → <h2> → <a href="/seccion/...">
PRENSA_IBERICA_DOMAINS = {
    'lne.es', 'diariocordoba.com', 'farodevigo.es', 'elperiodicodearagon.com',
    'diariodemallorca.es', 'levante-emv.com', 'laopiniondemurcia.es',
    'diariodeibiza.es', 'eldia.es', 'laprovincia.es',
}

def parse_prensa_iberica(url: str) -> list[dict]:
    soup = fetch_page(url)
    if not soup:
        return []
    return deduplicate(extract_from_articles(soup, url, heading_tags=('h2', 'h3')))


# --- ГРУППА 2: Vocento Group ---
# Сайты: heraldo.es, diariodenavarra.es, eldebate.com
# Структура: <article class="c-article..."> → <h2 class="c-article__title"> → <a>
VOCENTO_LIKE_DOMAINS = {
    'heraldo.es', 'diariodenavarra.es', 'eldebate.com',
}

def parse_vocento_like(url: str) -> list[dict]:
    soup = fetch_page(url)
    if not soup:
        return []
    articles = []
    for art in soup.select('article'):
        h = art.find(['h2', 'h3'])
        if not h:
            continue
        link_tag = h.find('a', href=True) or art.find('a', href=True)
        if not link_tag:
            continue
        title = h.get_text(strip=True)
        link = normalize_url(url, link_tag['href'])
        # diariodenavarra.es возвращает admin-v7 субдомен — исправляем
        if 'admin-v7.diariodenavarra' in link:
            link = link.replace('https://admin-v7.diariodenavarra.es',
                                'https://www.diariodenavarra.es')
        if title and link.startswith('http'):
            articles.append({'title': title, 'link': link})
    return deduplicate(articles)


# --- САЙТЫ С КАСТОМНЫМИ ПАРСЕРАМИ ---

def parse_elmundo(url: str) -> list[dict]:
    """elmundo.es — <article> → <h2> → <a href="https://...">"""
    soup = fetch_page(url)
    if not soup:
        return []
    return deduplicate(extract_from_articles(soup, url, heading_tags=('h2',)))


def parse_larazon(url: str) -> list[dict]:
    """larazon.es — <article class="article..."> → <h2 class="article__title"> → <a>"""
    soup = fetch_page(url)
    if not soup:
        return []
    return deduplicate(extract_from_articles(soup, url, heading_tags=('h2',)))


def parse_rtve(url: str) -> list[dict]:
    """rtve.es/noticias — <article> → <h3> → <a href="https://...">"""
    soup = fetch_page(url)
    if not soup:
        return []
    return deduplicate(extract_from_articles(soup, url, heading_tags=('h3', 'h2')))


def parse_cadenaser(url: str) -> list[dict]:
    """cadenaser.com — <article> → <h2> → <a href="/seccion/...">"""
    soup = fetch_page(url)
    if not soup:
        return []
    return deduplicate(extract_from_articles(soup, url, heading_tags=('h2',)))


def parse_cope(url: str) -> list[dict]:
    """cope.es — <article> → <h2> → <a href="/actualidad/...">"""
    soup = fetch_page(url)
    if not soup:
        return []
    return deduplicate(extract_from_articles(soup, url, heading_tags=('h2',)))


def parse_antena3(url: str) -> list[dict]:
    """antena3.com — <article> → <h3> → <a href="https://...">"""
    soup = fetch_page(url)
    if not soup:
        return []
    return deduplicate(extract_from_articles(soup, url, heading_tags=('h3', 'h2')))


def parse_expansion(url: str) -> list[dict]:
    """expansion.com — <article> → <h2> → <a href="https://...">"""
    soup = fetch_page(url)
    if not soup:
        return []
    return deduplicate(extract_from_articles(soup, url, heading_tags=('h2',)))


def parse_cincodias(url: str) -> list[dict]:
    """cincodias.elpais.com — <article> → <h2> → <a href="https://elpais.com/...">"""
    soup = fetch_page(url)
    if not soup:
        return []
    return deduplicate(extract_from_articles(soup, url, heading_tags=('h2',)))


def parse_vozpopuli(url: str) -> list[dict]:
    """vozpopuli.com — <article> без heading, используем 'h2 a' или 'h3 a'"""
    soup = fetch_page(url)
    if not soup:
        return []
    # Пробуем article → h2/h3 сначала
    results = extract_from_articles(soup, url, heading_tags=('h2', 'h3'))
    if not results:
        results = extract_from_headings(soup, url, 'h2 a') or \
                  extract_from_headings(soup, url, 'h3 a')
    return deduplicate(results)


def parse_infolibre(url: str) -> list[dict]:
    """infolibre.es — нет <article>, используем 'h2 a'"""
    soup = fetch_page(url)
    if not soup:
        return []
    results = extract_from_headings(soup, url, 'h2 a')
    if not results:
        results = extract_from_headings(soup, url, 'h3 a')
    return deduplicate(results)


def parse_publico(url: str) -> list[dict]:
    """publico.es — <article class="m-preview-default"> → <h2 class="title"> → <a>"""
    soup = fetch_page(url)
    if not soup:
        return []
    results = []
    for art in soup.select('article.m-preview-default, article[class*="m-preview"]'):
        h = art.find(['h2', 'h3'])
        if not h:
            continue
        link_tag = h.find('a', href=True) or art.find('a', href=True)
        if not link_tag:
            continue
        title = h.get_text(strip=True)
        link = normalize_url(url, link_tag['href'])
        if title and link.startswith('http'):
            results.append({'title': title, 'link': link})
    if not results:
        results = deduplicate(extract_from_articles(soup, url, heading_tags=('h2', 'h3')))
    return deduplicate(results)


def parse_elplural(url: str) -> list[dict]:
    """elplural.com — нет <article>, используем 'h2 a'"""
    soup = fetch_page(url)
    if not soup:
        return []
    results = extract_from_headings(soup, url, 'h2 a')
    if not results:
        results = extract_from_headings(soup, url, 'h3 a')
    return deduplicate(results)


def parse_eleconomista(url: str) -> list[dict]:
    """eleconomista.es — блокирует (403). Пробуем с другим UA."""
    try:
        headers_alt = dict(HEADERS)
        headers_alt['User-Agent'] = (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0'
        )
        resp = requests.get(url, timeout=REQUEST_TIMEOUT, headers=headers_alt)
        if resp.status_code == 403:
            log.warning("⚠️  %s вернул 403 — сайт блокирует парсеры.", url)
            return []
        soup = BeautifulSoup(resp.text, 'html.parser')
        return deduplicate(extract_from_articles(soup, url, heading_tags=('h2', 'h3')))
    except Exception as e:
        log.error("Ошибка %s: %s", url, e)
        return []


def parse_telecinco(url: str) -> list[dict]:
    """telecinco.es — блокирует (403). Попытка с другим UA."""
    try:
        headers_alt = dict(HEADERS)
        headers_alt['User-Agent'] = (
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
            'AppleWebKit/605.1.15 (KHTML, like Gecko) '
            'Version/17.0 Safari/605.1.15'
        )
        resp = requests.get(url, timeout=REQUEST_TIMEOUT, headers=headers_alt)
        if resp.status_code == 403:
            log.warning("⚠️  %s вернул 403 — сайт блокирует парсеры.", url)
            return []
        soup = BeautifulSoup(resp.text, 'html.parser')
        return deduplicate(extract_from_articles(soup, url, heading_tags=('h2', 'h3')))
    except Exception as e:
        log.error("Ошибка %s: %s", url, e)
        return []


# =================== МАРШРУТИЗАТОР ===================

# Таблица: домен → функция парсинга
PARSER_MAP: dict[str, callable] = {
    # Индивидуальные парсеры
    'elmundo.es':        parse_elmundo,
    'larazon.es':        parse_larazon,
    'rtve.es':           parse_rtve,
    'cadenaser.com':     parse_cadenaser,
    'cope.es':           parse_cope,
    'antena3.com':       parse_antena3,
    'expansion.com':     parse_expansion,
    'cincodias.elpais.com': parse_cincodias,
    'vozpopuli.com':     parse_vozpopuli,
    'infolibre.es':      parse_infolibre,
    'publico.es':        parse_publico,
    'elplural.com':      parse_elplural,
    'eleconomista.es':   parse_eleconomista,
    'telecinco.es':      parse_telecinco,
}


def get_parser(url: str) -> callable:
    """Возвращает подходящую функцию парсинга для URL."""
    domain_full = urlparse(url).netloc.lstrip('www.')
    # Проверяем точное совпадение
    if domain_full in PARSER_MAP:
        return PARSER_MAP[domain_full]
    # Проверяем группу Prensa Ibérica
    if domain_full in PRENSA_IBERICA_DOMAINS:
        return parse_prensa_iberica
    # Проверяем группу Vocento-like
    if domain_full in VOCENTO_LIKE_DOMAINS:
        return parse_vocento_like
    # Универсальный запасной парсер
    log.warning("⚠️  Нет специального парсера для %s — использую универсальный.", domain_full)
    return lambda u: deduplicate(
        extract_from_articles(fetch_page(u) or BeautifulSoup('', 'html.parser'), u)
        or extract_from_headings(fetch_page(u) or BeautifulSoup('', 'html.parser'), u, 'h2 a')
    )


# =================== ОСНОВНАЯ ЛОГИКА ===================

def load_sites(filepath: str) -> list[str]:
    """Читает список URL из файла, пропускает пустые строки и комментарии."""
    sites = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                sites.append(line)
    return sites


def scrape_site(url: str) -> tuple[str, list[dict], str | None]:
    """Парсит один сайт. Возвращает (url, articles, error_msg)."""
    log.info("🔄 Парсинг: %s", url)
    try:
        parser = get_parser(url)
        articles = parser(url)[:MAX_ARTICLES_PER_SITE]
        return url, articles, None
    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        log.error("❌ Ошибка при парсинге %s: %s", url, msg)
        return url, [], msg


def main():
    log.info("🚀 Запуск parsing_news.py")

    # 1. Инициализируем БД
    conn = sqlite3.connect(DB_FILE)
    init_db(conn)

    # 2. Загружаем список сайтов
    if not os.path.exists(DEAD_FEEDS_FILE):
        log.error("❌ Файл не найден: %s", DEAD_FEEDS_FILE)
        sys.exit(1)

    sites = load_sites(DEAD_FEEDS_FILE)
    log.info("📋 Загружено %d сайтов из %s", len(sites), DEAD_FEEDS_FILE)

    # 3. Парсим параллельно
    total_new = 0
    total_found = 0
    errors = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(scrape_site, url): url for url in sites}
        for future in concurrent.futures.as_completed(futures):
            url, articles, error = future.result()
            if error:
                errors.append((url, error))
                continue

            found = len(articles)
            new = save_articles(conn, url, articles)
            total_found += found
            total_new += new
            log.info("✅ %s → найдено: %d, новых в БД: %d", url, found, new)

    conn.close()

    # 4. Итог
    log.info("=" * 60)
    log.info("📊 ИТОГ:")
    log.info("   Сайтов обработано: %d / %d", len(sites) - len(errors), len(sites))
    log.info("   Статей найдено:    %d", total_found)
    log.info("   Новых в БД:        %d", total_new)
    if errors:
        log.warning("   Ошибок:            %d", len(errors))
        for url, err in errors:
            log.warning("     ❌ %s — %s", url, err)
    log.info("   БД:                %s", DB_FILE)
    log.info("=" * 60)


if __name__ == '__main__':
    main()
