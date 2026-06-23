#!/usr/bin/env python3
"""
Объединённый сборщик новостей (Адаптирован для Koyeb и локального запуска).
  Фаза 1 — парсит RSS/Atom ленты из feeds.json (исключая домены из dead_feeds.txt)
  Фаза 2 — скрапит HTML-страницы из dead_feeds.txt напрямую
Всё сохраняется в БД (PostgreSQL на Koyeb или SQLite локально).
При повторном запуске добавляются только новые статьи (дубли пропускаются).

Запуск:  python3 news_collector.py
"""

import sqlite3
import requests
import feedparser
import logging
import sys
import os
import json
import concurrent.futures
from datetime import datetime
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

# =================== КОНФИГУРАЦИЯ ===================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

DB_FILE = os.path.join(SCRIPT_DIR, 'baza.db')
FEEDS_CONFIG_FILE = os.path.join(SCRIPT_DIR, 'feeds.json')
DEAD_FEEDS_FILE = os.path.join(SCRIPT_DIR, 'dead_feeds.txt')
LOG_FILE = os.path.join(SCRIPT_DIR, 'news_collector.log')

REQUEST_TIMEOUT = 20
MAX_WORKERS = 5            # Параллельные потоки для скрапинга сайтов
MAX_ARTICLES_PER_SITE = 50 # Максимум статей с одного сайта при скрапинге

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
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
    ]
)
log = logging.getLogger(__name__)


# ================================================================
#                      БАЗА ДАННЫХ (PostgreSQL / SQLite)
# ================================================================

DATABASE_URL = os.environ.get("DATABASE_URL")
IS_POSTGRES = DATABASE_URL is not None and (DATABASE_URL.startswith("postgres://") or DATABASE_URL.startswith("postgresql://"))

def get_db_connection():
    if IS_POSTGRES:
        import psycopg2
        # Корректируем старый формат URL при необходимости
        db_url = DATABASE_URL
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        return psycopg2.connect(db_url)
    else:
        return sqlite3.connect(DB_FILE)


def init_db(conn):
    """Создаёт таблицу news если не существует."""
    cursor = conn.cursor()
    if IS_POSTGRES:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS news (
                id          SERIAL PRIMARY KEY,
                source      TEXT NOT NULL,
                source_type TEXT NOT NULL,
                title       TEXT NOT NULL,
                link        TEXT NOT NULL UNIQUE,
                description TEXT DEFAULT '',
                published   TIMESTAMP,
                scraped_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_news_link ON news(link)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_news_source ON news(source)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_news_published ON news(published)")
    else:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS news (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                source      TEXT NOT NULL,
                source_type TEXT NOT NULL,
                title       TEXT NOT NULL,
                link        TEXT NOT NULL UNIQUE,
                description TEXT DEFAULT '',
                published   DATETIME,
                scraped_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_news_link ON news(link)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_news_source ON news(source)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_news_published ON news(published)")
    conn.commit()
    cursor.close()
    log.info("✅ БД инициализирована")


def insert_article(cursor, source: str, source_type: str,
                   title: str, link: str, description: str = '',
                   published: str | None = None) -> bool:
    """
    Вставляет одну статью. Возвращает True если новая, False если дубль.
    """
    try:
        if IS_POSTGRES:
            cursor.execute(
                """INSERT INTO news
                   (source, source_type, title, link, description, published)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (link) DO NOTHING""",
                (source, source_type, title, link, description, published)
            )
        else:
            cursor.execute(
                """INSERT OR IGNORE INTO news
                   (source, source_type, title, link, description, published)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (source, source_type, title, link, description, published)
            )
        return cursor.rowcount > 0
    except Exception as e:
        log.warning("Ошибка вставки [%s]: %s", link, e)
        return False


# ================================================================
#                    ФАЗА 1 — RSS/Atom ЛЕНТЫ
# ================================================================

def load_dead_feed_domains() -> set[str]:
    """
    Читает dead_feeds.txt и возвращает множество доменов (без www).
    Эти домены исключаются из RSS-парсинга.
    """
    domains = set()
    if not os.path.exists(DEAD_FEEDS_FILE):
        return domains
    with open(DEAD_FEEDS_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                netloc = urlparse(line).netloc or ''
                domain = netloc.lstrip('www.')
                if domain:
                    domains.add(domain)
    return domains


def is_dead_feed(feed_url: str, dead_domains: set[str]) -> bool:
    """Проверяет, принадлежит ли RSS-ссылка домену из dead_feeds.txt."""
    netloc = urlparse(feed_url).netloc or ''
    domain = netloc.lstrip('www.')
    # Проверяем точное совпадение и суффикс (напр. cincodias.elpais.com → elpais.com)
    for dead in dead_domains:
        if domain == dead or domain.endswith('.' + dead):
            return True
    return False


def parse_rss_feeds(conn) -> tuple[int, int]:
    """
    Парсит RSS/Atom ленты из feeds.json.
    Пропускает домены из dead_feeds.txt.
    Возвращает (new_count, skip_count).
    """
    log.info("=" * 60)
    log.info("📡 ФАЗА 1: Парсинг RSS/Atom лент")
    log.info("=" * 60)

    # Загружаем feeds.json
    if not os.path.exists(FEEDS_CONFIG_FILE):
        log.error("❌ Файл конфигурации не найден: %s", FEEDS_CONFIG_FILE)
        return 0, 0
    try:
        with open(FEEDS_CONFIG_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        log.error("❌ Ошибка чтения %s: %s", FEEDS_CONFIG_FILE, e)
        return 0, 0

    all_feeds = data.get('rss', []) + data.get('atom', [])
    if not all_feeds:
        log.warning("⚠️ Список фидов пуст.")
        return 0, 0

    # Загружаем список мёртвых доменов для исключения
    dead_domains = load_dead_feed_domains()
    log.info("🚫 Исключаемые домены (dead_feeds): %d шт.", len(dead_domains))

    new_count = 0
    dup_count = 0
    skip_count = 0
    cursor = conn.cursor()

    for feed_url in all_feeds:
        # Пропускаем фиды, чей домен есть в dead_feeds.txt
        if is_dead_feed(feed_url, dead_domains):
            log.info("⏭️  Пропуск (dead feed): %s", feed_url)
            skip_count += 1
            continue

        log.info("🌐 RSS: %s", feed_url)
        try:
            resp = requests.get(
                feed_url, headers=HEADERS, timeout=REQUEST_TIMEOUT
            )
            resp.raise_for_status()

            feed = feedparser.parse(resp.content)
            if feed.bozo and not feed.entries:
                log.warning("⚠️  Невалидный фид: %s", feed_url)
                continue

            for entry in feed.entries:
                title = entry.get('title', 'Без заголовка')
                link = entry.get('link', '').strip()
                if not link:
                    continue

                # Описание
                description = (
                    entry.get('summary')
                    or entry.get('description')
                    or ''
                )
                if not description and 'content' in entry and len(entry.content) > 0:
                    description = entry.content[0].get('value', '')

                # Дата публикации
                pub_dt = None
                if hasattr(entry, 'published_parsed') and entry.published_parsed:
                    try:
                        pub_dt = datetime(*entry.published_parsed[:6]).isoformat()
                    except (TypeError, ValueError):
                        pass

                if insert_article(cursor, feed_url, 'rss', title, link,
                                  description, pub_dt):
                    new_count += 1
                else:
                    dup_count += 1

        except requests.RequestException as e:
            log.error("🌍 Сетевая ошибка %s: %s", feed_url, e)
        except Exception as e:
            log.error("💥 Ошибка %s: %s", feed_url, e)

    conn.commit()
    cursor.close()

    log.info("📡 RSS итог: новых=%d, дублей=%d, пропущено=%d",
             new_count, dup_count, skip_count)
    return new_count, skip_count


# ================================================================
#              ФАЗА 2 — СКРАПИНГ САЙТОВ НАПРЯМУЮ
# ================================================================

# ================== УТИЛИТЫ ==================

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
    """Загружает HTML-страницу и возвращает BeautifulSoup."""
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
    """Универсальный экстрактор: <article> → <h2/h3> → <a>."""
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
    """Запасной экстрактор через CSS-селектор 'h2 a'."""
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


# ================== ПАРСЕРЫ ПО ГРУППАМ ==================

# --- ГРУППА 1: Prensa Ibérica ---
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


# --- ГРУППА 2: Vocento-like ---
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
        if 'admin-v7.diariodenavarra' in link:
            link = link.replace('https://admin-v7.diariodenavarra.es',
                                'https://www.diariodenavarra.es')
        if title and link.startswith('http'):
            articles.append({'title': title, 'link': link})
    return deduplicate(articles)


# --- ИНДИВИДУАЛЬНЫЕ ПАРСЕРЫ ---

def parse_elmundo(url: str) -> list[dict]:
    soup = fetch_page(url)
    if not soup:
        return []
    return deduplicate(extract_from_articles(soup, url, heading_tags=('h2',)))


def parse_larazon(url: str) -> list[dict]:
    soup = fetch_page(url)
    if not soup:
        return []
    return deduplicate(extract_from_articles(soup, url, heading_tags=('h2',)))


def parse_rtve(url: str) -> list[dict]:
    soup = fetch_page(url)
    if not soup:
        return []
    return deduplicate(extract_from_articles(soup, url, heading_tags=('h3', 'h2')))


def parse_cadenaser(url: str) -> list[dict]:
    soup = fetch_page(url)
    if not soup:
        return []
    return deduplicate(extract_from_articles(soup, url, heading_tags=('h2',)))


def parse_cope(url: str) -> list[dict]:
    soup = fetch_page(url)
    if not soup:
        return []
    return deduplicate(extract_from_articles(soup, url, heading_tags=('h2',)))


def parse_antena3(url: str) -> list[dict]:
    soup = fetch_page(url)
    if not soup:
        return []
    return deduplicate(extract_from_articles(soup, url, heading_tags=('h3', 'h2')))


def parse_expansion(url: str) -> list[dict]:
    soup = fetch_page(url)
    if not soup:
        return []
    return deduplicate(extract_from_articles(soup, url, heading_tags=('h2',)))


def parse_cincodias(url: str) -> list[dict]:
    soup = fetch_page(url)
    if not soup:
        return []
    return deduplicate(extract_from_articles(soup, url, heading_tags=('h2',)))


def parse_vozpopuli(url: str) -> list[dict]:
    soup = fetch_page(url)
    if not soup:
        return []
    results = extract_from_articles(soup, url, heading_tags=('h2', 'h3'))
    if not results:
        results = extract_from_headings(soup, url, 'h2 a') or \
                  extract_from_headings(soup, url, 'h3 a')
    return deduplicate(results)


def parse_infolibre(url: str) -> list[dict]:
    soup = fetch_page(url)
    if not soup:
        return []
    results = extract_from_headings(soup, url, 'h2 a')
    if not results:
        results = extract_from_headings(soup, url, 'h3 a')
    return deduplicate(results)


def parse_publico(url: str) -> list[dict]:
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
    soup = fetch_page(url)
    if not soup:
        return []
    results = extract_from_headings(soup, url, 'h2 a')
    if not results:
        results = extract_from_headings(soup, url, 'h3 a')
    return deduplicate(results)


def parse_eleconomista(url: str) -> list[dict]:
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


# ================== МАРШРУТИЗАТОР ==================

PARSER_MAP: dict[str, callable] = {
    'elmundo.es':           parse_elmundo,
    'larazon.es':           parse_larazon,
    'rtve.es':              parse_rtve,
    'cadenaser.com':        parse_cadenaser,
    'cope.es':              parse_cope,
    'antena3.com':          parse_antena3,
    'expansion.com':        parse_expansion,
    'cincodias.elpais.com': parse_cincodias,
    'vozpopuli.com':        parse_vozpopuli,
    'infolibre.es':         parse_infolibre,
    'publico.es':           parse_publico,
    'elplural.com':         parse_elplural,
    'eleconomista.es':      parse_eleconomista,
    'telecinco.es':         parse_telecinco,
}


def get_parser(url: str) -> callable:
    """Возвращает подходящую функцию парсинга для URL."""
    domain_full = urlparse(url).netloc.lstrip('www.')
    if domain_full in PARSER_MAP:
        return PARSER_MAP[domain_full]
    if domain_full in PRENSA_IBERICA_DOMAINS:
        return parse_prensa_iberica
    if domain_full in VOCENTO_LIKE_DOMAINS:
        return parse_vocento_like
    log.warning("⚠️  Нет парсера для %s — использую универсальный.", domain_full)
    def _generic(u):
        soup = fetch_page(u)
        if not soup:
            return []
        return deduplicate(
            extract_from_articles(soup, u) or extract_from_headings(soup, u, 'h2 a')
        )
    return _generic


# ================== СКРАПИНГ DEAD FEEDS ==================

def load_sites(filepath: str) -> list[str]:
    """Читает список URL из файла."""
    sites = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                sites.append(line)
    return sites


def scrape_site(url: str) -> tuple[str, list[dict], str | None]:
    """Парсит один сайт. Возвращает (url, articles, error_msg)."""
    log.info("🔄 Скрапинг: %s", url)
    try:
        parser = get_parser(url)
        articles = parser(url)[:MAX_ARTICLES_PER_SITE]
        return url, articles, None
    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        log.error("❌ Ошибка при скрапинге %s: %s", url, msg)
        return url, [], msg


def scrape_dead_feeds(conn) -> int:
    """
    Скрапит все сайты из dead_feeds.txt.
    Возвращает количество новых статей.
    """
    log.info("=" * 60)
    log.info("🕷️  ФАЗА 2: Скрапинг сайтов напрямую (dead_feeds.txt)")
    log.info("=" * 60)

    if not os.path.exists(DEAD_FEEDS_FILE):
        log.warning("⚠️  Файл %s не найден — пропуск фазы 2.", DEAD_FEEDS_FILE)
        return 0

    sites = load_sites(DEAD_FEEDS_FILE)
    log.info("📋 Загружено %d сайтов из %s", len(sites), DEAD_FEEDS_FILE)

    total_new = 0
    total_found = 0
    errors = []
    cursor = conn.cursor()

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(scrape_site, url): url for url in sites}
        for future in concurrent.futures.as_completed(futures):
            url, articles, error = future.result()
            if error:
                errors.append((url, error))
                continue

            found = len(articles)
            new = 0
            for art in articles:
                if insert_article(cursor, url, 'scrape', art['title'], art['link']):
                    new += 1
            conn.commit()

            total_found += found
            total_new += new
            log.info("✅ %s → найдено: %d, новых: %d", url, found, new)

    cursor.close()

    log.info("🕷️  Скрапинг итог: найдено=%d, новых=%d, ошибок=%d",
             total_found, total_new, len(errors))
    if errors:
        for url, err in errors:
            log.warning("  ❌ %s — %s", url, err)
    return total_new


# ================================================================
#                         MAIN
# ================================================================

def main():
    start_time = datetime.now()
    log.info("🚀 Запуск news_collector.py — %s", start_time.strftime('%Y-%m-%d %H:%M:%S'))

    # 1. Инициализируем БД
    conn = get_db_connection()
    init_db(conn)

    # Считаем статьи до запуска
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM news")
    before = cursor.fetchone()[0]
    cursor.close()

    # 2. Фаза 1 — RSS/Atom
    rss_new, rss_skipped = parse_rss_feeds(conn)

    # 3. Фаза 2 — Скрапинг
    scrape_new = scrape_dead_feeds(conn)

    # 4. Итого
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM news")
    after = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    elapsed = (datetime.now() - start_time).total_seconds()

    log.info("")
    log.info("=" * 60)
    log.info("📊 ИТОГО:")
    log.info("   Было в БД:            %d", before)
    log.info("   Новых из RSS:         %d", rss_new)
    log.info("   RSS-фидов пропущено:  %d (dead feeds)", rss_skipped)
    log.info("   Новых из скрапинга:   %d", scrape_new)
    log.info("   Всего в БД сейчас:    %d", after)
    log.info("   Время:                %.1f сек", elapsed)
    if IS_POSTGRES:
        log.info("   БД:                   PostgreSQL (Koyeb)")
    else:
        log.info("   БД:                   %s", DB_FILE)
    log.info("=" * 60)


if __name__ == '__main__':
    main()
