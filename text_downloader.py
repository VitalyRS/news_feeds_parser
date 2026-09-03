#!/usr/bin/env python3
"""
Модуль загрузки и извлечения полных текстов статей (Full Text Downloader).
Использует каскадный алгоритм обхода защит (Googlebot bypass + curl_cffi Chrome + trafilatura + Jina Reader).

Поддерживает фильтрацию по дате (только сегодняшние или с указанной даты),
чтобы быстро обрабатывать свежие новости без застревания в архивных ссылках.

Запуск:
    python3 text_downloader.py --today            # Только сегодняшние статьи (быстро)
    python3 text_downloader.py --since 2026-08-19 # С конкретной даты
    python3 text_downloader.py --limit 100        # Ограничить количество
    python3 text_downloader.py --all              # Все статьи без текста из базы
"""

import sqlite3
import logging
import sys
import os
import argparse
import time
import threading
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
from date_filter import build_since_condition

import requests
from requests.adapters import HTTPAdapter
import trafilatura
from curl_cffi import requests as c_requests

# =================== КОНФИГУРАЦИЯ ===================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(SCRIPT_DIR, 'baza.db')
LOG_FILE = os.path.join(SCRIPT_DIR, 'text_downloader.log')

DEFAULT_WORKERS = 16
REQUEST_TIMEOUT = 10   # 10 секунд на попытку, чтобы не зависать
MIN_TEXT_LEN = 120     # минимальная длина извлечённого текста, чтобы считать успехом
COMMIT_EVERY = 50      # периодический commit, чтобы не терять прогресс и не блокировать БД

# Заголовки Googlebot (позволяют получать полный текст статей El País, Cinco Días и др. без пейволлов)
GOOGLEBOT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
}

# Браузерные заголовки
BROWSER_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'cross-site',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
}

GOOGLEBOT_PREFERRED_DOMAINS = {
    'elpais.com', 'cincodias.elpais.com', 'expansion.com', 'elmundo.es', 'abc.es', 'elconfidencial.com'
}

# Пул переиспользуемых HTTP-сессий: держим keep-alive соединения, чтобы не
# переустанавливать TCP+TLS на каждый запрос. По одной сессии на поток.
_thread_local = threading.local()


def _get_requests_session() -> requests.Session:
    """requests.Session для текущего потока с расширенным пулом соединений."""
    sess = getattr(_thread_local, 'req_session', None)
    if sess is None:
        sess = requests.Session()
        adapter = HTTPAdapter(pool_connections=32, pool_maxsize=32, max_retries=0)
        sess.mount('http://', adapter)
        sess.mount('https://', adapter)
        _thread_local.req_session = sess
    return sess


def _get_cffi_session() -> "c_requests.Session":
    """curl_cffi.Session для текущего потока (Chrome TLS fingerprint)."""
    sess = getattr(_thread_local, 'cffi_session', None)
    if sess is None:
        sess = c_requests.Session()
        _thread_local.cffi_session = sess
    return sess


def _domain_of(url: str) -> str:
    """Домен без префикса www. (removeprefix, а не lstrip — lstrip режет символы)."""
    return urlparse(url).netloc.removeprefix('www.')


def _extract(html: str) -> str | None:
    """Извлекает основной текст статьи из HTML с упором на полноту."""
    if not html:
        return None
    text = trafilatura.extract(
        html,
        include_comments=False,
        include_tables=True,
        favor_recall=True,
        output_format='txt',
    )
    if text:
        text = text.strip()
        if len(text) > MIN_TEXT_LEN and 'enable JS' not in text:
            return text
    return None

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


# =================== БАЗА ДАННЫХ ===================
def init_db(conn: sqlite3.Connection | None = None, db_path: str = DB_FILE):
    """Добавляет колонки full_text и download_status в таблицу news, если их нет."""
    should_close = False
    if conn is None:
        conn = sqlite3.connect(db_path)
        should_close = True

    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(news)")
    columns = [col[1] for col in cursor.fetchall()]

    if 'full_text' not in columns:
        log.info("➕ Добавляем колонку 'full_text' в таблицу news...")
        cursor.execute("ALTER TABLE news ADD COLUMN full_text TEXT")
    
    if 'download_status' not in columns:
        log.info("➕ Добавляем колонку 'download_status' в таблицу news...")
        cursor.execute("ALTER TABLE news ADD COLUMN download_status TEXT DEFAULT 'pending'")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_news_status ON news(download_status)")
    
    conn.commit()
    if should_close:
        conn.close()


# =================== КАСКАДНЫЙ ЗАГРУЗЧИК ===================
def fetch_article_text(url: str) -> tuple[str | None, str]:
    """
    Каскадное извлечение полного текста статьи:
      1. Если домен с защитой (El País и др.) -> Googlebot эмуляция
      2. curl_cffi с Chrome TLS fingerprinting
      3. Обычный requests + trafilatura
      4. Fallback через Googlebot
      5. Fallback через Jina Reader (для сложных Cloudflare защит)
    
    Возвращает: (текст_статьи, метод_успеха)
    """
    domain = _domain_of(url)
    req = _get_requests_session()
    is_protected = any(domain == d or domain.endswith('.' + d) for d in GOOGLEBOT_PREFERRED_DOMAINS)

    # 1. Googlebot — только для защищённых доменов (обходит пейволл El País и др.)
    if is_protected:
        try:
            resp = req.get(url, headers=GOOGLEBOT_HEADERS, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                text = _extract(resp.text)
                if text:
                    return text, 'googlebot'
        except Exception:
            pass

    # 2. curl_cffi (Chrome TLS fingerprint) — основной путь для обычных сайтов
    try:
        resp = _get_cffi_session().get(
            url, headers=BROWSER_HEADERS, impersonate='chrome124', timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            text = _extract(resp.text)
            if text:
                return text, 'curl_cffi_chrome'
    except Exception:
        pass

    # 3. Стандартный requests + trafilatura
    try:
        resp = req.get(url, headers=BROWSER_HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            text = _extract(resp.text)
            if text:
                return text, 'trafilatura_direct'
    except Exception:
        pass

    # 4. Googlebot fallback — если ещё не пробовали (незащищённый домен)
    if not is_protected:
        try:
            resp = req.get(url, headers=GOOGLEBOT_HEADERS, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                text = _extract(resp.text)
                if text:
                    return text, 'googlebot_fallback'
        except Exception:
            pass

    # 5. Jina Reader fallback (r.jina.ai) — для тяжёлых Cloudflare/JS защит
    try:
        jina_url = f"https://r.jina.ai/{url}"
        resp = req.get(jina_url, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200 and len(resp.text) > 200:
            lines = resp.text.splitlines()
            clean_lines = [l for l in lines if not l.startswith('Title:') and not l.startswith('URL Source:')]
            clean_text = "\n".join(clean_lines).strip()
            if len(clean_text) > MIN_TEXT_LEN:
                return clean_text, 'jina_reader'
    except Exception:
        pass

    return None, 'failed'


def _worker_process_article(art_id: int, url: str, title: str) -> tuple[int, str, str, str | None, str]:
    """Воркер потока: возвращает (art_id, url, title, text, method)."""
    text, method = fetch_article_text(url)
    return art_id, url, title, text, method


def download_pending_texts(conn: sqlite3.Connection | None = None,
                           limit: int | None = None,
                           workers: int = DEFAULT_WORKERS,
                           since_date: str | None = None,
                           retry_failed: bool = False) -> tuple[int, int]:
    """
    Загружает тексты для статей со статусом pending.
    
    Параметры:
        since_date: Дата в формате 'YYYY-MM-DD' или 'today' (фильтрует по дате сбора/публикации).
        limit: Максимальное количество статей.
        workers: Количество параллельных потоков.
        retry_failed: Повторить попытку для failed статей.
    
    Возвращает: (успешно_скачано, сбоев)
    """
    init_db(conn=conn)
    
    should_close = False
    if conn is None:
        conn = sqlite3.connect(DB_FILE)
        should_close = True

    # Формируем SQL запрос с учетом фильтрации по дате
    conditions = ["(full_text IS NULL OR length(full_text) <= 30)"]
    params = []

    if not retry_failed:
        conditions.append("(download_status IS NULL OR download_status = 'pending')")

    if since_date:
        cond, cond_params = build_since_condition(since_date)
        if cond:
            conditions.append(cond)
            params.extend(cond_params)

    where_clause = " WHERE " + " AND ".join(conditions)
    query = f"SELECT id, link, title FROM news {where_clause} ORDER BY id DESC"

    if limit:
        query += f" LIMIT {int(limit)}"

    cursor = conn.cursor()
    cursor.execute(query, params)
    rows = cursor.fetchall()
    cursor.close()

    if not rows:
        log.info("✅ Все статьи по выбранным критериям уже имеют полный текст.")
        if should_close:
            conn.close()
        return 0, 0

    log.info("📥 Найдено %d статей для загрузки текста (потоков: %d, дата >= %s)...",
             len(rows), workers, since_date or 'все')
    start_time = time.time()

    downloaded = 0
    failed = 0
    total_chars = 0

    # Одно соединение с БД на весь прогон. WAL + периодический commit вместо
    # открытия нового соединения на каждую статью (устраняет «database is locked»
    # и накладные расходы на переоткрытие).
    write_conn = sqlite3.connect(DB_FILE)
    write_conn.execute("PRAGMA journal_mode=WAL")
    write_conn.execute("PRAGMA synchronous=NORMAL")
    processed = 0

    try:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_worker_process_article, r[0], r[1], r[2]): r for r in rows}

            for future in as_completed(futures):
                art_id, url, title, text, method = future.result()

                if text:
                    write_conn.execute(
                        "UPDATE news SET full_text = ?, download_status = 'downloaded' WHERE id = ?",
                        (text, art_id)
                    )
                    downloaded += 1
                    total_chars += len(text)
                    log.info("✅ [%s] %s (%d симв.)", method, (title or url)[:45], len(text))
                else:
                    write_conn.execute(
                        "UPDATE news SET download_status = 'failed' WHERE id = ?",
                        (art_id,)
                    )
                    failed += 1
                    log.warning("⚠️  [Не удалось извлечь] %s → %s", (title or 'No title')[:40], url[:50])

                processed += 1
                if processed % COMMIT_EVERY == 0:
                    write_conn.commit()

        write_conn.commit()
    finally:
        write_conn.close()

    elapsed = time.time() - start_time
    avg_len = total_chars // downloaded if downloaded else 0

    log.info("")
    log.info("=" * 60)
    log.info("📊 ИТОГИ ЗАГРУЗКИ ТЕКСТОВ:")
    log.info("   Успешно скачано:  %d", downloaded)
    log.info("   Не удалось:       %d", failed)
    log.info("   Средняя длина:    %d симв.", avg_len)
    log.info("   Процент успеха:   %.1f%%", (downloaded / len(rows)) * 100 if rows else 0)
    log.info("   Время выполнения: %.1f сек", elapsed)
    log.info("=" * 60)

    if should_close:
        conn.close()

    return downloaded, failed


# =================== CLI ===================
def main():
    parser = argparse.ArgumentParser(description="Загрузка полных текстов статей в baza.db")
    parser.add_argument('--today', action='store_true', help="Скачивать только статьи за сегодня")
    parser.add_argument('--since', type=str, default='2026-08-20', help="Скачивать статьи начиная с даты YYYY-MM-DD (по умолчанию 2026-08-20)")
    parser.add_argument('--limit', type=int, default=None, help="Максимальное количество статей для обработки")
    parser.add_argument('--workers', type=int, default=DEFAULT_WORKERS, help="Количество параллельных потоков")
    parser.add_argument('--all', action='store_true', help="Обработать все доступные статьи без ограничения по дате")
    parser.add_argument('--retry-failed', action='store_true', help="Повторить попытку для ранее сбойных статей")

    args = parser.parse_args()

    since_date = args.since
    if args.all:
        since_date = None
    elif args.today:
        since_date = 'today'

    download_pending_texts(
        limit=args.limit,
        workers=args.workers,
        since_date=since_date,
        retry_failed=args.retry_failed
    )


if __name__ == '__main__':
    main()
