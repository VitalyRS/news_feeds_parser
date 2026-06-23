#!/usr/bin/env python3
"""
Парсер RSS/Atom каналов. 
Автоматически создаёт БД SQLite, защищает от дублей, запускается через cron.
"""

import feedparser
import sqlite3
import requests
import logging
import sys
import os
import json
from datetime import datetime

# ================= КОНФИГУРАЦИЯ =================

# Абсолютный путь к рабочей директории
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Путь к файлу базы данных SQLite
DATABASE_FILE = os.path.join(SCRIPT_DIR, 'feed_parser.db')

# Путь к конфигурационному файлу с фидами
FEEDS_CONFIG_FILE = os.path.join(SCRIPT_DIR, 'feeds.json')

REQUEST_TIMEOUT = 30
# ================================================

LOG_FILE = os.path.join(SCRIPT_DIR, 'feed_parser.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def ensure_database_exists():
    """Создаёт базу данных и таблицу, если их не существует"""
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        
        # Создаём таблицу с защитой от дублей
        # UNIQUE(link) — ключевая часть! Именно она не даёт писать дубли.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS feed_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                link TEXT UNIQUE,
                description TEXT,
                published DATETIME,
                source_feed TEXT,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Индексы для SQLite создаются отдельными командами
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_link ON feed_items (link);')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_published ON feed_items (published);')
        
        conn.commit()
        cursor.close()
        logger.info(f"✅ База данных и таблица 'feed_items' проверены/созданы ({DATABASE_FILE}).")
        
    except sqlite3.Error as e:
        logger.error(f"❌ Ошибка создания БД/таблицы: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()


def get_db_connection():
    """Возвращает подключение к уже созданной БД"""
    try:
        return sqlite3.connect(DATABASE_FILE)
    except sqlite3.Error as e:
        logger.error(f"❌ Не удалось подключиться к БД: {e}")
        sys.exit(1)


def load_feeds_config():
    """Загружает список фидов из JSON-конфига"""
    if not os.path.exists(FEEDS_CONFIG_FILE):
        logger.error(f"❌ Файл конфигурации не найден: {FEEDS_CONFIG_FILE}")
        return [], []
    try:
        with open(FEEDS_CONFIG_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get('rss', []), data.get('atom', [])
    except Exception as e:
        logger.error(f"❌ Ошибка чтения конфига фидов {FEEDS_CONFIG_FILE}: {e}")
        return [], []


def parse_and_save_feeds():
    """Парсит фиды и сохраняет ТОЛЬКО НОВЫЕ записи"""
    rss_feeds, atom_feeds = load_feeds_config()
    all_feeds = rss_feeds + atom_feeds
    if not all_feeds:
        logger.warning("⚠️ Список фидов пуст.")
        return

    new_items_count = 0
    duplicate_count = 0
    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
    except sqlite3.Error as e:
        logger.error(f"❌ Ошибка подключения: {e}")
        sys.exit(1)

    # INSERT OR IGNORE — если ссылка уже есть (UNIQUE), запись молча пропускается
    sql = '''
        INSERT OR IGNORE INTO feed_items (title, link, description, published, source_feed)
        VALUES (?, ?, ?, ?, ?)
    '''

    for feed_url in all_feeds:
        logger.info(f"🌐 Обработка фида: {feed_url}")
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (compatible; PythonFeedParser/1.0;)'
            }
            response = requests.get(feed_url, headers=headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()

            feed = feedparser.parse(response.content)
            if feed.bozo:
                logger.warning(f"⚠️ Предупреждение парсинга: {feed.bozo_exception}")

            for entry in feed.entries:
                title = entry.get('title', 'Без заголовка')
                link = entry.get('link', '').strip()
                
                if not link:
                    continue

                description = entry.get('summary') or entry.get('description') or ''
                if not description and 'content' in entry and len(entry.content) > 0:
                    description = entry.content[0].get('value', '')

                pub_dt = None
                if hasattr(entry, 'published_parsed') and entry.published_parsed:
                    try:
                        # Форматируем дату в ISO 8601 для SQLite
                        pub_dt = datetime(*entry.published_parsed[:6]).isoformat()
                    except (TypeError, ValueError):
                        pass

                try:
                    cursor.execute(sql, (title, link, description, pub_dt, feed_url))
                    
                    if cursor.rowcount > 0:
                        new_items_count += 1
                    else:
                        duplicate_count += 1
                        
                except sqlite3.Error as db_err:
                    logger.error(f"💾 Ошибка БД при обработке {link}: {db_err}")

        except requests.RequestException as req_err:
            logger.error(f"🌍 Сетевая ошибка {feed_url}: {req_err}")
        except Exception as e:
            logger.error(f"💥 Неожиданная ошибка {feed_url}: {e}")

    try:
        conn.commit()
        logger.info(
            f"✅ Цикл завершён. "
            f"Новых статей: {new_items_count} | "
            f"Пропущено дублей: {duplicate_count}"
        )
    except sqlite3.Error as e:
        logger.error(f"💾 Ошибка коммита: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def main():
    """Точка входа для cron"""
    logger.info("🚀 Запуск парсера (cron-запуск)...")
    ensure_database_exists()
    parse_and_save_feeds()
    logger.info("🏁 Завершение работы парсера.")


if __name__ == '__main__':
    main()