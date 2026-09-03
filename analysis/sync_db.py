#!/usr/bin/env python3
"""
Скрипт для синхронизации новостей из облачной базы данных PostgreSQL (Koyeb)
в локальную базу данных SQLite (baza.db).

Запуск:
  python3 sync_db.py "postgresql://user:password@host:port/dbname"
или задав переменную окружения:
  DATABASE_URL="postgresql://user:password@host:port/dbname" python3 sync_db.py
"""

import os
import sys
import sqlite3

def sync():
    # Получаем директорию скрипта
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Загружаем файл .env вручную, если он существует
    env_path = os.path.join(script_dir, ".env")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    os.environ[key.strip()] = val.strip().strip("'\"")

    # 1. Получаем строку подключения к PostgreSQL
    postgres_url = None
    if len(sys.argv) > 1:
        postgres_url = sys.argv[1]
    else:
        postgres_url = os.environ.get("DATABASE_URL")
    
    if not postgres_url:
        print("❌ Ошибка: Не указана строка подключения к PostgreSQL.")
        print("Задайте её в файле .env в папке analysis или запустите:")
        print("  python3 sync_db.py \"postgresql://user:password@host:port/dbname\"")
        sys.exit(1)
        
    # Корректируем формат url при необходимости
    if postgres_url.startswith("postgres://"):
        postgres_url = postgres_url.replace("postgres://", "postgresql://", 1)

    # 2. Определяем путь к локальной базе SQLite (baza.db в папке analysis)
    sqlite_db_path = os.path.abspath(os.path.join(script_dir, "baza.db"))
    
    # 3. Подключение к PostgreSQL
    print("🔌 Подключение к базе данных PostgreSQL...")
    try:
        import psycopg2
        pg_conn = psycopg2.connect(postgres_url)
        pg_cur = pg_conn.cursor()
        
        # Инициализируем таблицу в PostgreSQL, если она отсутствует
        pg_cur.execute("""
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
        pg_cur.execute("CREATE INDEX IF NOT EXISTS idx_news_link ON news(link)")
        pg_conn.commit()
    except ImportError:
        print("❌ Ошибка: Не установлен пакет 'psycopg2'. Установите его командой: pip install psycopg2-binary")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Не удалось подключиться к PostgreSQL или создать таблицу: {e}")
        sys.exit(1)

    # 4. Подключение к SQLite
    print(f"🔌 Подключение к локальной базе SQLite: {sqlite_db_path}")
    try:
        sl_conn = sqlite3.connect(sqlite_db_path)
        sl_cur = sl_conn.cursor()
    except Exception as e:
        print(f"❌ Не удалось подключиться к SQLite: {e}")
        pg_conn.close()
        sys.exit(1)

    # 5. Инициализация таблицы SQLite, если она ещё не создана
    sl_cur.execute("""
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
    sl_cur.execute("CREATE INDEX IF NOT EXISTS idx_news_link ON news(link)")
    sl_conn.commit()

    # 6. Получение списка ссылок из локальной SQLite
    print("🔎 Получение списка локальных ссылок...")
    try:
        sl_cur.execute("SELECT link FROM news")
        local_links = set(r[0] for r in sl_cur.fetchall())
        print(f"✅ В локальной базе найдено {len(local_links)} уникальных ссылок.")
    except Exception as e:
        print(f"❌ Ошибка чтения из SQLite: {e}")
        pg_conn.close()
        sl_conn.close()
        sys.exit(1)

    # 7. Получение списка ссылок из PostgreSQL
    print("🔎 Получение списка удалённых ссылок...")
    try:
        pg_cur.execute("SELECT link FROM news")
        remote_links = set(r[0] for r in pg_cur.fetchall())
        print(f"✅ В удалённой базе найдено {len(remote_links)} уникальных ссылок.")
    except Exception as e:
        print(f"❌ Ошибка чтения из PostgreSQL: {e}")
        pg_conn.close()
        sl_conn.close()
        sys.exit(1)

    # Вычисляем разницу
    new_links = list(remote_links - local_links)
    print(f"🆕 Найдено новых статей для скачивания: {len(new_links)}")

    rows = []
    if new_links:
        print("📥 Загрузка новых новостей из PostgreSQL...")
        chunk_size = 500
        for i in range(0, len(new_links), chunk_size):
            chunk = new_links[i:i + chunk_size]
            placeholders = ', '.join(['%s'] * len(chunk))
            try:
                pg_cur.execute(
                    f"""SELECT source, source_type, title, link, description, published, scraped_at 
                        FROM news
                        WHERE link IN ({placeholders})""",
                    chunk
                )
                rows.extend(pg_cur.fetchall())
            except Exception as e:
                print(f"❌ Ошибка загрузки порции данных из PostgreSQL: {e}")
                pg_conn.close()
                sl_conn.close()
                sys.exit(1)
        print(f"✅ Успешно получено {len(rows)} новых записей из PostgreSQL.")

    if not rows:
        print("📭 Нет новых записей для копирования.")
        pg_conn.close()
        sl_conn.close()
        return

    # 8. Запись новостей в SQLite
    print("📤 Сохранение новостей в локальную базу SQLite...")
    new_inserted = 0
    for row in rows:
        source, source_type, title, link, description, published, scraped_at = row
        
        # Преобразуем datetime в строки для предотвращения DeprecationWarning в Python 3.12+
        if hasattr(published, 'isoformat'):
            published = published.isoformat()
        elif published is not None:
            published = str(published)
            
        if hasattr(scraped_at, 'isoformat'):
            scraped_at = scraped_at.isoformat()
        elif scraped_at is not None:
            scraped_at = str(scraped_at)

        try:
            sl_cur.execute(
                """INSERT OR IGNORE INTO news
                   (source, source_type, title, link, description, published, scraped_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (source, source_type, title, link, description, published, scraped_at)
            )
            if sl_cur.rowcount > 0:
                new_inserted += 1
        except sqlite3.Error as e:
            print(f"⚠️ Ошибка записи статьи [{link}]: {e}")

    sl_conn.commit()
    print("🎉 Синхронизация успешно завершена!")
    print(f"📊 Всего новых записей обработано: {len(rows)}")
    print(f"🆕 Добавлено новых статей в SQLite: {new_inserted}")

    # Закрытие подключений
    pg_cur.close()
    pg_conn.close()
    sl_cur.close()
    sl_conn.close()

if __name__ == "__main__":
    sync()
