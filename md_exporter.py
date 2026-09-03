#!/usr/bin/env python3
"""
Модуль экспорта новостей из SQLite в Markdown структуру:
1. RAW/<Source>/<YYYY>/<MM>/<DD>/<slug_or_id>.md — индивидуальные статьи с YAML-метаданными.
2. NOTEBOOKLM/<YYYY-MM-DD>[_partN].md — подневные агрегированные файлы для прямой загрузки в Google NotebookLM.

Каждый день — отдельный файл (2026-08-20.md, 2026-08-21.md …).
Размер файлов для NotebookLM автоматически ограничен по числу слов
(до 450,000 слов на файл при жёстком лимите Google NotebookLM в 500,000 слов
на один источник). При достижении лимита создаётся следующая часть _partN.
"""

import sqlite3
import os
import re
import sys
import logging
import argparse
from datetime import datetime, date
from urllib.parse import urlparse
from date_filter import build_since_condition

# =================== КОНФИГУРАЦИЯ ===================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB_FILE = os.path.join(SCRIPT_DIR, 'baza.db')
DEFAULT_OUTPUT_DIR = os.path.join(SCRIPT_DIR, 'NewsRAG')
LOG_FILE = os.path.join(SCRIPT_DIR, 'md_exporter.log')

# Лимит одного источника NotebookLM — 500,000 слов. Режем с запасом на 450,000,
# чтобы добавляемая статья гарантированно не перешагнула порог.
MAX_NOTEBOOKLM_WORDS = 450_000
# Байтовый предохранитель на случай источника с очень длинными «словами».
MAX_NOTEBOOKLM_FILE_BYTES = 20_000_000


def _count_words(text: str) -> int:
    """Число слов по разбиению на пробелы — так же считает лимит NotebookLM."""
    return len(text.split())

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding='utf-8')
    ]
)
log = logging.getLogger(__name__)


# =================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ===================

def sanitize_filename(name: str, max_length: int = 80) -> str:
    """Очищает строку для безопасного использования в пути к файлу и Google Drive."""
    if not name:
        return "untitled"
    # Удаляем недопустимые символы и одинарные кавычки
    cleaned = re.sub(r'[\\/*?:"<>|#\'`\n\r\t]', ' ', name).strip()
    # Заменяем повторяющиеся пробелы на один дефис
    cleaned = re.sub(r'\s+', '-', cleaned)
    # Удаляем точки в начале/конце и дефисы
    cleaned = cleaned.strip('.-_')
    if not cleaned:
        return "article"
    return cleaned[:max_length].rstrip('-')


def sanitize_source_name(source: str) -> str:
    """Приводит название источника к красивому и чистому имени папки."""
    if not source:
        return "Unknown"
    # Если источник передан как URL
    if source.startswith('http://') or source.startswith('https://'):
        netloc = urlparse(source).netloc.lstrip('www.')
        # Убираем расширения доменов для краткости (elpais.com -> Elpais)
        parts = netloc.split('.')
        if len(parts) >= 2:
            return parts[0].capitalize()
        return netloc.capitalize()
    
    # Очищаем от недопустимых символов
    clean = re.sub(r'[\\/*?:"<>|\']', '', source).strip()
    return clean or "Unknown"


def parse_article_date(published_str: str | None, scraped_str: str | None) -> datetime:
    """Парсит дату публикации или время скрапинга в объект datetime."""
    if published_str:
        for fmt in (
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M:%S.%f',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%d',
            '%a, %d %b %Y %H:%M:%S %z',
            '%a, %d %b %Y %H:%M:%S %Z',
            '%d/%m/%Y %H:%M:%S',
        ):
            try:
                cleaned = published_str.split('+')[0].strip()
                return datetime.strptime(cleaned, fmt)
            except Exception:
                pass

    if scraped_str:
        for fmt in (
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M:%S.%f',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d',
        ):
            try:
                return datetime.strptime(scraped_str.strip(), fmt)
            except Exception:
                pass

    return datetime.now()


# =================== СХЕМА БД ===================

def init_export_columns(conn: sqlite3.Connection):
    """Добавляет колонки экспорта и саммари в таблицу news, если их нет."""
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(news)")
    columns = [col[1] for col in cursor.fetchall()]

    if 'exported_raw' not in columns:
        cursor.execute("ALTER TABLE news ADD COLUMN exported_raw INTEGER DEFAULT 0")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_news_exp_raw ON news(exported_raw)")

    if 'exported_notebooklm' not in columns:
        cursor.execute("ALTER TABLE news ADD COLUMN exported_notebooklm INTEGER DEFAULT 0")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_news_exp_nlm ON news(exported_notebooklm)")

    if 'llm_summary' not in columns:
        cursor.execute("ALTER TABLE news ADD COLUMN llm_summary TEXT")

    if 'summary_status' not in columns:
        cursor.execute("ALTER TABLE news ADD COLUMN summary_status TEXT DEFAULT 'pending'")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_news_sum_stat ON news(summary_status)")

    conn.commit()


# =================== ЭКСПОРТ RAW ===================

def format_raw_markdown(article_id: int, title: str, source: str, source_type: str,
                        url: str, published: str | None, scraped_at: str | None,
                        full_text: str) -> str:
    """Форматирует одну статью в полноценный Markdown документ с YAML Frontmatter."""
    clean_title = (title or "Untitled").replace('"', '\\"')
    clean_source = (source or "Unknown").replace('"', '\\"')
    pub_val = published or scraped_at or "Unknown"
    scraped_val = scraped_at or "Unknown"

    md = f"""---
id: {article_id}
title: "{clean_title}"
source: "{clean_source}"
source_type: "{source_type}"
url: "{url}"
published: "{pub_val}"
scraped_at: "{scraped_val}"
---

# {title or 'Untitled'}

> **Source:** {source} | **Published:** {pub_val} | **Original Article:** [{url}]({url})

---

{full_text.strip()}
"""
    return md


def export_single_raw(output_dir: str, article_id: int, source: str, title: str,
                      source_type: str, url: str, published: str | None,
                      scraped_at: str | None, full_text: str) -> str:
    """
    Сохраняет статью в структуру:
    NewsRAG/RAW/<Source>/<YYYY>/<MM>/<DD>/<id>_<slug>.md
    Возвращает абсолютный путь к созданному файлу.
    """
    dt = parse_article_date(published, scraped_at)
    source_folder = sanitize_source_name(source)
    year_str = f"{dt.year:04d}"
    month_str = f"{dt.month:02d}"
    day_str = f"{dt.day:02d}"

    target_dir = os.path.join(output_dir, 'RAW', source_folder, year_str, month_str, day_str)
    os.makedirs(target_dir, exist_ok=True)

    slug = sanitize_filename(title)
    filename = f"{article_id}_{slug}.md"
    filepath = os.path.join(target_dir, filename)

    content = format_raw_markdown(
        article_id=article_id,
        title=title,
        source=source,
        source_type=source_type,
        url=url,
        published=published,
        scraped_at=scraped_at,
        full_text=full_text
    )

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

    return filepath


# =================== ЭКСПОРТ NOTEBOOKLM ===================

def append_to_notebooklm_digest(output_dir: str, article_id: int, source: str,
                                title: str, url: str, published: str | None,
                                scraped_at: str | None, full_text: str,
                                llm_summary: str | None = None) -> str | None:
    """
    Добавляет статью в подневный дайджест для NotebookLM (NewsRAG/NOTEBOOKLM/<YYYY-MM-DD>[_partN].md).
    ВАЖНО: В NotebookLM отправляются ТОЛЬКО компактные структурированные саммари (по промпту Испании).
    Статьи группируются в файлы по дню (до 500,000 слов на источник), а при переполнении дня
    создаётся часть _partN. Если саммари отсутствует или вердикт 'SKIP', статья не включается в дайджест.
    """
    if not llm_summary:
        return None

    clean_sum = llm_summary.strip()
    # Ни SKIP (не про Испанию), ни PAYWALL (обрубок за подпиской) не попадают в дайджест.
    if clean_sum in ('SKIP', 'PAYWALL') or clean_sum.startswith(('SKIP\n', 'SKIP ')):
        return None

    dt = parse_article_date(published, scraped_at)
    date_str = f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d}"

    target_dir = os.path.join(output_dir, 'NOTEBOOKLM')
    os.makedirs(target_dir, exist_ok=True)

    entry_content = f"""## [{date_str}] {title}
- **Article ID:** {article_id}
- **Source:** {source}
- **Original URL:** [{url}]({url})

{clean_sum}

---

"""
    entry_words = _count_words(entry_content)
    entry_bytes = len(entry_content.encode('utf-8'))

    # Ищем часть файла месяца, в которую статья влезет, не перешагнув лимит
    # NotebookLM в 500,000 слов (режем с запасом на MAX_NOTEBOOKLM_WORDS).
    # Дополнительно держим байтовый предохранитель. Если текущая часть уже
    # заполнена — переходим к следующей _partN.
    part = 1
    while True:
        suffix = "" if part == 1 else f"_part{part}"
        filepath = os.path.join(target_dir, f"{date_str}{suffix}.md")

        if not os.path.exists(filepath):
            break

        with open(filepath, 'r', encoding='utf-8') as existing:
            existing_text = existing.read()
        # Уже записанная статья? Возвращаем этот же файл (защита от дублей при рестарте).
        if f"- **Article ID:** {article_id}" in existing_text:
            return filepath

        cur_words = _count_words(existing_text)
        cur_bytes = os.path.getsize(filepath)
        if (cur_words + entry_words <= MAX_NOTEBOOKLM_WORDS
                and cur_bytes + entry_bytes < MAX_NOTEBOOKLM_FILE_BYTES):
            break

        part += 1

    # Защита от дублей при рестарте уже выполнена в цикле выбора части выше
    # (статья с этим Article ID найдена бы там и вернула свой файл).
    is_new = not os.path.exists(filepath)

    with open(filepath, 'a', encoding='utf-8') as f:
        if is_new:
            header = f"""# News Intelligence Digest (Spain) — {date_str}{f' (Part {part})' if part > 1 else ''}

*Structured news summaries corpus optimized for NotebookLM analysis (Daily Digest).*

---

"""
            f.write(header)
        f.write(entry_content)

    return filepath


def split_oversized_digests(output_dir: str = DEFAULT_OUTPUT_DIR,
                            max_words: int = MAX_NOTEBOOKLM_WORDS) -> dict:
    """Разрезает уже созданные дайджесты, превысившие лимит слов NotebookLM.

    Режет строго по границам статей (разделитель '---'), сохраняя заголовок
    файла, чтобы ни одна статья не оказалась разорвана между частями.
    Часть 1 остаётся под именем месяца, продолжения уходят в _partN.
    Идемпотентно: файлы в пределах лимита не трогаются.
    """
    target_dir = os.path.join(output_dir, 'NOTEBOOKLM')
    if not os.path.isdir(target_dir):
        return {"checked": 0, "split": 0, "parts_created": 0}

    # Берём только исходные подневные файлы без суффикса _partN — их и режем.
    day_files = sorted(
        f for f in os.listdir(target_dir)
        if re.match(r'^\d{4}-\d{2}-\d{2}\.md$', f)
    )
    checked = 0
    split_files = 0
    parts_created = 0

    for fname in day_files:
        checked += 1
        filepath = os.path.join(target_dir, fname)
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        if _count_words(content) <= max_words:
            continue

        date_str = fname[:-3]  # без '.md'

        # Отделяем заголовок файла от статей. Заголовок — всё до первой статьи '## '.
        first_article = content.find('\n## ')
        if first_article == -1:
            log.warning("⚠️ %s превышает лимит, но статьи не найдены — пропуск", fname)
            continue
        header_block = content[:first_article + 1]  # включая перевод строки

        # Каждая статья заканчивается блоком '---\n\n'. Режем по нему, сохраняя разделители.
        body = content[first_article + 1:]
        raw_entries = re.split(r'(?<=\n---\n\n)', body)
        entries = [e for e in raw_entries if e.strip()]

        # Распределяем статьи по частям, не превышая лимит слов.
        parts: list[list[str]] = [[]]
        header_words = _count_words(header_block)
        cur_words = header_words
        for entry in entries:
            ew = _count_words(entry)
            if parts[-1] and cur_words + ew > max_words:
                parts.append([])
                cur_words = header_words
            parts[-1].append(entry)
            cur_words += ew

        if len(parts) <= 1:
            continue  # после подсчёта уложилось — резать не нужно

        # Записываем части. Part 1 перезаписывает исходный файл.
        for idx, part_entries in enumerate(parts, start=1):
            suffix = "" if idx == 1 else f"_part{idx}"
            part_path = os.path.join(target_dir, f"{date_str}{suffix}.md")
            part_header = (
                f"# News Intelligence Digest (Spain) — {date_str}"
                f"{f' (Part {idx})' if idx > 1 else ''}\n\n"
                "*Structured news summaries corpus optimized for NotebookLM analysis (Daily Digest).*\n\n---\n\n"
            )
            with open(part_path, 'w', encoding='utf-8') as f:
                f.write(part_header)
                f.write("".join(part_entries))
            if idx > 1:
                parts_created += 1

        split_files += 1
        log.info("✂️  %s разрезан на %d частей (было %d слов, лимит %d)",
                 fname, len(parts), _count_words(content), max_words)

    return {"checked": checked, "split": split_files, "parts_created": parts_created}


def export_notebooklm_pending(conn: sqlite3.Connection, output_dir: str = DEFAULT_OUTPUT_DIR,
                              since_date: str | None = None) -> dict:
    """Export all completed, not-yet-exported summaries at a pipeline checkpoint."""
    init_export_columns(conn)
    conditions = [
        "summary_status IN ('done', 'skipped')",
        "COALESCE(exported_notebooklm, 0) = 0",
    ]
    params = []
    if since_date:
        cond, cond_params = build_since_condition(since_date)
        if cond:
            conditions.append(cond)
            params.extend(cond_params)
    rows = conn.execute(
        f"""SELECT id, source, title, link, published, scraped_at, full_text,
                   llm_summary, summary_status
            FROM news WHERE {' AND '.join(conditions)} ORDER BY id ASC""",
        params,
    ).fetchall()
    exported = 0
    skipped = 0
    for art_id, source, title, url, published, scraped_at, full_text, llm_sum, status in rows:
        if status == 'done' and llm_sum:
            path = append_to_notebooklm_digest(
                output_dir, art_id, source, title, url, published, scraped_at,
                full_text, llm_sum)
            if path:
                exported += 1
        else:
            skipped += 1
        conn.execute("UPDATE news SET exported_notebooklm = 1 WHERE id = ?", (art_id,))
        conn.commit()
    if rows:
        log.info("📓 Контрольный экспорт NOTEBOOKLM: добавлено=%d, SKIP отмечено=%d",
                 exported, skipped)
    return {"notebooklm_exported": exported, "skipped": skipped, "processed": len(rows)}


# =================== ОСНОВНОЙ ПАЙПЛАЙН ЭКСПОРТА ===================

def export_news(conn: sqlite3.Connection, output_dir: str = DEFAULT_OUTPUT_DIR,
                limit: int | None = None, since_date: str | None = None,
                re_export: bool = False, skip_raw: bool = True) -> dict:
    """
    Выполняет экспорт скачанных статей из SQLite в Markdown структуру.

    skip_raw: если True (по умолчанию), не создаёт папку RAW — экспортирует только NOTEBOOKLM.

    Возвращает словарь со статистикой:
    {'raw_exported': int, 'notebooklm_exported': int, 'skipped_non_spain': int, 'total_processed': int}
    """
    init_export_columns(conn)

    conditions = ["full_text IS NOT NULL", "length(full_text) > 30"]
    params = []

    if not re_export:
        conditions.append("(exported_raw = 0 OR exported_notebooklm = 0)")

    if since_date:
        cond, cond_params = build_since_condition(since_date)
        if cond:
            conditions.append(cond)
            params.extend(cond_params)

    where_clause = " WHERE " + " AND ".join(conditions)
    query = f"""
        SELECT id, source, source_type, title, link, published, scraped_at, full_text, llm_summary, summary_status
        FROM news
        {where_clause}
        ORDER BY id ASC
    """

    if limit:
        query += f" LIMIT {int(limit)}"

    cursor = conn.cursor()
    cursor.execute(query, params)
    rows = cursor.fetchall()
    cursor.close()

    if not rows:
        log.info("ℹ️ Нет новых статей для экспорта в Markdown.")
        return {'raw_exported': 0, 'notebooklm_exported': 0, 'skipped_non_spain': 0, 'total_processed': 0}

    log.info("🚀 Начинаем экспорт %d статей в Markdown (%s)...", len(rows), output_dir)

    raw_count = 0
    nlm_count = 0
    skipped_count = 0

    for row in rows:
        art_id, source, source_type, title, url, published, scraped_at, full_text, llm_sum, sum_stat = row

        try:
            # 1. Экспорт RAW (пропускается если skip_raw=True)
            if not skip_raw:
                export_single_raw(
                    output_dir=output_dir,
                    article_id=art_id,
                    source=source,
                    title=title,
                    source_type=source_type,
                    url=url,
                    published=published,
                    scraped_at=scraped_at,
                    full_text=full_text
                )
                raw_count += 1

            # 2. Добавление в NotebookLM дайджест (с саммари или пропуском если SKIP)
            digest_file = append_to_notebooklm_digest(
                output_dir=output_dir,
                article_id=art_id,
                source=source,
                title=title,
                url=url,
                published=published,
                scraped_at=scraped_at,
                full_text=full_text,
                llm_summary=llm_sum
            )

            if digest_file is not None:
                nlm_count += 1
            else:
                skipped_count += 1

            # 3. Обновление статуса в БД
            if llm_sum is not None or sum_stat in ('done', 'skipped'):
                conn.execute(
                    "UPDATE news SET exported_raw = 1, exported_notebooklm = 1 WHERE id = ?",
                    (art_id,)
                )
            else:
                conn.execute(
                    "UPDATE news SET exported_raw = 1 WHERE id = ?",
                    (art_id,)
                )

        except Exception as e:
            log.error("❌ Ошибка при экспорте статьи ID=%d (%s): %s", art_id, (title or '')[:40], e)

    conn.commit()
    log.info("✅ Экспорт завершён: RAW=%d, NOTEBOOKLM=%d (пропущено не-Испания=%d)",
             raw_count, nlm_count, skipped_count)

    return {
        'raw_exported': raw_count,
        'notebooklm_exported': nlm_count,
        'skipped_non_spain': skipped_count,
        'total_processed': len(rows)
    }


# =================== CLI ===================

def main():
    parser = argparse.ArgumentParser(description="Экспорт новостей из базы в Markdown (RAW & NotebookLM)")
    parser.add_argument('--db', default=DEFAULT_DB_FILE, help="Путь к файлу базы данных SQLite")
    parser.add_argument('--out', default=DEFAULT_OUTPUT_DIR, help="Целевая директория (по умолчанию NewsRAG)")
    parser.add_argument('--today', action='store_true', help="Экспортировать только статьи за сегодня")
    parser.add_argument('--since', type=str, default='2026-08-20', help="Экспортировать статьи начиная с даты (YYYY-MM-DD или 'today', по умолчанию '2026-08-20')")
    parser.add_argument('--all-time', action='store_true', help="Экспортировать всю базу без фильтра по дате")
    parser.add_argument('--limit', type=int, default=None, help="Ограничить количество экспортируемых статей")
    parser.add_argument('--re-export', action='store_true', help="Переэкспортировать все статьи заново")
    args = parser.parse_args()

    if not os.path.exists(args.db):
        log.error("❌ База данных не найдена: %s", args.db)
        sys.exit(1)

    since_date = args.since
    if args.all_time:
        since_date = None
    elif args.today:
        since_date = 'today'

    with sqlite3.connect(args.db) as conn:
        stats = export_news(
            conn=conn,
            output_dir=args.out,
            limit=args.limit,
            since_date=since_date,
            re_export=args.re_export
        )

    print("\n" + "=" * 50)
    print("📊 ИТОГ ЭКСПОРТА MARKDOWN:")
    print(f"   Обработано статей:       {stats['total_processed']}")
    print(f"   Экспортировано в RAW:    {stats['raw_exported']}")
    print(f"   Добавлено в NOTEBOOKLM:  {stats['notebooklm_exported']}")
    print(f"   Папка выгрузки:          {args.out}")
    print("=" * 50)


if __name__ == '__main__':
    main()
