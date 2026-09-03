#!/usr/bin/env python3
"""
Главный оркестратор пайплайна сбора новостей и подготовки данных для NotebookLM / RAG:

Пайплайн включает 6 фаз:
1. 📡 Фаза 1: Сбор свежих ссылок из RSS/Atom (feeds.json) и прямой скрапинг (dead_feeds.txt) в SQLite.
2. 📥 Фаза 2: Выкачивание полных текстов свежих статей (Googlebot bypass + curl_cffi Chrome + trafilatura + Jina Reader).
3. 🧠 Фаза 3: LLM-классификация и генерация саммари статей про Испанию.
4. 📄 Фаза 4: Экспорт новых статей в Markdown (RAW/<Source>/<Date>/article.md и NOTEBOOKLM/<YYYY-MM>.md).
5. 🧠 Фаза 5: Прямая загрузка сводки в Google NotebookLM (notebooklm-py).
6. ☁️ Фаза 6: Синхронизация папки NewsRAG с Google Drive (Service Account / OAuth).

Запуск:
    python3 main.py                          # Полный быстрый цикл за сегодня
    python3 main.py --all-time               # Обработать все статьи из базы
    python3 main.py --since 2026-08-19       # Обработать статьи начиная с даты
    python3 main.py --skip-drive             # Без синхронизации с Google Drive
    python3 main.py --export-only            # Только экспорт в Markdown и NotebookLM
"""

import os
import sys
import json
import time
import sqlite3
import logging
import argparse
from datetime import datetime, date, timezone

# Импортируем модули пайплайна
from news_collector import (
    DB_FILE,
    init_db as init_news_db,
    parse_rss_feeds,
    scrape_dead_feeds,
    download_full_texts_phase
)
from summarizer import (
    summarize_pending_news,
    init_summary_db,
    ANYMODEL_MODEL
)
from md_exporter import (export_news, export_notebooklm_pending,
                         split_oversized_digests, DEFAULT_OUTPUT_DIR)
from gdrive_sync import sync_to_gdrive
from upload_to_notebooklm import upload_monthly_digest, is_authenticated

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(SCRIPT_DIR, 'pipeline.log')

# Модули пайплайна вызывают logging.basicConfig() на импорте и занимают
# root-логгер первыми, из-за чего basicConfig здесь был бы no-op, а pipeline.log
# оставался пустым. Поэтому настраиваем root-логгер явно и идемпотентно:
# pipeline.log получает вывод всех фаз, логи модулей продолжают работать.
_LOG_FORMAT = '%(asctime)s [%(levelname)s] %(message)s'
_root_logger = logging.getLogger()
_root_logger.setLevel(logging.INFO)

# Гарантируем вывод в stdout (ровно один потоковый хендлер).
if not any(isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
           for h in _root_logger.handlers):
    _sh = logging.StreamHandler(sys.stdout)
    _sh.setFormatter(logging.Formatter(_LOG_FORMAT))
    _root_logger.addHandler(_sh)

# Гарантируем сток в pipeline.log (без дублирующего хендлера при повторном импорте).
if not any(isinstance(h, logging.FileHandler)
           and getattr(h, 'baseFilename', '') == os.path.abspath(LOG_FILE)
           for h in _root_logger.handlers):
    _fh = logging.FileHandler(LOG_FILE, encoding='utf-8')
    _fh.setFormatter(logging.Formatter(_LOG_FORMAT))
    _root_logger.addHandler(_fh)

log = logging.getLogger(__name__)


# Отсечка на самый первый запуск, когда файла состояния ещё нет.
DEFAULT_SINCE_DATE = '2026-08-20'

# Файл состояния хранит UTC-timestamp последнего запуска ('YYYY-MM-DD HH:MM:SS').
# В дефолтном (инкрементальном) режиме скрипт берёт всё начиная с этого момента
# и до текущего — с точностью до часа/минуты. scraped_at в БД тоже в UTC
# (SQLite CURRENT_TIMESTAMP), поэтому сравнение идёт как есть, без конвертаций.
STATE_FILE = os.path.join(SCRIPT_DIR, '.pipeline_state.json')

# Формат timestamp, совпадающий с представлением scraped_at в SQLite.
_TS_FMT = '%Y-%m-%d %H:%M:%S'


def load_last_run_ts() -> str | None:
    """Возвращает UTC-timestamp последнего прогона ('YYYY-MM-DD HH:MM:SS') или None.

    Понимает и старый формат (только дата) для плавной миграции."""
    try:
        with open(STATE_FILE, encoding='utf-8') as f:
            data = json.load(f)
        value = data.get('last_run_ts') or data.get('last_run_date')
        if not isinstance(value, str):
            return None
        # Валидируем формат, чтобы битый файл не сломал SQL-фильтр.
        if len(value) == 10:               # старый формат 'YYYY-MM-DD'
            datetime.strptime(value, '%Y-%m-%d')
            return value
        norm = value.replace('T', ' ')[:19]
        datetime.strptime(norm, _TS_FMT)
        return norm
    except (OSError, ValueError, json.JSONDecodeError):
        pass
    return None


def save_last_run_ts(run_ts: str) -> None:
    """Сохраняет UTC-timestamp прогона как точку отсчёта для следующего инкремента."""
    try:
        with open(STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump({'last_run_ts': run_ts,
                       'updated_at': datetime.now().isoformat(timespec='seconds')},
                      f, ensure_ascii=False, indent=2)
    except OSError as e:
        log.warning("⚠️ Не удалось сохранить состояние в %s: %s", STATE_FILE, e)


def run_pipeline(skip_scrape: bool = False,
                 skip_text: bool = False,
                 skip_summary: bool = False,
                 skip_export: bool = False,
                 skip_notebooklm: bool = False,
                 skip_drive: bool = True,
                 since_date: str | None = DEFAULT_SINCE_DATE,
                 text_limit: int | None = None,
                 summary_limit: int | None = None,
                 text_workers: int = 16,
                 summary_workers: int = 25,
                 scrape_workers: int = 5,
                 model: str = ANYMODEL_MODEL,
                 re_summarize: bool = False,
                 re_export: bool = False,
                 drive_dry_run: bool = False,
                 output_dir: str = DEFAULT_OUTPUT_DIR) -> dict:
    """Выполняет полный цикл пайплайна от сбора до NotebookLM и Google Drive."""
    start_time = datetime.now()
    log.info("=" * 70)
    log.info("🚀 ЗАПУСК НОВОСТНОГО ПАЙПЛАЙНА — %s (фильтр: %s)",
             start_time.strftime('%Y-%m-%d %H:%M:%S'), since_date or 'все даты')
    log.info("=" * 70)

    summary = {
        "rss_new": 0,
        "rss_skipped": 0,
        "scrape_new": 0,
        "texts_downloaded": 0,
        "texts_failed": 0,
        "summaries_created": 0,
        "summaries_skipped": 0,
        "summaries_failed": 0,
        "raw_exported": 0,
        "notebooklm_exported": 0,
        "notebooklm_synced": False,
        "drive_uploaded": 0,
        "drive_updated": 0,
        "drive_skipped": 0,
        "drive_errors": 0,
        "total_articles": 0,
        "total_with_text": 0,
        "total_with_summary": 0,
        "elapsed_seconds": 0.0
    }

    # Инициализация БД
    conn = sqlite3.connect(DB_FILE)
    init_news_db(conn)
    init_summary_db(conn)

    # -------------------------------------------------------------
    # ФАЗА 1: Сбор ссылок (RSS + HTML Скрапинг)
    # -------------------------------------------------------------
    if not skip_scrape:
        log.info("\n📡 === [ФАЗА 1/6] Сбор свежих новостей (RSS & HTML) ===")
        rss_new, rss_skip = parse_rss_feeds(conn)
        scrape_new = scrape_dead_feeds(conn, workers=scrape_workers)
        summary["rss_new"] = rss_new
        summary["rss_skipped"] = rss_skip
        summary["scrape_new"] = scrape_new
    else:
        log.info("\n⏩ === [ФАЗА 1/6] Сбор новостей пропущен (--skip-scrape) ===")

    # -------------------------------------------------------------
    # ФАЗА 2: Скачивание полных текстов статей (Trafilatura + Googlebot)
    # -------------------------------------------------------------
    if not skip_text:
        log.info("\n📥 === [ФАЗА 2/6] Извлечение полных текстов статей (дата >= %s) ===", since_date or 'все')
        dl, fail = download_full_texts_phase(
            conn,
            workers=text_workers,
            limit=text_limit,
            since_date=since_date
        )
        summary["texts_downloaded"] = dl
        summary["texts_failed"] = fail
    else:
        log.info("\n⏩ === [ФАЗА 2/6] Скачивание текстов пропущено (--skip-text) ===")

    # -------------------------------------------------------------
    # ФАЗА 3: Интеллектуальная суммаризация (AnyModel / España Prompt)
    # -------------------------------------------------------------
    if not skip_summary:
        log.info("\n🧠 === [ФАЗА 3/6] Генерация LLM Саммари для NotebookLM (%s) ===", model)
        def checkpoint_export(checkpoint_conn, processed):
            if skip_export:
                return
            stats = export_notebooklm_pending(
                checkpoint_conn, output_dir=output_dir, since_date=since_date)
            summary["notebooklm_exported"] += stats.get("notebooklm_exported", 0)
            log.info("📓 После %d статей NOTEBOOKLM пополнен на %d записей",
                     processed, stats.get("notebooklm_exported", 0))

        sum_stats = summarize_pending_news(
            conn=conn,
            limit=summary_limit,
            workers=summary_workers,
            since_date=since_date,
            model=model,
            force_resummarize=re_summarize,
            checkpoint_callback=checkpoint_export
        )
        summary["summaries_created"] = sum_stats.get("success", 0)
        summary["summaries_skipped"] = sum_stats.get("skipped", 0)
        summary["summaries_failed"] = sum_stats.get("failed", 0)
    else:
        log.info("\n⏩ === [ФАЗА 3/6] Генерация саммари пропущена (--skip-summary) ===")

    # -------------------------------------------------------------
    # ФАЗА 4: Экспорт в Markdown (только NOTEBOOKLM, RAW отключён)
    # -------------------------------------------------------------
    if not skip_export:
        log.info("\n📄 === [ФАЗА 4/6] Генерация Markdown (только NotebookLM, RAW пропущен) ===")
        exp_stats = export_news(
            conn=conn,
            output_dir=output_dir,
            since_date=since_date,
            re_export=re_export,
            skip_raw=True
        )
        summary["raw_exported"] = exp_stats.get("raw_exported", 0)
        summary["notebooklm_exported"] += exp_stats.get("notebooklm_exported", 0)

        # Страховка: режем дайджесты, перешагнувшие лимит NotebookLM (500k слов),
        # на части _partN — прежде чем выгружать их в фазе 5.
        split_stats = split_oversized_digests(output_dir=output_dir)
        if split_stats.get("split"):
            log.info("✂️  Разрезано переполненных дайджестов: %d (создано частей: %d)",
                     split_stats["split"], split_stats["parts_created"])
    else:
        log.info("\n⏩ === [ФАЗА 4/6] Экспорт Markdown пропущен (--skip-export) ===")

    # Собираем общую статистику из БД
    summary["total_articles"] = conn.execute("SELECT COUNT(*) FROM news").fetchone()[0]
    summary["total_with_text"] = conn.execute("SELECT COUNT(*) FROM news WHERE full_text IS NOT NULL AND length(full_text) > 30").fetchone()[0]
    summary["total_with_summary"] = conn.execute("SELECT COUNT(*) FROM news WHERE llm_summary IS NOT NULL AND summary_status = 'done'").fetchone()[0]
    conn.close()

    # -------------------------------------------------------------
    # ФАЗА 5: Прямая загрузка в Google NotebookLM
    # -------------------------------------------------------------
    summary["notebooklm_synced"] = False
    if not skip_notebooklm:
        if is_authenticated():
            log.info("\n📓 === [ФАЗА 5/6] Прямая выгрузка в Google NotebookLM (since=%s) ===", since_date)
            nlm_ok = upload_monthly_digest(since_date=since_date)
            summary["notebooklm_synced"] = nlm_ok
        else:
            log.info("\n⏩ === [ФАЗА 5/6] Прямая выгрузка в NotebookLM пропущена (требуется `notebooklm login`) ===")
    else:
        log.info("\n⏩ === [ФАЗА 5/6] Прямая выгрузка в NotebookLM пропущена (--skip-notebooklm) ===")

    # -------------------------------------------------------------
    # ФАЗА 6: Синхронизация с Google Drive
    # -------------------------------------------------------------
    if not skip_drive:
        log.info("\n☁️ === [ФАЗА 6/6] Синхронизация с Google Drive ===")
        try:
            drive_stats = sync_to_gdrive(local_dir=output_dir, dry_run=drive_dry_run)
            summary["drive_uploaded"] = drive_stats.get("uploaded", 0)
            summary["drive_updated"] = drive_stats.get("updated", 0)
            summary["drive_skipped"] = drive_stats.get("skipped", 0)
            summary["drive_errors"] = drive_stats.get("errors", 0)
        except Exception as e:
            log.warning("⚠️ Синхронизация с Google Drive не удалась: %s", e)
    else:
        log.info("\n⏩ === [ФАЗА 6/6] Google Drive пропущен (--skip-drive) ===")

    elapsed = (datetime.now() - start_time).total_seconds()
    summary["elapsed_seconds"] = elapsed

    # -------------------------------------------------------------
    # ИТОГОВЫЙ ОТЧЕТ
    # -------------------------------------------------------------
    log.info("\n" + "=" * 70)
    log.info("📊 СВОДНЫЙ ОТЧЁТ ПАЙПЛАЙНА:")
    log.info("   📡 Новых ссылок из RSS:       %d", summary["rss_new"])
    log.info("   🕷️  Новых ссылок со скрапинга:  %d", summary["scrape_new"])
    log.info("   📥 Скачано полных текстов:    %d (ошибок/пейволл: %d)", summary["texts_downloaded"], summary["texts_failed"])
    log.info("   🧠 Саммари создано:           %d (пейволл/пропущено: %d, ошибок: %d)", summary["summaries_created"], summary["summaries_skipped"], summary["summaries_failed"])
    log.info("   📁 Экспортировано в RAW .md:   %d", summary["raw_exported"])
    log.info("   📓 Добавлено в NOTEBOOKLM .md: %d", summary["notebooklm_exported"])
    log.info("   📓 Статус Google NotebookLM:   %s", "✅ Выгружено напрямую" if summary["notebooklm_synced"] else "Пропущено")
    if not skip_drive:
        log.info("   ☁️ Google Drive загружено:    %d, обновлено: %d", summary["drive_uploaded"], summary["drive_updated"])
    log.info("   ------------------------------------------------------")
    log.info("   📚 Всего статей в базе данных: %d (с текстом: %d, с саммари: %d)",
             summary["total_articles"], summary["total_with_text"], summary["total_with_summary"])
    log.info("   ⏱️  Время выполнения:          %.1f сек", elapsed)
    log.info("=" * 70 + "\n")

    return summary


def main():
    parser = argparse.ArgumentParser(description="Полный пайплайн сбора и подготовки новостей для NotebookLM / RAG")
    parser.add_argument('--since', type=str, default=None, help="Начальная дата отсечения YYYY-MM-DD (по умолчанию — с даты последнего запуска)")
    parser.add_argument('--today', action='store_true', help="Обрабатывать только статьи за сегодня")
    parser.add_argument('--all-time', action='store_true', help="Обработать всю архивную базу без фильтра по дате")
    parser.add_argument('--skip-scrape', action='store_true', help="Пропустить фазу 1 (сбор ссылок)")
    parser.add_argument('--skip-text', action='store_true', help="Пропустить фазу 2 (скачивание текстов)")
    parser.add_argument('--skip-summary', action='store_true', help="Пропустить фазу 3 (генерация саммари)")
    parser.add_argument('--skip-export', action='store_true', help="Пропустить фазу 4 (экспорт в Markdown)")
    parser.add_argument('--skip-notebooklm', action='store_true', help="Пропустить выгрузку в NotebookLM")
    parser.add_argument('--with-drive', action='store_true', help="Включить синхронизацию с Google Drive (по умолчанию отключена)")
    parser.add_argument('--skip-drive', action='store_true', help="Принудительно пропустить фазу Google Drive")
    parser.add_argument('--export-only', action='store_true', help="Выполнить только экспорт в Markdown и NotebookLM")
    parser.add_argument('--text-limit', type=int, default=None, help="Лимит на скачивание текстов за один прогон")
    parser.add_argument('--summary-limit', type=int, default=None, help="Лимит на суммаризацию статей за один прогон")
    parser.add_argument('--text-workers', type=int, default=16, help="Число потоков для извлечения текстов")
    parser.add_argument('--summary-workers', type=int, default=25, help="Общий пул: до 5 запросов на каждого LLM-провайдера")
    parser.add_argument('--scrape-workers', type=int, default=5, help="Число потоков для скрапинга сайтов")
    parser.add_argument('--model', default=ANYMODEL_MODEL, help=f"LLM модель (по умолчанию {ANYMODEL_MODEL})")
    parser.add_argument('--re-summarize', action='store_true', help="Перегенерировать существующие саммари заново")
    parser.add_argument('--re-export', action='store_true', help="Переэкспортировать все статьи в Markdown заново")
    parser.add_argument('--drive-dry-run', action='store_true', help="Имитация синхронизации с Google Drive")
    parser.add_argument('--output-dir', default=DEFAULT_OUTPUT_DIR, help="Каталог для Markdown файлов")

    args = parser.parse_args()

    # Разрешение отсечки (приоритет сверху вниз):
    #   --all-time                    → без фильтра (вся база)
    #   --today                       → только сегодня
    #   --since YYYY-MM-DD[ HH:MM]    → явная точка от пользователя
    #   иначе (дефолт)                → инкрементально с момента последнего
    #                                   запуска; первый раз — DEFAULT_SINCE_DATE.
    incremental = False
    # Момент СТАРТА этого прогона в UTC (совпадает с зоной scraped_at). Сохраняем
    # именно его, а не конец: статьи, собранные во время прогона, не выпадут из
    # следующего инкремента (максимум — обработаются повторно, что безопасно).
    run_start_ts = datetime.now(timezone.utc).strftime(_TS_FMT)
    if args.all_time:
        since_date = None
    elif args.today:
        since_date = 'today'
    elif args.since:
        since_date = args.since
    else:
        incremental = True
        since_date = load_last_run_ts() or DEFAULT_SINCE_DATE
        log.info("🔄 Инкрементальный режим: обрабатываю статьи с %s UTC (последний запуск)", since_date)

    skip_scrape = args.skip_scrape or args.export_only
    skip_text = args.skip_text or args.export_only
    skip_summary = args.skip_summary or args.export_only
    skip_drive = not args.with_drive or args.skip_drive

    run_pipeline(
        skip_scrape=skip_scrape,
        skip_text=skip_text,
        skip_summary=skip_summary,
        skip_export=args.skip_export,
        skip_notebooklm=args.skip_notebooklm,
        skip_drive=skip_drive,
        since_date=since_date,
        text_limit=args.text_limit,
        summary_limit=args.summary_limit,
        text_workers=args.text_workers,
        summary_workers=args.summary_workers,
        scrape_workers=args.scrape_workers,
        model=args.model,
        re_summarize=args.re_summarize,
        re_export=args.re_export,
        drive_dry_run=args.drive_dry_run,
        output_dir=args.output_dir
    )

    # После успешного инкрементального прогона сдвигаем точку отсчёта на момент
    # СТАРТА этого прогона, чтобы следующий запуск подхватил ровно тот период,
    # что прошёл с этого раза (с точностью до секунды). Явные
    # --since/--today/--all-time точку отсчёта не трогают (разовые прогоны).
    if incremental:
        save_last_run_ts(run_start_ts)


if __name__ == '__main__':
    main()
