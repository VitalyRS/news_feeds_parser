#!/usr/bin/env python3
"""
Очистка базы данных baza.db от отработанного контента.

Логика работы:
- Для статей, которые уже просуммированы и отправлены в NotebookLM (exported_notebooklm = 1):
  1. Удаляет тяжелые полные тексты (full_text = NULL).
  2. Удаляет llm_summary и description (по умолчанию оставляет последние keep_days дней для Telegram-бота).
  3. Удаляет старые оценки из news_scores.
  4. Сохраняет ссылку (link), заголовок и дату в таблице news для надежной защиты от дубликатов.
  5. Запускает VACUUM для возврата дискового пространства операционной системе.
"""

import os
import sys
import sqlite3
import argparse
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
log = logging.getLogger(__name__)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(SCRIPT_DIR, 'baza.db')


def clean_db(db_path: str = DB_FILE, keep_days: int = 0, vacuum: bool = True, dry_run: bool = False) -> dict:
    """
    Очищает тяжелые поля у экспортированных статей в SQLite.
    keep_days: сколько последних дней саммари и скоринга сохранять (0 = чистить все экспортированные).
    """
    if not os.path.exists(db_path):
        log.error("❌ Файл базы %s не найден", db_path)
        return {"error": "db_not_found"}

    size_before = os.path.getsize(db_path) / (1024 * 1024)
    log.info("📊 Размер %s до очистки: %.1f МБ", os.path.basename(db_path), size_before)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # 1. Сколько статей экспортировано
    cur.execute("SELECT COUNT(*) FROM news WHERE exported_notebooklm = 1")
    total_exported = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM news WHERE exported_notebooklm = 1 AND full_text IS NOT NULL")
    with_full_text = cur.fetchone()[0]

    # Условие по дате для llm_summary / scores
    date_filter = ""
    params = []
    if keep_days > 0:
        date_filter = "AND (published < date('now', ?) OR (published IS NULL AND scraped_at < date('now', ?)))"
        params = [f"-{keep_days} days", f"-{keep_days} days"]

    cur.execute(f"SELECT COUNT(*) FROM news WHERE exported_notebooklm = 1 AND llm_summary IS NOT NULL {date_filter}", params)
    with_summary_to_clean = cur.fetchone()[0]

    cur.execute(f"""
        SELECT COUNT(*) FROM news_scores
        WHERE news_id IN (
            SELECT id FROM news WHERE exported_notebooklm = 1 {date_filter}
        )
    """, params)
    scores_to_clean = cur.fetchone()[0]

    log.info("   📌 Всего экспортировано в NotebookLM: %d", total_exported)
    log.info("   📄 full_text для удаления:           %d", with_full_text)
    log.info("   🧠 llm_summary для очистки:          %d (храним последние %d дней)", with_summary_to_clean, keep_days)
    log.info("   🎯 news_scores для удаления:         %d", scores_to_clean)

    if dry_run:
        log.info("🔍 [DRY-RUN] Изменения не применены.")
        conn.close()
        return {
            "size_before_mb": size_before,
            "exported": total_exported,
            "full_text_to_clean": with_full_text,
            "summary_to_clean": with_summary_to_clean,
            "scores_to_clean": scores_to_clean
        }

    # Всегда очищаем full_text для ВСЕХ экспортированных статей (он точно не нужен)
    log.info("🧹 1. Обнуляю full_text у всех экспортированных статей...")
    cur.execute("UPDATE news SET full_text = NULL WHERE exported_notebooklm = 1 AND full_text IS NOT NULL")
    cleaned_texts = cur.rowcount

    # Очищаем llm_summary и description у экспортированных статей старше keep_days
    log.info("🧹 2. Очищаю llm_summary и description у отработанных статей...")
    cur.execute(f"""
        UPDATE news
        SET llm_summary = NULL, description = ''
        WHERE exported_notebooklm = 1 AND (llm_summary IS NOT NULL OR description != '') {date_filter}
    """, params)
    cleaned_summaries = cur.rowcount

    # Удаляем устаревшие оценки из news_scores
    log.info("🧹 3. Удаляю устаревшие оценки из news_scores...")
    cur.execute(f"""
        DELETE FROM news_scores
        WHERE news_id IN (
            SELECT id FROM news WHERE exported_notebooklm = 1 {date_filter}
        )
    """, params)
    cleaned_scores = cur.rowcount

    conn.commit()

    size_after_updates = os.path.getsize(db_path) / (1024 * 1024)

    if vacuum:
        log.info("🗜️  4. Запускаю сжатие базы (VACUUM)... Это может занять несколько секунд...")
        cur.execute("VACUUM")
        conn.commit()

    conn.close()

    size_after = os.path.getsize(db_path) / (1024 * 1024)
    saved = size_before - size_after
    pct = (saved / size_before * 100) if size_before > 0 else 0

    log.info("=" * 60)
    log.info("✅ ОЧИСТКА ЗАВЕРШЕНА:")
    log.info("   Очищено full_text:    %d", cleaned_texts)
    log.info("   Очищено llm_summary:  %d", cleaned_summaries)
    log.info("   Удалено news_scores:  %d", cleaned_scores)
    log.info("   Размер базы ДО:       %.1f МБ", size_before)
    log.info("   Размер базы ПОСЛЕ:    %.1f МБ", size_after)
    log.info("   Освобождено на диске: %.1f МБ (%.1f%%)", saved, pct)
    log.info("=" * 60)

    return {
        "size_before_mb": size_before,
        "size_after_mb": size_after,
        "saved_mb": saved,
        "cleaned_texts": cleaned_texts,
        "cleaned_summaries": cleaned_summaries,
        "cleaned_scores": cleaned_scores
    }


def main():
    parser = argparse.ArgumentParser(description="Очистка baza.db от экспортированного контента")
    parser.add_argument('--keep-days', type=int, default=0,
                        help="Сколько последних дней саммари сохранять для Telegram-бота (по умолчанию 0 = чистить всё)")
    parser.add_argument('--no-vacuum', action='store_true',
                        help="Не выполнять сжатие базы (VACUUM)")
    parser.add_argument('--dry-run', action='store_true',
                        help="Показать объем очистки без внесения изменений")
    parser.add_argument('--db', type=str, default=DB_FILE,
                        help="Путь к файлу базы данных")

    args = parser.parse_args()
    clean_db(
        db_path=args.db,
        keep_days=args.keep_days,
        vacuum=not args.no_vacuum,
        dry_run=args.dry_run
    )


if __name__ == '__main__':
    main()
