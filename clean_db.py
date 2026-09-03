#!/usr/bin/env python3
"""
Разовая чистка baza.db: удаляет full_text из статей с готовым саммари.
После успешной суммаризации full_text больше не нужен — он занимает ~200 МБ.
"""
import sqlite3
import os

DB_FILE = 'baza.db'

if not os.path.exists(DB_FILE):
    print(f"❌ Файл {DB_FILE} не найден")
    exit(1)

print(f"📊 Анализ базы данных {DB_FILE}...")
conn = sqlite3.connect(DB_FILE)
c = conn.cursor()

# Размер до чистки
size_before = os.path.getsize(DB_FILE) / (1024*1024)
print(f"   Размер до чистки: {size_before:.1f} МБ")

# Сколько статей с full_text и готовым саммари
c.execute("SELECT COUNT(*) FROM news WHERE summary_status = 'done' AND full_text IS NOT NULL")
to_clean = c.fetchone()[0]
print(f"   Статей с саммари и full_text: {to_clean:,}")

if to_clean == 0:
    print("✅ Нечего чистить — все full_text уже удалены")
    conn.close()
    exit(0)

# Оценка экономии
c.execute("SELECT SUM(LENGTH(full_text)) FROM news WHERE summary_status = 'done' AND full_text IS NOT NULL")
bytes_to_free = c.fetchone()[0] or 0
mb_to_free = bytes_to_free / (1024*1024)
print(f"   Примерная экономия: ~{mb_to_free:.1f} МБ")

print(f"\n🧹 Удаляю full_text из {to_clean:,} статей с готовым саммари...")
c.execute("UPDATE news SET full_text = NULL WHERE summary_status = 'done' AND full_text IS NOT NULL")
conn.commit()
print(f"✅ Удалено full_text из {c.rowcount:,} статей")

print("\n🗜️  Запускаю VACUUM для сжатия базы...")
c.execute("VACUUM")
conn.commit()
conn.close()

size_after = os.path.getsize(DB_FILE) / (1024*1024)
saved = size_before - size_after
print(f"✅ VACUUM завершён")
print(f"\n📊 Итого:")
print(f"   Размер до:  {size_before:.1f} МБ")
print(f"   Размер после: {size_after:.1f} МБ")
print(f"   Сэкономлено: {saved:.1f} МБ ({saved/size_before*100:.1f}%)")
