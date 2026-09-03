#!/usr/bin/env bash
# ==============================================================================
# Скрипт автоматического запуска новостного пайплайна для CRON / Systemd Timer
# ==============================================================================

# Директория проекта (абсолютный путь)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

# Поиск интерпретатора Python (виртуальное окружение или системный python3)
if [ -f "$SCRIPT_DIR/venv/bin/python3" ]; then
    PYTHON_EXEC="$SCRIPT_DIR/venv/bin/python3"
elif [ -f "$HOME/anaconda3/bin/python3" ]; then
    PYTHON_EXEC="$HOME/anaconda3/bin/python3"
elif command -v python3 >/dev/null 2>&1; then
    PYTHON_EXEC="$(command -v python3)"
else
    echo "❌ Python 3 не найден!"
    exit 1
fi

LOG_FILE="$SCRIPT_DIR/cron_execution.log"
DATE_STR="$(date '+%Y-%m-%d %H:%M:%S')"

echo "=================================================================" >> "$LOG_FILE"
echo "⏰ [$DATE_STR] Запуск новостного пайплайна через CRON" >> "$LOG_FILE"
echo "=================================================================" >> "$LOG_FILE"

# Запуск основного пайплайна
# Лимит на скачивание 100 полных текстов за прогон, чтобы не перегружать сеть
"$PYTHON_EXEC" "$SCRIPT_DIR/main.py" --text-limit 100 >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

DATE_END="$(date '+%Y-%m-%d %H:%M:%S')"
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ [$DATE_END] Пайплайн успешно завершён." >> "$LOG_FILE"
else
    echo "❌ [$DATE_END] Ошибка выполнения пайплайна (код $EXIT_CODE)." >> "$LOG_FILE"
fi

echo "" >> "$LOG_FILE"
exit $EXIT_CODE
