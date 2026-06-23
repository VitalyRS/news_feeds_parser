#!/bin/bash
# ============================================================
# Скрипт установки news_collector на сервере (Ubuntu/Oracle Linux)
# Запуск:  bash setup_server.sh
# ============================================================
set -e

APP_DIR="$HOME/news_collector"
VENV_DIR="$APP_DIR/venv"
LOG_DIR="$APP_DIR/logs"

echo "🚀 Установка news_collector..."

# 1. Системные зависимости
echo "📦 Устанавливаю системные пакеты..."
if command -v apt-get &> /dev/null; then
    sudo apt-get update -qq
    sudo apt-get install -y python3 python3-venv python3-pip
elif command -v dnf &> /dev/null; then
    sudo dnf install -y python3 python3-pip
elif command -v yum &> /dev/null; then
    sudo yum install -y python3 python3-pip
fi

# 2. Создаём рабочую директорию
echo "📁 Создаю директорию $APP_DIR ..."
mkdir -p "$APP_DIR"
mkdir -p "$LOG_DIR"

# 3. Копируем файлы (предполагается что скрипт запускается из папки проекта)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
echo "📋 Копирую файлы проекта..."
cp "$SCRIPT_DIR/news_collector.py" "$APP_DIR/"
cp "$SCRIPT_DIR/feeds.json"        "$APP_DIR/"
cp "$SCRIPT_DIR/dead_feeds.txt"    "$APP_DIR/"
cp "$SCRIPT_DIR/requirements.txt"  "$APP_DIR/"

# 4. Виртуальное окружение
echo "🐍 Создаю виртуальное окружение..."
python3 -m venv "$VENV_DIR"
source "$VENV_DIR/bin/activate"
pip install --upgrade pip -q
pip install -r "$APP_DIR/requirements.txt" -q
deactivate

# 5. Создаём обёрточный скрипт для cron
cat > "$APP_DIR/run.sh" << 'RUNEOF'
#!/bin/bash
# Обёртка для cron — активирует venv и запускает парсер
DIR="$(cd "$(dirname "$0")" && pwd)"
source "$DIR/venv/bin/activate"
cd "$DIR"
python3 news_collector.py >> "$DIR/logs/cron.log" 2>&1
deactivate
RUNEOF
chmod +x "$APP_DIR/run.sh"

# 6. Добавляем cron-задачу (каждый час)
CRON_LINE="0 * * * * $APP_DIR/run.sh"
(crontab -l 2>/dev/null | grep -v "news_collector" ; echo "$CRON_LINE") | crontab -
echo "⏰ Cron установлен: каждый час в :00"

# 7. Тестовый запуск
echo ""
echo "🧪 Тестовый запуск..."
"$APP_DIR/run.sh"

echo ""
echo "============================================================"
echo "✅ УСТАНОВКА ЗАВЕРШЕНА!"
echo ""
echo "   Директория:   $APP_DIR"
echo "   БД:           $APP_DIR/baza.db"
echo "   Логи:         $APP_DIR/logs/cron.log"
echo "                 $APP_DIR/news_collector.log"
echo "   Cron:         каждый час в :00"
echo ""
echo "   Полезные команды:"
echo "     crontab -l                    # проверить cron"
echo "     $APP_DIR/run.sh               # запустить вручную"
echo "     sqlite3 $APP_DIR/baza.db      # открыть БД"
echo "     tail -f $APP_DIR/logs/cron.log  # смотреть логи"
echo "============================================================"
