#!/usr/bin/env python3
"""
Модуль интеллектуальной суммаризации новостей Испании для RAG / NotebookLM.

Использует LLM API (AnyModel / OpenAI-compatible endpoint) с моделью 'am/deepseek-v4-flash'
и специализированным системным промптом на испанском языке.

Функции:
1. Анализирует статьи и структурирует факты, персоны, организации, темы, географию и оценки.
2. Фильтрует нерелевантные новости (не относящиеся к Испании) через вердикт 'SKIP'.
3. Сохраняет структурированное саммари в базу данных SQLite (baza.db).
4. Оптимизировано для NotebookLM и семантического поиска.

Конфигурация (в .env или переменных окружения):
  ANYMODEL_KEY=...             # Ключ доступа к AnyModel API
  ANYMODEL_BASE_URL=https://anymodel.org/v1
  ANYMODEL_MODEL=am/deepseek-v4-flash

Запуск:
  python3 summarizer.py --today                 # Суммаризировать свежие статьи за сегодня
  python3 summarizer.py --since 2026-08-19      # Суммаризировать статьи начиная с даты
  python3 summarizer.py --limit 50              # Обработать до 50 статей
  python3 summarizer.py --workers 5             # Параллельные потоки
"""

import os
import sys
import time
import json
import sqlite3
import logging
import argparse
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from date_filter import build_since_condition

# =================== КОНФИГУРАЦИЯ ===================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(SCRIPT_DIR, 'baza.db')
LOG_FILE = os.path.join(SCRIPT_DIR, 'summarizer.log')

# Загрузка переменных из .env файла
def load_env_file():
    env_paths = [
        os.path.join(SCRIPT_DIR, '.env'),
        os.path.join(SCRIPT_DIR, 'analysis', '.env'),
    ]
    for env_path in env_paths:
        if os.path.exists(env_path):
            with open(env_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        k, v = line.split('=', 1)
                        k = k.strip()
                        v = v.strip().strip("'\"")
                        if k not in os.environ:
                            os.environ[k] = v

load_env_file()

ANYMODEL_KEY = os.environ.get('ANYMODEL_KEY') or os.environ.get('OPENAI_API_KEY') or os.environ.get('LLM_API_KEY')
ANYMODEL_BASE_URL = os.environ.get('ANYMODEL_BASE_URL', 'https://anymodel.org/v1').rstrip('/')
# Use the explicitly configured AnyModel model.  Do not let GEMINI_MODEL from
# an old shell environment silently select a paid model.
ANYMODEL_MODEL = (
    os.environ.get('ANYMODEL_MODEL')
    or os.environ.get('LLM_MODEL', 'am/deepseek-v4-flash')
).strip()
# Keep the provider's free catalogue in one place for selection and fallback.
# Приоритетно берем ANYMODELS_MODELS, если задан в .env
ANYMODEL_FREE_MODELS = tuple(
    m.strip() for m in (os.environ.get('ANYMODELS_MODELS') or os.environ.get('ANYMODEL_FREE_MODELS', '')).split(',') if m.strip()
)
ANYMODEL_FALLBACK_MODEL = os.environ.get(
    'ANYMODEL_FALLBACK_MODEL',
    ANYMODEL_FREE_MODELS[0] if ANYMODEL_FREE_MODELS else 'am/deepseek-v4-flash',
).strip()
MISTRAL_API_KEY = os.environ.get('MISTRAL_API_KEY', '').strip()
MISTRAL_BASE_URL = os.environ.get('MISTRAL_BASE_URL', 'https://api.mistral.ai/v1').rstrip('/')
MISTRAL_MODEL = os.environ.get('MISTRAL_MODEL', 'mistral-small-latest').strip()
# Прямой Google Gemini API в summarizer отключен: GEMINI_API_KEY в .env используется ТОЛЬКО для генерации картинок в telegram_bot.py
GEMINI_API_KEY = ""
GEMINI_BASE_URL = os.environ.get(
    'GEMINI_BASE_URL', 'https://generativelanguage.googleapis.com/v1beta'
).rstrip('/')
GEMINI_MODEL = ""
GEMINI_MODELS = ()
OPENROUTER_API_KEY = os.environ.get('OPENROUTER_API_KEY', '').strip()
OPENROUTER_BASE_URL = os.environ.get('OPENROUTER_BASE_URL', 'https://openrouter.ai/api/v1').rstrip('/')
OPENROUTER_MODEL = os.environ.get('OPENROUTER_MODEL', 'openrouter/free').strip()
CEREBRAS_API_KEY = os.environ.get('CEREBRAS_API_KEY', '').strip()
CEREBRAS_BASE_URL = os.environ.get('CEREBRAS_BASE_URL', 'https://api.cerebras.ai/v1').rstrip('/')
CEREBRAS_MODEL = os.environ.get('CEREBRAS_MODEL', 'gemma-4-31b').strip()

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_WORKERS = 8
# (connect, read). Длинные статьи требуют времени на генерацию: 12с рвало
# ответ и сыпало статьи в error. Даём модели дочитать, а ротация подхватит
# только реально зависшие эндпоинты.
REQUEST_TIMEOUT = (
    float(os.environ.get('ANYMODEL_CONNECT_TIMEOUT', '5')),
    float(os.environ.get('ANYMODEL_READ_TIMEOUT', '45')),
)
MAX_RETRIES = max(1, int(os.environ.get('ANYMODEL_MAX_RETRIES', '4')))
CLASSIFICATION_BATCH_SIZE = max(1, int(os.environ.get('CLASSIFICATION_BATCH_SIZE', '10')))
SUMMARY_BATCH_SIZE = max(1, int(os.environ.get('SUMMARY_BATCH_SIZE', '3')))
API_MAX_CONCURRENT = max(1, int(os.environ.get('ANYMODEL_MAX_CONCURRENT', '2')))
API_MIN_INTERVAL = max(0.0, float(os.environ.get('ANYMODEL_MIN_INTERVAL', '1.5')))
PROVIDER_MAX_CONCURRENT = max(1, int(os.environ.get('PROVIDER_MAX_CONCURRENT', '5')))
MISTRAL_MAX_CONCURRENT = max(1, int(os.environ.get('MISTRAL_MAX_CONCURRENT', '1')))
MISTRAL_MIN_INTERVAL = max(0.0, float(os.environ.get('MISTRAL_MIN_INTERVAL', '1.5')))
GEMINI_MAX_CONCURRENT = max(1, int(os.environ.get('GEMINI_MAX_CONCURRENT', '5')))
# По умолчанию перебираем ВЕСЬ каталог бесплатных моделей, а не первые 5:
# таймаут/отказ одной модели не должен ронять статью в error, пока есть
# непробованные. Переопределяемо через ANYMODEL_MODEL_ATTEMPTS.
ANYMODEL_MODEL_ATTEMPTS = max(
    1,
    int(os.environ.get('ANYMODEL_MODEL_ATTEMPTS', str(len(ANYMODEL_FREE_MODELS) or 5)))
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding='utf-8')
    ]
)
log = logging.getLogger(__name__)

# Пул постоянных HTTP-сессий для параллельных потоков с Keep-Alive
_HTTP_SESSION = None
_API_SEMAPHORE = threading.BoundedSemaphore(API_MAX_CONCURRENT)
_MISTRAL_SEMAPHORE = threading.BoundedSemaphore(MISTRAL_MAX_CONCURRENT)
_GEMINI_SEMAPHORE = threading.BoundedSemaphore(GEMINI_MAX_CONCURRENT)
_OPENROUTER_SEMAPHORE = threading.BoundedSemaphore(PROVIDER_MAX_CONCURRENT)
_CEREBRAS_SEMAPHORE = threading.BoundedSemaphore(PROVIDER_MAX_CONCURRENT)
_API_RATE_LOCK = threading.Lock()
_API_NEXT_REQUEST_AT = 0.0
_API_COOLDOWN_UNTIL = 0.0
_MISTRAL_RATE_LOCK = threading.Lock()
_MISTRAL_NEXT_REQUEST_AT = 0.0
_MISTRAL_COOLDOWN_UNTIL = 0.0
_UNAVAILABLE_MODELS: set[str] = set()
_MODEL_STATE_LOCK = threading.Lock()
_CEREBRAS_DISABLED = False
_CLASSIFIER_ROUTE_INDEX = 0
_CLASSIFIER_ROUTE_LOCK = threading.Lock()
_ANYMODEL_ROUTE_INDEX = 0
_GEMINI_MODEL_INDEX = 0
_GEMINI_STATE_LOCK = threading.Lock()
_GEMINI_COOLDOWN_UNTIL = 0.0
_GEMINI_SUCCESS_LOGGED = False
_MISTRAL_SUCCESS_LOGGED = False
_OPENROUTER_SUCCESS_LOGGED = False
_CEREBRAS_SUCCESS_LOGGED = False
_ANYMODEL_SUCCESS_MODELS: set[str] = set()


def _wait_for_api_slot():
    """Apply a process-wide request interval and respect global cooldowns."""
    global _API_NEXT_REQUEST_AT
    while True:
        with _API_RATE_LOCK:
            now = time.monotonic()
            if now < _API_COOLDOWN_UNTIL:
                wait_cooldown = _API_COOLDOWN_UNTIL - now
            else:
                wait_cooldown = 0.0
                wait_for = max(0.0, _API_NEXT_REQUEST_AT - now)
                _API_NEXT_REQUEST_AT = max(now, _API_NEXT_REQUEST_AT) + API_MIN_INTERVAL
                break
        if wait_cooldown > 0:
            time.sleep(min(wait_cooldown, 2.0))
    if wait_for > 0:
        time.sleep(wait_for)
        # Если во время ожидания слота другой поток словил 429, дожидаемся окончания кулдауна
        while True:
            with _API_RATE_LOCK:
                now = time.monotonic()
                rem = _API_COOLDOWN_UNTIL - now
            if rem <= 0:
                break
            time.sleep(min(rem, 2.0))


def _apply_api_cooldown(seconds: float):
    """Pause every worker after a rate-limit response instead of retrying in parallel."""
    global _API_NEXT_REQUEST_AT, _API_COOLDOWN_UNTIL
    with _API_RATE_LOCK:
        until = time.monotonic() + seconds
        _API_COOLDOWN_UNTIL = max(_API_COOLDOWN_UNTIL, until)
        _API_NEXT_REQUEST_AT = max(_API_NEXT_REQUEST_AT, _API_COOLDOWN_UNTIL)


def _wait_for_mistral_slot():
    """Apply a process-wide request interval for Mistral API to avoid HTTP 429."""
    global _MISTRAL_NEXT_REQUEST_AT
    while True:
        with _MISTRAL_RATE_LOCK:
            now = time.monotonic()
            if now < _MISTRAL_COOLDOWN_UNTIL:
                wait_cooldown = _MISTRAL_COOLDOWN_UNTIL - now
            else:
                wait_cooldown = 0.0
                wait_for = max(0.0, _MISTRAL_NEXT_REQUEST_AT - now)
                _MISTRAL_NEXT_REQUEST_AT = max(now, _MISTRAL_NEXT_REQUEST_AT) + MISTRAL_MIN_INTERVAL
                break
        if wait_cooldown > 0:
            time.sleep(min(wait_cooldown, 2.0))
    if wait_for > 0:
        time.sleep(wait_for)


def _apply_mistral_cooldown(seconds: float):
    """Pause Mistral workers after HTTP 429."""
    global _MISTRAL_NEXT_REQUEST_AT, _MISTRAL_COOLDOWN_UNTIL
    with _MISTRAL_RATE_LOCK:
        until = time.monotonic() + seconds
        _MISTRAL_COOLDOWN_UNTIL = max(_MISTRAL_COOLDOWN_UNTIL, until)
        _MISTRAL_NEXT_REQUEST_AT = max(_MISTRAL_NEXT_REQUEST_AT, _MISTRAL_COOLDOWN_UNTIL)

def get_http_session() -> requests.Session:
    """Создаёт и возвращает переиспользуемую HTTP-сессию с пулом соединений."""
    global _HTTP_SESSION
    if _HTTP_SESSION is None:
        session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=30,
            pool_maxsize=30,
            max_retries=Retry(total=1, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
        )
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        _HTTP_SESSION = session
    return _HTTP_SESSION


# =================== СИСТЕМНЫЙ ПРОМПТ ДЛЯ ESPAÑA RAG ===================

SPAIN_NEWS_SYSTEM_PROMPT = """# SISTEMA DE PROCESAMIENTO DE NOTICIAS — RAG / NotebookLM

Eres un sistema especializado en estructurar noticias españolas para RAG / NotebookLM. TODO EL CONTENIDO EN ESPAÑOL.

OBJETIVO: resumen factual y compacto (máx. 250 palabras) conservando la máxima información útil para RAG. NO reproduzcas el artículo.

Prioridad: Hechos > Personas > Organizaciones > Acciones/Decisiones > Fechas > Números > Lugares > Causas/Consecuencias.
Elimina: introducciones periodísticas, frases de relleno, repeticiones, opiniones del periodista, citas largas.

DATOS OBLIGATORIOS A CONSERVAR (si aparecen): nombres completos, partidos políticos, organizaciones, empresas, instituciones, organismos públicos, leyes, operaciones policiales, casos judiciales, ciudades, comunidades autónomas, fechas, cantidades, porcentajes, cifras económicas, plazos.
No sustituyas nombres concretos por términos genéricos. Usa "Pedro Sánchez, presidente del Gobierno" — no "el presidente".

HECHOS VS. DECLARACIONES: distingue estrictamente. Si alguien afirma algo, indícalo: "Pedro Sánchez afirmó que...", "El PP acusó al Gobierno de...". No presentes acusaciones como hechos confirmados.

CAMPOS DE SALIDA:
- PERSONAS: Nombre completo — cargo — relación con el hecho. Si no hay: —
- ORGANIZACIONES: partidos, Gobierno, ministerios, tribunales, policía, empresas, sindicatos, ONG. Conserva nombres originales y siglas. Si no hay: —
- POLÍTICAS/MEDIDAS: leyes, reformas, proyectos de ley, decisiones, programas, subvenciones, impuestos, cambios regulatorios, sanciones, acuerdos. Si no hay: —
- HECHOS CLAVE: 3–6 hechos independientes con datos concretos. Prioriza cifras, porcentajes, fechas, personas afectadas, condenas, plazos.
- CAUSA / CONSECUENCIAS: solo si constan en el artículo. No inventes. Si no hay: —
- UBICACIÓN: comunidad autónoma, provincia y/o ciudad directamente implicada. No añadas ubicaciones secundarias.
- IMPORTANCIA (1–5): 1=local menor, 2=ordinaria, 3=relevante, 4=amplio interés público, 5=nacional/crisis/escándalo mayor.
- INTERÉS (1–5): 1=poco, 2=moderado, 3=interesante, 4=muy interesante, 5=excepcional/polémico/impactante. No subas por titular clickbait.

TIPO (selecciona los que apliquen):
Noticia, Escándalo, Escándalo político, Escándalo de corrupción, Investigación, Investigación policial, Investigación judicial, Revelación, Filtración, Acusación, Arresto, Detención, Juicio, Sentencia, Condena, Dimisión, Nombramiento, Elecciones, Protesta, Manifestación, Huelga, Ley, Reforma, Decisión política, Acuerdo, Conflicto político, Conflicto social, Delito, Asesinato, Desaparición, Secuestro, Robo, Fraude, Corrupción, Narcotráfico, Tráfico de personas, Delito sexual, Violencia, Accidente, Catástrofe, Incendio, Inundación, Temporal, Evacuación, Operación de rescate, Descubrimiento, Descubrimiento científico, Acontecimiento histórico, Investigación histórica, Caso insólito, Historia sorprendente, Historia impactante, Historia viral, Curiosidad, Tragedia, Historia humana, Heroísmo, Gran repercusión pública

TEMAS (selecciona los que apliquen):
Política, Gobierno, Oposición, Elecciones, Partidos políticos, Parlamento, Monarquía, Familia Real, Unión Europea, Relaciones internacionales, Corrupción, Criminalidad, Crimen organizado, Narcotráfico, Contrabando, Tráfico de personas, Delitos sexuales, Violencia, Violencia de género, Violencia doméstica, Delitos contra menores, Ciberdelincuencia, Policía, Justicia, Tribunales, Inmigración, Migración, Integración, Problemas sociales, Pobreza, Desigualdad, Personas sin hogar, Derechos humanos, Discriminación, Seguridad, Economía, Finanzas, Empresas, Negocios, Startups, Vivienda, Alquiler, Inmobiliario, Inflación, Precios, Impuestos, Salarios, Pensiones, Desempleo, Mercado laboral, Turismo, Transporte, Energía, Sanidad, Salud pública, Medicina, Educación, Universidades, Ciencia, Tecnología, Inteligencia artificial, Medio ambiente, Cambio climático, Agricultura, Pesca, Familia, Niños, Jóvenes, Personas mayores, Mujeres, Cultura, Cine, Televisión, Música, Literatura, Teatro, Arte, Museos, Tradiciones, Religión, Historia, Guerra Civil, Franquismo, Memoria histórica, Arqueología, Patrimonio histórico, Deporte, Fútbol, La Liga

REGLAS ESPECIALES:
- Crimen: conserva tipo de delito, lugar, fecha, víctimas, detenidos, estado investigación, tribunal, condena.
- Política: conserva políticos, partidos, instituciones, decisiones, leyes, votaciones, alianzas, conflictos, acusaciones, fechas.
- Economía: conserva cantidades, euros, porcentajes, salarios, impuestos, precios, inflación, empleo, previsiones, plazos.
- Historia: conserva periodo histórico, fecha, lugar, personajes, acontecimiento, contexto, descubrimiento.

FORMATO DE SALIDA (usa EXACTAMENTE esta estructura):
TÍTULO: [título breve e informativo]
FECHA: [fecha o —]
FUENTE: [medio o —]
RESUMEN:
[2–3 frases]
HECHOS CLAVE:
- ...
PERSONAS:
- Nombre — cargo — relación
ORGANIZACIONES:
- ...
POLÍTICAS/MEDIDAS:
- ...
CAUSA:
[... o —]
CONSECUENCIAS:
[... o —]
TIPO:
[tag1, tag2]
TEMAS:
[tag1, tag2]
UBICACIÓN:
[comunidad autónoma, ciudad]
IMPORTANCIA:
[1–5]
INTERÉS:
[1–5]

REGLAS FINALES: No inventes información. No repitas. No hagas análisis político propio. No expreses opiniones. No presentes acusaciones como hechos. Usa "—" si no hay datos para un campo. Prioriza precisión sobre cantidad de etiquetas. "Historia sorprendente", "Caso insólito", "Curiosidad" solo cuando esté realmente justificado."""


# =================== СХЕМА БД И КЭША ===================

import hashlib

def init_summary_db(conn: sqlite3.Connection):
    """Инициализирует колонки llm_summary, summary_status и таблицу локального кэша."""
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(news)")
    columns = [col[1] for col in cursor.fetchall()]

    if 'llm_summary' not in columns:
        log.info("➕ Добавляем колонку 'llm_summary' в таблицу news...")
        cursor.execute("ALTER TABLE news ADD COLUMN llm_summary TEXT")

    if 'summary_status' not in columns:
        log.info("➕ Добавляем колонку 'summary_status' в таблицу news...")
        cursor.execute("ALTER TABLE news ADD COLUMN summary_status TEXT DEFAULT 'pending'")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_news_sum_stat ON news(summary_status)")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS llm_cache (
            content_hash     TEXT PRIMARY KEY,
            model            TEXT NOT NULL,
            summary          TEXT NOT NULL,
            status           TEXT NOT NULL,
            prompt_tokens    INTEGER DEFAULT 0,
            cached_tokens    INTEGER DEFAULT 0,
            completion_tokens INTEGER DEFAULT 0,
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_llm_cache_hash ON llm_cache(content_hash)")
    conn.commit()

# =================== БЫСТРЫЙ КЛАССИФИКАТОР "ТОЛЬКО ИСПАНИЯ" ===================

import re

# Стоп-слова очевидного спама/скидок/купонов (0 токенов, мгновенный SKIP)
COMMERCIAL_PATTERNS = re.compile(
    r'\b(código descuento|cupón descuento|cupones|descuentos|chollos|ofertas flash|rebajas|'
    r'código promocional|cashback|cupon temu|cupon ebay|descuento amazon|descuento adidas|'
    r'descuento leroy|descuento pandora|descuento xiaomi|descuento barceló|descuento emma)\b',
    re.IGNORECASE
)

# Маркеры Испании (регионы, города, институты, ключевые политики)
SPAIN_POSITIVE_PATTERNS = re.compile(
    r'\b(españa|español|española|españoles|españolas|madrid|barcelona|valencia|sevilla|'
    r'zaragoza|málaga|murcia|palma|bilbao|alicante|córdoba|valladolid|vigo|gijón|a coruña|'
    r'granada|vitoria|oviedo|santander|pamplona|almería|san sebastián|burgos|salamanca|'
    r'huelva|logroño|badajoz|tenerife|gran canaria|canarias|baleares|galicia|andalucía|'
    r'cataluña|catalunya|euskadi|país vasco|castilla y león|castilla-la mancha|extremadura|'
    r'asturias|cantabria|navarra|la rioja|ceuta|melilla|moncloa|cortes generales|'
    r'congreso de los diputados|senado|guardia civil|policía nacional|dgt|aemet|renfe|'
    r'pedro sánchez|feijóo|abascal|yolanda díaz|junta de andalucía|generalitat|'
    r'comunidad de madrid|gobierno de españa|ibex|laliga|liga f|copa del rey)\b',
    re.IGNORECASE
)

# Международные новости без отношения к Испании
PURE_INTERNATIONAL_PATTERNS = re.compile(
    r'\b(putin|zelenski|ucrania|gaza|netanyahu|hamás|hezbolá|trump|biden|kamala|'
    r'corea del norte|evergrande|china condena|rusia lanza|kremlin|taiwan|venezuela maduro|'
    r'milei argentina|bukele el salvador|pentágono|casa blanca)\b',
    re.IGNORECASE
)

# Маркеры начала пейволл-хвоста: реальный текст статьи заканчивается ДО них,
# дальше идёт только приглашение оформить подписку.
PAYWALL_MARKERS = re.compile(
    r'(contenido exclusivo para suscriptores|suscríbete para seguir leyendo|'
    r'exclusivo suscriptores|suscríbete y accede|para seguir leyendo)',
    re.IGNORECASE
)

# Минимальная длина реального тела (без пейволл-хвоста), при которой есть смысл
# суммаризировать. Ниже — обрубок за пейволлом, саммари получится пустым.
MIN_BODY_LEN = 350


def effective_body(full_text: str | None) -> tuple[str, int]:
    """Отрезает пейволл-хвост и возвращает (тело, длина_без_лишних_пробелов).

    La Voz de Galicia и часть Prensa Ibérica отдают ботам только лид статьи,
    за которым следует блок «Suscríbete para seguir leyendo». Такой обрубок
    нельзя суммаризировать — модель отвечает SKIP по пустому телу. Обрезаем по
    первому пейволл-маркеру и меряем длину оставшегося содержания."""
    if not full_text:
        return '', 0
    m = PAYWALL_MARKERS.search(full_text)
    body = full_text[:m.start()] if m else full_text
    body = body.strip()
    normalized_len = len(re.sub(r'\s+', ' ', body).strip())
    return body, normalized_len


# Мусорные ответы модели: она иногда протекает наружу служебный safety-ярлык
# модерации ("User Safety: safe"), эхо системного промпта или один заголовок
# вместо саммари. Такие ответы нельзя принимать за 'done'.
_GARBAGE_SUMMARY_RE = re.compile(
    r'^\s*(?:'
    r'(?:user|response|content|assistant)\s+safety\s*:'   # User Safety: safe/unsafe
    r'|you\s+are\s+(?:a|an)\s+'                            # эхо system-промпта
    r'|i\s+(?:cannot|can\'t|am\s+unable)'                  # отказ модели
    r'|as\s+an?\s+(?:ai|language\s+model)'                 # "As an AI language model"
    # Утечка «размышлений» модели наружу вместо готового саммари:
    r'|(?:the\s+user|we\s+need\s+to|let\s+me|i\s+need\s+to|first,?\s+i|okay,?\s+so)\b'
    r')',
    re.IGNORECASE,
)

# Эхо сырого входа: модель возвращает наш промпт целиком с меткой TEXTO:
# (сам текст статьи), а не саммари. Метка RESUMEN: наоборот означает, что
# саммари есть — такие ответы (с шапкой TÍTULO/FUENTE) считаем валидными.
_PROMPT_ECHO_RE = re.compile(r'(?im)^\s*texto\s*:')


def is_garbage_summary(text: str | None) -> bool:
    """True, если ответ модели — не саммари, а служебный мусор/эхо/отказ.

    Отсекает safety-ярлыки провайдера, утечку размышлений, эхо сырого входа,
    отказы и слишком короткие ответы без реального содержания статьи.
    Ответы с шапкой TÍTULO/FUENTE + RESUMEN считаются валидными (саммари есть)."""
    if not text:
        return True
    clean = text.strip()
    if len(re.sub(r'\s+', ' ', clean)) < 40:
        return True
    if _GARBAGE_SUMMARY_RE.match(clean):
        return True
    # Модель вернула сырой текст статьи (метка TEXTO:) вместо саммари.
    if _PROMPT_ECHO_RE.search(clean):
        return True
    return False


def fast_classify_spain(title: str, source: str, full_text: str,
                        model: str = ANYMODEL_MODEL,
                        api_key: str | None = ANYMODEL_KEY) -> tuple[bool, dict]:
    """
    Двухэтапный ультрабыстрый классификатор релевантности Испании:
    Этап 1: Локальные эвристики (0 мс, 0 токенов, мгновенный отсев скидок и чисто международного контента).
    Этап 2: Микро-запрос к LLM (100 токенов, ответ SI/NO за ~0.3 сек) только для неоднозначных статей.
    
    Возвращает: (is_spain: bool, usage_info: dict)
    """
    usage_info = {"prompt_tokens": 0, "cached_tokens": 0, "completion_tokens": 0,
                  "rate_limited": False}
    
    full_sample = f"{title} {full_text[:1200]}".lower()

    # 1. Отсев коммерческих купонов и промокодов (100% SKIP, 0 токенов)
    if COMMERCIAL_PATTERNS.search(title or '') or COMMERCIAL_PATTERNS.search(full_sample[:400]):
        return False, usage_info

    # 2. Проверка на наличие явных испанских сущностей (90% испанских новостей распознаются за 0 мс)
    has_spain_markers = bool(SPAIN_POSITIVE_PATTERNS.search(full_sample))
    has_pure_intl_markers = bool(PURE_INTERNATIONAL_PATTERNS.search(full_sample[:500]))

    # Если есть испанские маркеры и нет доминирующего внешнего контекста — это точно Испания
    if has_spain_markers and not has_pure_intl_markers:
        return True, usage_info

    # Если явный международный контекст и НЕТ ни одного испанского маркера — это точно НЕ Испания
    if has_pure_intl_markers and not has_spain_markers:
        return False, usage_info

    # 3. Mistral has first priority for ambiguous single-article fallbacks too.
    mistral_result, mistral_usage = classify_articles_mistral(
        [(0, source, title, full_text)]
    )
    if mistral_result and 0 in mistral_result:
        return mistral_result[0], mistral_usage

    # 4. If Mistral is unavailable, use the AnyModel micro-request.
    endpoint = f"{ANYMODEL_BASE_URL}/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key or ANYMODEL_KEY}",
        "Content-Type": "application/json"
    }
    mini_prompt = (
        "Clasifica si la siguiente noticia trata significativamente sobre España "
        "(acontecimientos, sociedad, política, economía o personas en España). "
        "Responde ÚNICAMENTE 'SI' o 'NO'."
    )
    user_content = f"TÍTULO: {title or 'Sin título'}\nFUENTE: {source}\nINICIO: {full_text[:500]}"
    
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": mini_prompt},
            {"role": "user", "content": user_content}
        ],
        "temperature": 0.0,
        "max_tokens": 3
    }

    try:
        session = get_http_session()
        with _API_SEMAPHORE:
            _wait_for_api_slot()
            resp = session.post(endpoint, headers=headers, json=payload, timeout=(5, 15))
        if resp.status_code == 429:
            _apply_api_cooldown(4.0)
            log.warning("⚠️ API rate limit при классификации; использую локальную эвристику")
            return has_spain_markers, usage_info
        if resp.status_code == 200:
            data = resp.json()
            u = data.get("usage")
            if isinstance(u, dict):
                usage_info["prompt_tokens"] = u.get("prompt_tokens") or 0
                usage_info["completion_tokens"] = u.get("completion_tokens") or 0
            
            choices = data.get("choices") or []
            if choices:
                ans = (choices[0].get("message") or {}).get("content", "").strip().upper()
                if "SI" in ans or "SÍ" in ans:
                    return True, usage_info
                return False, usage_info
    except Exception:
        pass

    # При сомнениях — доверяем испанским маркерам
    return has_spain_markers, usage_info


def compute_content_hash(model: str, title: str, full_text: str) -> str:
    """Вычисляет детерминированный SHA-256 хэш запроса."""
    raw = f"{model}::{SPAIN_NEWS_SYSTEM_PROMPT}::{title or ''}::{full_text.strip()}"
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


def _parse_json_array(content: str) -> list[dict] | None:
    """Extract a JSON array from a model response, tolerating markdown fences."""
    if not content:
        return None
    text = content.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.IGNORECASE | re.DOTALL).strip()
    try:
        value = json.loads(text)
    except (TypeError, json.JSONDecodeError):
        match = re.search(r"\[.*\]", text, flags=re.DOTALL)
        if not match:
            return None
        try:
            value = json.loads(match.group(0))
        except json.JSONDecodeError:
            return None
    return value if isinstance(value, list) and all(isinstance(x, dict) for x in value) else None


def classify_articles_mistral(pending: list[tuple]) -> tuple[dict[int, bool] | None, dict]:
    """Classify ambiguous articles with Mistral, falling back cleanly on failure."""
    global _MISTRAL_SUCCESS_LOGGED
    usage = {"prompt_tokens": 0, "completion_tokens": 0, "cached_tokens": 0,
             "rate_limited": False}
    if not MISTRAL_API_KEY:
        return None, usage
    prompt = (
        "Clasifica cada noticia. Devuelve SOLO un array JSON con objetos "
        "{\"id\": número, \"is_spain\": true/false}. "
        "is_spain=true solo si la noticia trata significativamente sobre España.\n\n" +
        "\n\n".join(
            f"ID: {aid}\nTÍTULO: {title}\nFUENTE: {source}\nINICIO: {(text or '')[:700]}"
            for aid, source, title, text in pending
        )
    )
    payload = {
        "model": MISTRAL_MODEL,
        "messages": [
            {"role": "system", "content": "Eres un clasificador. Responde solo JSON válido."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.0,
        "max_tokens": max(40, len(pending) * 12),
    }
    try:
        with _MISTRAL_SEMAPHORE:
            _wait_for_mistral_slot()
            resp = get_http_session().post(
                f"{MISTRAL_BASE_URL}/chat/completions",
                headers={"Authorization": f"Bearer {MISTRAL_API_KEY}", "Content-Type": "application/json"},
                json=payload, timeout=(5, 20),
            )
        if resp.status_code == 429:
            usage["rate_limited"] = True
            _apply_mistral_cooldown(15.0)
            log.warning("⚠️ Mistral rate limit (429); кулдаун 15 с, классификация продолжится через резерв")
            return None, usage
        if resp.status_code != 200:
            log.warning("⚠️ Mistral HTTP %d; классификация продолжится через резерв", resp.status_code)
            return None, usage
        if not _MISTRAL_SUCCESS_LOGGED:
            log.info("✅ Mistral работает: модель %s, параллельных запросов не более %d",
                     MISTRAL_MODEL, MISTRAL_MAX_CONCURRENT)
            _MISTRAL_SUCCESS_LOGGED = True
        data = resp.json()
        api_usage = data.get("usage") or {}
        usage["prompt_tokens"] = api_usage.get("prompt_tokens") or 0
        usage["completion_tokens"] = api_usage.get("completion_tokens") or 0
        choices = data.get("choices") or []
        content = ((choices[0].get("message") or {}).get("content") or "") if choices else ""
        parsed = _parse_json_array(content)
        if parsed is None:
            return None, usage
        valid_ids = {aid for aid, *_ in pending}
        result = {}
        for item in parsed:
            try:
                aid = int(item["id"])
                if aid in valid_ids and isinstance(item.get("is_spain"), bool):
                    result[aid] = item["is_spain"]
            except (KeyError, TypeError, ValueError):
                continue
        return (result if result else None), usage
    except requests.RequestException as exc:
        log.warning("⚠️ Mistral недоступен; классификация продолжится через Gemini: %s", exc)
        return None, usage


def classify_articles_gemini(pending: list[tuple]) -> tuple[dict[int, bool] | None, dict]:
    """Classify articles through Google's native Gemini generateContent API."""
    usage = {"prompt_tokens": 0, "completion_tokens": 0, "cached_tokens": 0,
             "rate_limited": False}
    if not GEMINI_API_KEY:
        return None, usage
    prompt = (
        "Clasifica cada noticia y responde SOLO un array JSON con objetos "
        "{\"id\": número, \"is_spain\": true/false}. "
        "true únicamente si trata significativamente sobre España.\n\n" +
        "\n\n".join(f"ID: {aid}\nTÍTULO: {title}\nFUENTE: {source}\nINICIO: {(text or '')[:700]}"
                  for aid, source, title, text in pending)
    )
    global _GEMINI_MODEL_INDEX, _GEMINI_COOLDOWN_UNTIL, _GEMINI_SUCCESS_LOGGED
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.0,
            "maxOutputTokens": max(120, len(pending) * 20),
            "responseMimeType": "application/json",
            "responseJsonSchema": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "is_spain": {"type": "boolean"},
                    },
                    "required": ["id", "is_spain"],
                    "additionalProperties": False,
                },
            },
        },
    }
    try:
        models = GEMINI_MODELS or (GEMINI_MODEL,)
        with _GEMINI_STATE_LOCK:
            start = 0
            _GEMINI_MODEL_INDEX += 1
            cooldown = max(0.0, _GEMINI_COOLDOWN_UNTIL - time.monotonic())
        if cooldown:
            time.sleep(min(cooldown, 2.0))
        resp = None
        selected_model = None
        for offset in range(min(3, len(models))):
            selected_model = models[(start + offset) % len(models)]
            url = f"{GEMINI_BASE_URL}/models/{selected_model}:generateContent"
            with _GEMINI_SEMAPHORE:
                resp = get_http_session().post(
                    url, headers={"Content-Type": "application/json", "X-goog-api-key": GEMINI_API_KEY},
                    json=payload, timeout=(5, 20))
            if resp.status_code not in (404, 500, 502, 503, 504):
                break
            error_text = resp.text[:200].replace("\n", " ")
            log.warning("⚠️ Gemini %s HTTP %d (%s); пробую следующую модель",
                        selected_model, resp.status_code, error_text)
            with _GEMINI_STATE_LOCK:
                _GEMINI_COOLDOWN_UNTIL = max(_GEMINI_COOLDOWN_UNTIL, time.monotonic() + 2.0)
            time.sleep(0.5)
        if resp.status_code == 429:
            usage["rate_limited"] = True
            retry_after = resp.headers.get("Retry-After", "")
            log.warning("⚠️ Gemini %s rate limit (429, Retry-After=%s); используется следующий провайдер",
                        selected_model, retry_after or "нет")
            return None, usage
        if resp.status_code != 200:
            log.warning("⚠️ Gemini: три модели недоступны, последний HTTP %d; используется следующий провайдер",
                        resp.status_code)
            return None, usage
        if not _GEMINI_SUCCESS_LOGGED:
            with _GEMINI_STATE_LOCK:
                if not _GEMINI_SUCCESS_LOGGED:
                    log.info("✅ Gemini работает: модель %s, параллельных запросов не более %d",
                             selected_model, GEMINI_MAX_CONCURRENT)
                    _GEMINI_SUCCESS_LOGGED = True
        data = resp.json()
        metadata = data.get("usageMetadata") or {}
        usage["prompt_tokens"] = metadata.get("promptTokenCount") or 0
        usage["completion_tokens"] = metadata.get("candidatesTokenCount") or 0
        candidates = data.get("candidates") or []
        content = ""
        if candidates:
            content = "".join(p.get("text", "") for p in (candidates[0].get("content", {}).get("parts", []) or []))
        parsed = _parse_json_array(content)
        if parsed is None:
            log.warning("⚠️ Gemini вернул некорректный JSON; используется AnyModel")
            return None, usage
        valid_ids = {aid for aid, *_ in pending}
        result = {}
        for item in parsed:
            try:
                aid = int(item["id"])
                if aid in valid_ids and isinstance(item.get("is_spain"), bool):
                    result[aid] = item["is_spain"]
            except (KeyError, TypeError, ValueError):
                continue
        return (result if result else None), usage
    except requests.RequestException as exc:
        log.warning("⚠️ Gemini недоступен; классификация продолжится через OpenRouter: %s", exc)
        return None, usage


def classify_articles_openrouter(pending: list[tuple]) -> tuple[dict[int, bool] | None, dict]:
    """Classify articles through OpenRouter's free automatic model router."""
    global _OPENROUTER_SUCCESS_LOGGED
    usage = {"prompt_tokens": 0, "completion_tokens": 0, "cached_tokens": 0,
             "rate_limited": False}
    if not OPENROUTER_API_KEY:
        return None, usage
    prompt = (
        "Clasifica cada noticia. Devuelve SOLO JSON válido: "
        "[{\"id\": número, \"is_spain\": true/false}]. "
        "true solo si trata significativamente sobre España.\n\n" +
        "\n\n".join(f"ID: {aid}\nTÍTULO: {title}\nFUENTE: {source}\nINICIO: {(text or '')[:700]}"
                  for aid, source, title, text in pending)
    )
    payload = {
        "model": OPENROUTER_MODEL,
        "messages": [
            {"role": "system", "content": "Responde únicamente con el array JSON solicitado."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.0,
        "max_tokens": max(300, len(pending) * 40),
        "reasoning": {"effort": "none", "exclude": True},
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "spain_classification",
                "strict": True,
                "schema": {
                    "type": "object",
                    "properties": {
                        "results": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "id": {"type": "integer"},
                                    "is_spain": {"type": "boolean"},
                                },
                                "required": ["id", "is_spain"],
                                "additionalProperties": False,
                            },
                        }
                    },
                    "required": ["results"],
                    "additionalProperties": False,
                },
            },
        },
    }
    try:
        with _OPENROUTER_SEMAPHORE:
            resp = get_http_session().post(
                f"{OPENROUTER_BASE_URL}/chat/completions",
                headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json"},
                json=payload, timeout=(5, 25),
            )
        if resp.status_code == 429:
            usage["rate_limited"] = True
            log.warning("⚠️ OpenRouter rate limit (429); классификация продолжится через AnyModel")
            return None, usage
        if resp.status_code != 200:
            log.warning("⚠️ OpenRouter HTTP %d; классификация продолжится через AnyModel: %s",
                        resp.status_code, resp.text[:200])
            return None, usage
        if not _OPENROUTER_SUCCESS_LOGGED:
            log.info("✅ OpenRouter работает: модель %s, параллельных запросов не более %d",
                     OPENROUTER_MODEL, PROVIDER_MAX_CONCURRENT)
            _OPENROUTER_SUCCESS_LOGGED = True
        data = resp.json()
        api_usage = data.get("usage") or {}
        usage["prompt_tokens"] = api_usage.get("prompt_tokens") or 0
        usage["completion_tokens"] = api_usage.get("completion_tokens") or 0
        choices = data.get("choices") or []
        message = (choices[0].get("message") or {}) if choices else {}
        content = message.get("content") or message.get("reasoning") or ""
        parsed = _parse_json_array(content)
        if parsed is None and content:
            try:
                wrapped = json.loads(content)
                parsed = wrapped.get("results") if isinstance(wrapped, dict) else None
            except json.JSONDecodeError:
                parsed = None
        if parsed is None:
            log.warning("⚠️ OpenRouter вернул некорректный JSON; используется AnyModel")
            return None, usage
        valid_ids = {aid for aid, *_ in pending}
        result = {}
        for item in parsed:
            try:
                aid = int(item["id"])
                if aid in valid_ids and isinstance(item.get("is_spain"), bool):
                    result[aid] = item["is_spain"]
            except (KeyError, TypeError, ValueError):
                continue
        return (result if result else None), usage
    except requests.RequestException as exc:
        log.warning("⚠️ OpenRouter недоступен; классификация продолжится через Cerebras: %s", exc)
        return None, usage


def classify_articles_cerebras(pending: list[tuple]) -> tuple[dict[int, bool] | None, dict]:
    """Classify articles with Cerebras' fast OpenAI-compatible API."""
    usage = {"prompt_tokens": 0, "completion_tokens": 0, "cached_tokens": 0,
             "rate_limited": False}
    global _CEREBRAS_DISABLED, _CEREBRAS_SUCCESS_LOGGED
    if not CEREBRAS_API_KEY or _CEREBRAS_DISABLED:
        return None, usage
    prompt = (
        "Clasifica cada noticia. Devuelve SOLO un array JSON: "
        "[{\"id\": número, \"is_spain\": true/false}]. "
        "true solo si trata significativamente sobre España.\n\n" +
        "\n\n".join(f"ID: {aid}\nTÍTULO: {title}\nFUENTE: {source}\nINICIO: {(text or '')[:700]}"
                  for aid, source, title, text in pending)
    )
    payload = {
        "model": CEREBRAS_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.0,
        "max_completion_tokens": max(120, len(pending) * 20),
    }
    try:
        with _CEREBRAS_SEMAPHORE:
            resp = get_http_session().post(
                f"{CEREBRAS_BASE_URL}/chat/completions",
                headers={"Authorization": f"Bearer {CEREBRAS_API_KEY}", "Content-Type": "application/json"},
                json=payload, timeout=(5, 20),
            )
        if resp.status_code == 429:
            usage["rate_limited"] = True
            log.warning("⚠️ Cerebras rate limit (429); классификация продолжится через AnyModel")
            return None, usage
        if resp.status_code == 402:
            _CEREBRAS_DISABLED = True
            log.warning("⚠️ Cerebras отключён до перезапуска: у API-ключа нет бесплатной квоты (HTTP 402)")
            return None, usage
        if resp.status_code != 200:
            log.warning("⚠️ Cerebras HTTP %d; классификация продолжится через AnyModel: %s",
                        resp.status_code, resp.text[:200])
            return None, usage
        if not _CEREBRAS_SUCCESS_LOGGED:
            log.info("✅ Cerebras работает: модель %s, параллельных запросов не более %d",
                     CEREBRAS_MODEL, PROVIDER_MAX_CONCURRENT)
            _CEREBRAS_SUCCESS_LOGGED = True
        data = resp.json()
        api_usage = data.get("usage") or {}
        usage["prompt_tokens"] = api_usage.get("prompt_tokens") or 0
        usage["completion_tokens"] = api_usage.get("completion_tokens") or 0
        choices = data.get("choices") or []
        content = ((choices[0].get("message") or {}).get("content") or "") if choices else ""
        parsed = _parse_json_array(content)
        if parsed is None:
            log.warning("⚠️ Cerebras вернул некорректный JSON; используется AnyModel")
            return None, usage
        valid_ids = {aid for aid, *_ in pending}
        result = {}
        for item in parsed:
            try:
                aid = int(item["id"])
                if aid in valid_ids and isinstance(item.get("is_spain"), bool):
                    result[aid] = item["is_spain"]
            except (KeyError, TypeError, ValueError):
                continue
        return (result if result else None), usage
    except requests.RequestException as exc:
        log.warning("⚠️ Cerebras недоступен; классификация продолжится через AnyModel: %s", exc)
        return None, usage


def _next_provider_name() -> str:
    """Assign successive batches to independent provider queues.

    Пропускаем провайдеров без ключа и отключённый Cerebras (HTTP 402): иначе
    их доля пакетов немедленно сваливается на AnyModel и переполняет его лимит.
    """
    global _CLASSIFIER_ROUTE_INDEX
    providers = ["mistral", "gemini", "openrouter", "cerebras", "anymodel"]
    available = []
    for p in providers:
        if p == "anymodel":
            available.append(p)  # AnyModel — всегда доступный фолбэк
        elif p == "cerebras" and (_CEREBRAS_DISABLED or not CEREBRAS_API_KEY):
            continue
        elif p == "mistral" and not MISTRAL_API_KEY:
            continue
        elif p == "gemini" and not GEMINI_API_KEY:
            continue
        elif p == "openrouter" and not OPENROUTER_API_KEY:
            continue
        else:
            available.append(p)
    if not available:
        available = ["anymodel"]
    with _CLASSIFIER_ROUTE_LOCK:
        provider = available[_CLASSIFIER_ROUTE_INDEX % len(available)]
        _CLASSIFIER_ROUTE_INDEX += 1
    return provider


def classify_articles_batch(articles: list[tuple], model: str, api_key: str | None,
                            provider_name: str = "anymodel") -> tuple[dict[int, bool], dict]:
    """Classify up to ten articles with one request; deterministic heuristics run first."""
    results: dict[int, bool] = {}
    usage = {"prompt_tokens": 0, "completion_tokens": 0, "cached_tokens": 0,
             "rate_limited": False}
    pending = []
    for art_id, source, title, link, published, full_text in articles:
        sample = f"{title or ''} {(full_text or '')[:1200]}".lower()
        if COMMERCIAL_PATTERNS.search(sample):
            results[art_id] = False
        elif SPAIN_POSITIVE_PATTERNS.search(sample) and not PURE_INTERNATIONAL_PATTERNS.search(sample[:500]):
            results[art_id] = True
        elif PURE_INTERNATIONAL_PATTERNS.search(sample) and not SPAIN_POSITIVE_PATTERNS.search(sample):
            results[art_id] = False
        else:
            pending.append((art_id, source, title, full_text))
    log.info("🔎 Классификация: провайдер=%s, пакет=%d, через API=%d, локально=%d",
             provider_name, len(articles), len(pending), len(articles) - len(pending))
    if not pending:
        return results, usage
    provider_func = {
        "mistral": classify_articles_mistral,
        "gemini": classify_articles_gemini,
        "openrouter": classify_articles_openrouter,
        "cerebras": classify_articles_cerebras,
    }.get(provider_name)
    log.debug("Классификационный пакет направлен только в %s", provider_name)
    if provider_func:
        provider_results, provider_usage = provider_func(pending)
        for name in usage:
            if name == "rate_limited":
                usage[name] = usage[name] or provider_usage.get(name, False)
            else:
                usage[name] += provider_usage.get(name, 0)
        if provider_results:
            results.update(provider_results)
            pending = [item for item in pending if item[0] not in provider_results]
            if not pending:
                return results, usage
    prompt = (
        "Clasifica cada noticia. Devuelve SOLO un array JSON con objetos "
        "{\"id\": número, \"is_spain\": true/false}. "
        "is_spain=true solo si trata significativamente sobre España.\n\n" +
        "\n\n".join(f"ID: {aid}\nTÍTULO: {title}\nFUENTE: {source}\nINICIO: {(text or '')[:700]}"
                  for aid, source, title, text in pending)
    )
    system = "Eres un clasificador de noticias. Responde únicamente JSON válido, sin markdown ni comentarios."
    content, call_usage = call_anymodel_llm(prompt, model=model, api_key=api_key,
                                             system_prompt=system, max_tokens=max(20, len(pending) * 8))
    usage.update({k: call_usage.get(k, False if k == "rate_limited" else 0) for k in usage})
    parsed = _parse_json_array(content)
    if parsed is None:
        if not call_usage.get("rate_limited"):
            for aid, source, title, text in pending:
                results[aid] = fast_classify_spain(title, source, text, model=model, api_key=api_key)[0]
    else:
        valid_ids = {aid for aid, *_ in pending}
        for item in parsed:
            try:
                aid = int(item["id"])
                if aid in valid_ids and isinstance(item.get("is_spain"), bool):
                    results[aid] = item["is_spain"]
            except (KeyError, TypeError, ValueError):
                continue
        for aid, source, title, text in pending:
            if aid not in results and not call_usage.get("rate_limited"):
                results[aid] = fast_classify_spain(title, source, text, model=model, api_key=api_key)[0]
    return results, usage


def summarize_articles_batch(articles: list[tuple], model: str, api_key: str | None) -> tuple[dict[int, tuple[str | None, str]], dict]:
    """Classify + summarize in one call. Non-Spain articles return is_spain=false (no summary written)."""
    usage = {"prompt_tokens": 0, "completion_tokens": 0, "cached_tokens": 0,
             "rate_limited": False}
    prompt = (
        "Para cada artículo: si trata significativamente sobre España, devuelve "
        "{\"id\": N, \"is_spain\": true, \"summary\": texto_en_español_max_250_palabras}. "
        "Si no trata sobre España, devuelve {\"id\": N, \"is_spain\": false}. "
        "Devuelve SOLO un array JSON válido. No mezcles artículos.\n\n" +
        "\n\n".join(f"ID: {aid}\nTÍTULO: {title}\nFUENTE: {source}\nFECHA: {published or 'Desconocida'}\n"
                  f"URL: {link}\nTEXTO:\n{(text or '').strip()[:8000]}"
                  for aid, source, title, link, published, text in articles)
    )
    batch_system_prompt = (
        SPAIN_NEWS_SYSTEM_PROMPT +
        "\n\nREGLA DE SALIDA POR LOTES: responde únicamente con un array JSON válido. "
        "Cada objeto debe contener el ID recibido, is_spain (bool) y, si is_spain=true, el campo summary."
    )
    content, call_usage = call_anymodel_llm(
        prompt, model=model, api_key=api_key, system_prompt=batch_system_prompt,
        max_tokens=max(1000, len(articles) * 1000))
    usage.update({k: call_usage.get(k, False if k == "rate_limited" else 0) for k in usage})
    parsed = _parse_json_array(content)
    output: dict[int, tuple[str | None, str]] = {}
    valid_ids = {a[0] for a in articles}
    if parsed:
        for item in parsed:
            try:
                aid = int(item["id"])
                if aid not in valid_ids:
                    continue
                if not item.get("is_spain", True):
                    output[aid] = ("SKIP", "skipped")
                    continue
                value = str(item.get("summary", "")).strip()
                if value:
                    if is_garbage_summary(value):
                        continue  # мусор — оставляем pending для повторной попытки
                    output[aid] = (value, "done")
            except (KeyError, TypeError, ValueError):
                continue
    return output, usage


def summarize_articles_provider(articles: list[tuple], provider_name: str,
                                model: str, api_key: str | None):
    """Summarize a batch on its assigned provider queue; AnyModel is fallback."""
    if provider_name == "anymodel":
        return summarize_articles_batch(articles, model, api_key)
    usage = {"prompt_tokens": 0, "completion_tokens": 0, "cached_tokens": 0,
             "rate_limited": False}
    prompt = (
        "Para cada artículo: si trata significativamente sobre España, devuelve "
        "{\"id\": N, \"is_spain\": true, \"summary\": texto_en_español_max_250_palabras}. "
        "Si no trata sobre España, devuelve {\"id\": N, \"is_spain\": false}. "
        "Devuelve SOLO un array JSON válido. No mezcles artículos.\n\n" +
        "\n\n".join(f"ID: {aid}\nTÍTULO: {title}\nFUENTE: {source}\nFECHA: {published or 'Desconocida'}\n"
                  f"URL: {link}\nTEXTO:\n{(text or '')[:8000]}"
                  for aid, source, title, link, published, text in articles)
    )
    try:
        if provider_name == "gemini":
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.2, "maxOutputTokens": max(1200, len(articles) * 1000),
                                     "responseMimeType": "application/json"},
            }
            content = ""
            # Перебираем все модели Gemini, а не только первую: если модель
            # отдаёт rate limit / ошибку / пустой ответ — пробуем следующую,
            # прежде чем отдавать пакет фолбэку.
            for selected in (GEMINI_MODELS or (GEMINI_MODEL,)):
                with _GEMINI_SEMAPHORE:
                    resp = get_http_session().post(
                        f"{GEMINI_BASE_URL}/models/{selected}:generateContent",
                        headers={"Content-Type": "application/json", "X-goog-api-key": GEMINI_API_KEY},
                        json=payload, timeout=(5, 40))
                if resp.status_code == 200:
                    data = resp.json()
                    candidates = data.get("candidates") or []
                    content = "".join(p.get("text", "") for p in
                                      ((candidates[0].get("content") or {}).get("parts") or [])) if candidates else ""
                    if content.strip():
                        break
                    log.warning("⚠️ Gemini %s вернул пустой ответ на саммари; пробую следующую модель", selected)
                elif resp.status_code == 429:
                    log.warning("⚠️ Gemini %s rate limit (429) на саммари; пробую следующую модель", selected)
                else:
                    log.warning("⚠️ Gemini %s HTTP %d на саммари; пробую следующую модель",
                                selected, resp.status_code)
        else:
            config = {
                "mistral": (MISTRAL_BASE_URL, MISTRAL_API_KEY, MISTRAL_MODEL, _MISTRAL_SEMAPHORE),
                "openrouter": (OPENROUTER_BASE_URL, OPENROUTER_API_KEY, OPENROUTER_MODEL, _OPENROUTER_SEMAPHORE),
                "cerebras": (CEREBRAS_BASE_URL, CEREBRAS_API_KEY, CEREBRAS_MODEL, _CEREBRAS_SEMAPHORE),
            }.get(provider_name)
            if not config or (provider_name == "cerebras" and _CEREBRAS_DISABLED):
                return summarize_articles_batch(articles, model, api_key)
            base_url, key, provider_model, semaphore = config
            batch_system_prompt = (
                SPAIN_NEWS_SYSTEM_PROMPT +
                "\n\nREGLA DE SALIDA POR LOTES: responde únicamente con un array JSON válido. "
                "Cada objeto debe contener el ID recibido, is_spain (bool) y, si is_spain=true, el campo summary."
            )
            payload = {
                "model": provider_model,
                "messages": [
                    {"role": "system", "content": batch_system_prompt},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.2,
                "max_tokens": max(1200, len(articles) * 1000),
            }
            if provider_name == "openrouter":
                payload["reasoning"] = {"effort": "none", "exclude": True}
            with semaphore:
                if provider_name == "mistral":
                    _wait_for_mistral_slot()
                resp = get_http_session().post(
                    f"{base_url}/chat/completions",
                    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                    json=payload, timeout=(5, 40))
            if resp.status_code == 402 and provider_name == "cerebras":
                globals()["_CEREBRAS_DISABLED"] = True
            if resp.status_code == 429 and provider_name == "mistral":
                usage["rate_limited"] = True
                _apply_mistral_cooldown(15.0)
                log.warning("⚠️ Mistral rate limit (429) на саммари; кулдаун 15 с, пакет передан AnyModel")
            if resp.status_code == 200:
                data = resp.json()
                choices = data.get("choices") or []
                content = ((choices[0].get("message") or {}).get("content") or "") if choices else ""
                api_usage = data.get("usage") or {}
                usage["prompt_tokens"] = api_usage.get("prompt_tokens") or 0
                usage["completion_tokens"] = api_usage.get("completion_tokens") or 0
            else:
                content = ""
        parsed = _parse_json_array(content)
        if parsed:
            valid_ids = {a[0] for a in articles}
            output = {}
            for item in parsed:
                try:
                    aid = int(item["id"])
                    if aid not in valid_ids:
                        continue
                    if not item.get("is_spain", True):
                        output[aid] = ("SKIP", "skipped")
                        continue
                    value = str(item.get("summary", "")).strip()
                    if value:
                        if is_garbage_summary(value):
                            continue  # мусор — оставляем pending для повторной попытки
                        output[aid] = (value, "done")
                except (KeyError, TypeError, ValueError):
                    continue
            if output:
                return output, usage
    except requests.RequestException as exc:
        log.warning("⚠️ %s не завершил саммаризацию: %s", provider_name, exc)
    log.warning("⚠️ %s не обработал пакет саммари; пакет передан AnyModel", provider_name)
    return summarize_articles_batch(articles, model, api_key)


# =================== LLM API ВЫЗОВ С ПРЕФИКСНЫМ КЭШИРОВАНИЕМ ===================

def call_anymodel_llm(user_content: str, model: str = ANYMODEL_MODEL,
                      api_key: str | None = ANYMODEL_KEY,
                      base_url: str = ANYMODEL_BASE_URL,
                      system_prompt: str = SPAIN_NEWS_SYSTEM_PROMPT,
                      max_tokens: int = 1000,
                      _tried_models: set[str] | None = None) -> tuple[str | None, dict]:
    """
    Отправляет запрос к AnyModel / OpenAI-совместимому API с поддержкой
    серверного префиксного кэширования системного промпта (Prompt Caching).
    
    Возвращает (content, usage_info):
      usage_info: {'prompt_tokens': int, 'cached_tokens': int, 'completion_tokens': int}
    """
    global _ANYMODEL_ROUTE_INDEX
    # New concurrent requests start on different free aliases instead of all
    # blocking on the configured primary model.
    with _MODEL_STATE_LOCK:
        if not _tried_models and ANYMODEL_FREE_MODELS:
            available = [m for m in ANYMODEL_FREE_MODELS if m not in _UNAVAILABLE_MODELS]
            if available:
                model = available[_ANYMODEL_ROUTE_INDEX % len(available)]
                _ANYMODEL_ROUTE_INDEX += 1
        if model in _UNAVAILABLE_MODELS:
            for candidate in ANYMODEL_FREE_MODELS:
                if candidate not in _UNAVAILABLE_MODELS:
                    model = candidate
                    break
    usage_info = {"prompt_tokens": 0, "cached_tokens": 0, "completion_tokens": 0,
                  "rate_limited": False}
    # Keep model rotation local to this request, preventing loops and ensuring
    # that every configured free model is tried at most once.
    tried_models = set(_tried_models or ())
    tried_models.add(model)
    if len(tried_models) > ANYMODEL_MODEL_ATTEMPTS:
        log.warning("⚠️ AnyModel: достигнут быстрый лимит перебора (%d моделей)", ANYMODEL_MODEL_ATTEMPTS)
        return None, usage_info
    key = api_key or os.environ.get('ANYMODEL_KEY')
    if not key:
        log.error("❌ Не задан ANYMODEL_KEY! Укажите его в .env или переменных окружения.")
        return None, usage_info

    endpoint = f"{base_url}/chat/completions"
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json"
    }

    # Формируем структуру с неизменным статическим системным сообщением
    # Для моделей с поддержкой Prefix / Prompt Caching (DeepSeek, Claude, OpenAI)
    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": system_prompt
            },
            {
                "role": "user",
                "content": user_content
            }
        ],
        "temperature": 0.2,
        "max_tokens": max_tokens
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            session = get_http_session()
            with _API_SEMAPHORE:
                # Another worker may have marked this model unavailable while
                # this request was waiting for the single API slot.
                with _MODEL_STATE_LOCK:
                    if model in _UNAVAILABLE_MODELS:
                        replacement = next(
                            (m for m in ANYMODEL_FREE_MODELS
                             if m not in _UNAVAILABLE_MODELS and m not in tried_models),
                            None,
                        )
                        if replacement:
                            model = replacement
                            tried_models.add(model)
                            payload["model"] = model
                _wait_for_api_slot()
                resp = session.post(endpoint, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()
                choices = data.get("choices") or []
                if not choices:
                    log.warning("⚠️ AnyModel вернул пустой список choices")
                    return None, usage_info
                with _MODEL_STATE_LOCK:
                    if model not in _ANYMODEL_SUCCESS_MODELS:
                        log.info("✅ AnyModel работает: модель %s, параллельных запросов не более %d",
                                 model, API_MAX_CONCURRENT)
                        _ANYMODEL_SUCCESS_MODELS.add(model)
                
                msg = choices[0].get("message") or {}
                raw_content = msg.get("content")
                if not raw_content or not raw_content.strip():
                    # Модель деградировала: отвечает 200 OK, но с пустым телом.
                    # Помечаем её недоступной, иначе round-robin будет снова и
                    # снова подсовывать её всем воркерам (шторм пустых ответов).
                    with _MODEL_STATE_LOCK:
                        _UNAVAILABLE_MODELS.add(model)
                    candidates = (ANYMODEL_FREE_MODELS or (ANYMODEL_FALLBACK_MODEL,))
                    fallback = next(
                        (m for m in candidates
                         if m not in tried_models and m not in _UNAVAILABLE_MODELS),
                        None)
                    if fallback:
                        log.warning("⚠️ AnyModel %s вернул пустой ответ; пробую %s", model, fallback)
                        return call_anymodel_llm(
                            user_content, model=fallback, api_key=api_key,
                            base_url=base_url, system_prompt=system_prompt,
                            max_tokens=max_tokens, _tried_models=tried_models)
                    return None, usage_info

                content = raw_content.strip()
                
                # Извлекаем метрики использования токенов и серверного кэша
                u = data.get("usage")
                if isinstance(u, dict):
                    usage_info["prompt_tokens"] = u.get("prompt_tokens") or 0
                    usage_info["completion_tokens"] = u.get("completion_tokens") or 0
                    details = u.get("prompt_tokens_details") if isinstance(u.get("prompt_tokens_details"), dict) else {}
                    cached = (
                        u.get("prompt_cache_hit_tokens") or
                        u.get("cached_tokens") or
                        details.get("cached_tokens", 0) or
                        0
                    )
                    usage_info["cached_tokens"] = cached

                # Удаляем обрамляющие markdown блоки ```markdown ... ``` если модель их вернула
                if content.startswith("```"):
                    lines = content.splitlines()
                    if lines[0].startswith("```"):
                        lines = lines[1:]
                    if lines and lines[-1].startswith("```"):
                        lines = lines[:-1]
                    content = "\n".join(lines).strip()
                return content, usage_info
            elif resp.status_code in (429, 529):
                retry_after = resp.headers.get("Retry-After")
                try:
                    server_wait = float(retry_after) if retry_after else 0.0
                except ValueError:
                    server_wait = 0.0
                if not server_wait:
                    try:
                        body = resp.json()
                        server_wait = float((body.get("error") or {}).get("retry_after_seconds") or 0)
                    except Exception:
                        server_wait = 0.0
                cooldown = min(max(server_wait or 15.0, 10.0), 60.0)
                usage_info["rate_limited"] = True
                _apply_api_cooldown(cooldown)
                # Если в ANYMODELS_MODELS есть другие модели — переключаемся на следующую
                candidates = (ANYMODEL_FREE_MODELS or (ANYMODEL_FALLBACK_MODEL,))
                next_model = next(
                    (m for m in candidates if m not in tried_models and m not in _UNAVAILABLE_MODELS),
                    None
                )
                if next_model:
                    model = next_model
                    tried_models.add(model)
                    payload["model"] = model
                if attempt < MAX_RETRIES:
                    log.warning(
                        "⚠️ AnyModel %s (HTTP %d): кулдаун %.0f с, повтор %d/%d (модель: %s)",
                        "rate limit" if resp.status_code == 429 else "upstream unavailable",
                        resp.status_code, cooldown, attempt, MAX_RETRIES, model
                    )
                    continue
                log.warning(
                    "⚠️ AnyModel %s (HTTP %d): исчерпаны попытки (%d/%d) после кулдауна %.0f с",
                    "rate limit" if resp.status_code == 429 else "upstream unavailable",
                    resp.status_code, attempt, MAX_RETRIES, cooldown
                )
                return None, usage_info
            elif resp.status_code in (500, 502, 503, 504):
                # Транзиентная ошибка шлюза — короткая пауза и один повтор той же модели.
                sleep_time = attempt * 2
                log.warning("⚠️ API HTTP %d, повтор через %.0f сек. (попытка %d/%d)",
                            resp.status_code, sleep_time, attempt, MAX_RETRIES)
                _apply_api_cooldown(min(sleep_time, 5.0))
                if attempt < MAX_RETRIES:
                    time.sleep(sleep_time)
                    continue
                return None, usage_info
            elif resp.status_code in (400, 403, 404, 410, 422):
                # Провайдер отверг именно ЭТУ модель (исчезла — 404/410, не
                # принимает запрос — 400/403/422). Это не общий сбой аккаунта:
                # помечаем модель недоступной и честно перебираем каталог, пока
                # какая-то не ответит, вместо мгновенного error.
                candidates = (ANYMODEL_FREE_MODELS or (ANYMODEL_FALLBACK_MODEL,))
                with _MODEL_STATE_LOCK:
                    _UNAVAILABLE_MODELS.add(model)
                fallback = next((m for m in candidates if m not in tried_models), None)
                if fallback:
                    log.warning("⚠️ Модель %s отклонена провайдером (HTTP %d); пробую следующую: %s",
                                model, resp.status_code, fallback)
                    return call_anymodel_llm(
                        user_content, model=fallback, api_key=api_key,
                        base_url=base_url, system_prompt=system_prompt,
                        max_tokens=max_tokens, _tried_models=tried_models)
                log.error("❌ Все бесплатные модели из ANYMODEL_FREE_MODELS отклонены/недоступны")
                return None, usage_info
            else:
                log.error("❌ Ошибка API HTTP %d: %s", resp.status_code, resp.text[:300])
                return None, usage_info
        except Exception as e:
            log.warning("⚠️ Ошибка сетевого запроса к AnyModel (модель %s, попытка %d/%d): %s",
                        model, attempt, MAX_RETRIES, e)
            # A read timeout means this model is not responding quickly enough
            # for the current workload. Rotate immediately instead of spending
            # another 90 seconds retrying the same endpoint.
            if isinstance(e, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)):
                candidates = (ANYMODEL_FREE_MODELS or (ANYMODEL_FALLBACK_MODEL,))
                with _MODEL_STATE_LOCK:
                    _UNAVAILABLE_MODELS.add(model)
                fallback = next((m for m in candidates if m not in tried_models), None)
                if fallback:
                    log.warning("⚠️ Модель %s не отвечает; пробую следующую бесплатную: %s", model, fallback)
                    return call_anymodel_llm(
                        user_content, model=fallback, api_key=api_key,
                        base_url=base_url, system_prompt=system_prompt,
                        max_tokens=max_tokens, _tried_models=tried_models)
            if attempt < MAX_RETRIES:
                time.sleep(attempt)

    return None, usage_info


def generate_article_summary(title: str, source: str, published: str | None,
                             url: str, full_text: str,
                             model: str = ANYMODEL_MODEL,
                             api_key: str | None = ANYMODEL_KEY,
                             conn_cache: sqlite3.Connection | None = None) -> tuple[str | None, str, dict]:
    """
    Генерирует структурированное саммари для одной статьи с 2-уровневым кэшированием:
    1. Локальный SQLite кэш по хэшу текста (0 токенов расхода при дублях).
    2. Серверный префиксный кэш промпта (Prompt Cache).
    
    Возвращает (summary_text, status, usage_info).
    """
    usage_info = {"prompt_tokens": 0, "cached_tokens": 0, "completion_tokens": 0, "from_local_cache": False}
    if not full_text or len(full_text.strip()) < 30:
        return None, 'error', usage_info

    content_hash = compute_content_hash(model, title, full_text)

    # 1. Проверяем локальный кэш
    if conn_cache:
        try:
            cur = conn_cache.cursor()
            cur.execute("SELECT summary, status FROM llm_cache WHERE content_hash = ?", (content_hash,))
            cached_row = cur.fetchone()
            cur.close()
            if cached_row:
                c_sum, c_stat = cached_row
                usage_info["from_local_cache"] = True
                return c_sum, c_stat, usage_info
        except Exception:
            pass

    # 2. Быстрый двухэтапный классификатор Испании (эвристика 0 мс / микро-запрос)
    is_spain, class_usage = fast_classify_spain(title, source, full_text, model=model, api_key=api_key)
    usage_info["prompt_tokens"] += class_usage.get("prompt_tokens", 0)
    usage_info["completion_tokens"] += class_usage.get("completion_tokens", 0)

    if not is_spain:
        # Если статья не относится к Испании — мгновенно пропускаем без полного запроса к LLM
        res_text, status = "SKIP", 'skipped'
        if conn_cache:
            try:
                conn_cache.execute(
                    """INSERT OR REPLACE INTO llm_cache
                       (content_hash, model, summary, status, prompt_tokens, cached_tokens, completion_tokens)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (content_hash, model, res_text, status,
                     usage_info.get("prompt_tokens", 0),
                     usage_info.get("cached_tokens", 0),
                     usage_info.get("completion_tokens", 0))
                )
                conn_cache.commit()
            except Exception:
                pass
        return res_text, status, usage_info

    # 3. Полная генерация RAG-саммари по 20 правилам ТОЛЬКО для испанских новостей
    trimmed_text = full_text.strip()[:15000]
    user_prompt = f"""TÍTULO: {title or 'Sin título'}
FUENTE: {source or 'Desconocida'}
FECHA: {published or 'Desconocida'}
URL: {url}

TEXTO DEL ARTÍCULO:
{trimmed_text}"""

    summary, api_usage = call_anymodel_llm(user_prompt, model=model, api_key=api_key)
    if api_usage:
        usage_info["prompt_tokens"] += api_usage.get("prompt_tokens", 0)
        usage_info["cached_tokens"] += api_usage.get("cached_tokens", 0)
        usage_info["completion_tokens"] += api_usage.get("completion_tokens", 0)

    if summary is None:
        return None, 'error', usage_info

    clean_sum = summary.strip()
    if clean_sum == "SKIP" or clean_sum.startswith("SKIP\n") or clean_sum.startswith("SKIP "):
        res_text, status = "SKIP", 'skipped'
    elif is_garbage_summary(clean_sum):
        # Модель вернула safety-ярлык / эхо промпта / отказ вместо саммари —
        # не кэшируем, помечаем ошибкой для повторной попытки.
        return None, 'error', usage_info
    else:
        res_text, status = clean_sum, 'done'

    # Сохраняем в локальный кэш
    if conn_cache:
        try:
            conn_cache.execute(
                """INSERT OR REPLACE INTO llm_cache
                   (content_hash, model, summary, status, prompt_tokens, cached_tokens, completion_tokens)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (content_hash, model, res_text, status,
                 usage_info.get("prompt_tokens", 0),
                 usage_info.get("cached_tokens", 0),
                 usage_info.get("completion_tokens", 0))
            )
            conn_cache.commit()
        except Exception:
            pass

    return res_text, status, usage_info


# =================== ПАКЕТНАЯ ОБРАБОТКА НОВОСТЕЙ ===================

def summarize_pending_news(conn: sqlite3.Connection,
                           limit: int | None = None,
                           workers: int = DEFAULT_WORKERS,
                           since_date: str | None = '2026-08-20',
                           model: str = ANYMODEL_MODEL,
                           api_key: str | None = ANYMODEL_KEY,
                           force_resummarize: bool = False,
                           checkpoint_callback=None) -> dict:
    """
    Находит статьи с полным текстом без готового саммари и генерирует для них
    структурированные саммари параллельно с подсчетом экономии кэша токенов.
    По умолчанию обрабатывает статьи начиная с 2026-08-20 (since_date='2026-08-20').
    """
    init_summary_db(conn)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    active_capacity = (
        API_MAX_CONCURRENT
        + (MISTRAL_MAX_CONCURRENT if MISTRAL_API_KEY else 0)
        + (GEMINI_MAX_CONCURRENT if GEMINI_API_KEY else 0)
        + (PROVIDER_MAX_CONCURRENT if OPENROUTER_API_KEY else 0)
        + (PROVIDER_MAX_CONCURRENT if CEREBRAS_API_KEY and not _CEREBRAS_DISABLED else 0)
    )
    workers = min(max(1, workers), max(2, active_capacity * 2))

    key = api_key or ANYMODEL_KEY
    if not key:
        log.warning("⚠️ ANYMODEL_KEY не задан. Генерация саммари пропущена.")
        log.info("👉 Задайте ANYMODEL_KEY в файле .env")
        return {
            "processed": 0, "success": 0, "skipped": 0, "failed": 0,
            "local_cache_hits": 0, "prompt_tokens": 0, "cached_tokens": 0, "completion_tokens": 0
        }

    conditions = [
        "full_text IS NOT NULL",
        "length(full_text) > 30"
    ]
    params = []

    if not force_resummarize:
        conditions.append("(llm_summary IS NULL OR summary_status = 'pending' OR summary_status = 'error')")

    if since_date:
        cond, cond_params = build_since_condition(since_date)
        if cond:
            conditions.append(cond)
            params.extend(cond_params)

    where_clause = " WHERE " + " AND ".join(conditions)
    query = f"""
        SELECT id, source, title, link, published, full_text
        FROM news
        {where_clause}
        ORDER BY id DESC
    """
    if limit:
        query += f" LIMIT {int(limit)}"

    cursor = conn.cursor()
    cursor.execute(query, params)
    articles = cursor.fetchall()
    cursor.close()

    total = len(articles)
    if total == 0:
        log.info("ℹ️ Нет статей, требующих генерации саммари.")
        return {
            "processed": 0, "success": 0, "skipped": 0, "failed": 0,
            "local_cache_hits": 0, "prompt_tokens": 0, "cached_tokens": 0, "completion_tokens": 0
        }

    log.info("🧠 Начинаем суммаризацию %d статей (модель: %s, потоков: %d, включен Prompt Caching)...",
             total, model, workers)
    log.info("⚡ Независимые очереди: Mistral=%d, Gemini=%d, OpenRouter=%d, Cerebras=%d, AnyModel=%d",
             MISTRAL_MAX_CONCURRENT if MISTRAL_API_KEY else 0,
             GEMINI_MAX_CONCURRENT if GEMINI_API_KEY else 0,
             PROVIDER_MAX_CONCURRENT if OPENROUTER_API_KEY else 0,
             PROVIDER_MAX_CONCURRENT if CEREBRAS_API_KEY and not _CEREBRAS_DISABLED else 0,
             API_MAX_CONCURRENT)

    success_cnt = 0
    skipped_cnt = 0
    paywall_cnt = 0
    failed_cnt = 0
    local_cache_hits = 0
    total_prompt_tokens = 0
    total_cached_tokens = 0
    total_completion_tokens = 0

    def process_batch(batch):
        """Pre-filter via heuristics, then classify+summarize in one LLM call."""
        provider_name = _next_provider_name()
        result = {"items": [], "usage": {
            "prompt_tokens": 0, "completion_tokens": 0,
            "cached_tokens": 0, "rate_limited": False}}

        to_summarize = []
        for art in batch:
            aid, source, title, link, published, full_text = art
            body, body_len = effective_body(full_text)
            if body_len < MIN_BODY_LEN:
                result["items"].append((aid, title, "PAYWALL", "paywall"))
                continue
            sample = f"{title or ''} {body[:1200]}".lower()
            # Коммерческий спам и явно-международные без испанского контекста — 0 токенов
            if COMMERCIAL_PATTERNS.search(sample):
                result["items"].append((aid, title, "SKIP", "skipped"))
                continue
            if PURE_INTERNATIONAL_PATTERNS.search(sample[:500]) and not SPAIN_POSITIVE_PATTERNS.search(sample):
                result["items"].append((aid, title, "SKIP", "skipped"))
                continue
            to_summarize.append(art)

        if not to_summarize:
            return result

        log.info("🧠 Классификация+саммари: провайдер=%s, в LLM=%d/%d (эвристика отсеяла %d)",
                 provider_name, len(to_summarize), len(batch), len(batch) - len(to_summarize))

        for offset in range(0, len(to_summarize), SUMMARY_BATCH_SIZE):
            group = to_summarize[offset:offset + SUMMARY_BATCH_SIZE]
            summaries, batch_usage = summarize_articles_provider(
                group, provider_name=provider_name, model=model, api_key=key)
            for name in result["usage"]:
                if name == "rate_limited":
                    result["usage"][name] = result["usage"][name] or batch_usage.get(name, False)
                else:
                    result["usage"][name] += batch_usage.get(name, 0)
            for art in group:
                aid, source, title, link, published, full_text = art
                if aid in summaries:
                    sum_text, status = summaries[aid]
                else:
                    local_conn = sqlite3.connect(DB_FILE)
                    try:
                        sum_text, status, single_usage = generate_article_summary(
                            title, source, published, link, full_text,
                            model=model, api_key=key, conn_cache=local_conn)
                    finally:
                        local_conn.close()
                    for name in result["usage"]:
                        if name != "rate_limited":
                            result["usage"][name] += single_usage.get(name, 0)
                result["items"].append((aid, title, sum_text, status))
        return result

    start_time = time.time()
    batches = [articles[i:i + CLASSIFICATION_BATCH_SIZE]
               for i in range(0, len(articles), CLASSIFICATION_BATCH_SIZE)]
    executor = ThreadPoolExecutor(max_workers=workers)
    futures = {executor.submit(process_batch, batch): batch for batch in batches}
    interrupted = False
    try:
        idx = 0
        for future in as_completed(futures):
            try:
                batch_result = future.result()
                usage = batch_result["usage"]
                total_prompt_tokens += usage.get("prompt_tokens", 0)
                total_cached_tokens += usage.get("cached_tokens", 0)
                total_completion_tokens += usage.get("completion_tokens", 0)
                for aid, atitle, sum_text, status in batch_result["items"]:
                    idx += 1
                    conn.execute("UPDATE news SET llm_summary = ?, summary_status = ? WHERE id = ?",
                                 (sum_text, status, aid))
                    # Сразу очищаем full_text после успешного саммари — он больше не нужен
                    if status == 'done':
                        conn.execute("UPDATE news SET full_text = NULL WHERE id = ?", (aid,))
                    # Persist every article immediately. A stopped process can
                    # lose only currently running API work, never completed rows.
                    conn.commit()
                    if status == 'done':
                        success_cnt += 1
                        status_icon = "✅"
                    elif status == 'skipped':
                        skipped_cnt += 1
                        status_icon = "⏩ SKIP"
                    elif status == 'paywall':
                        paywall_cnt += 1
                        status_icon = "🔒 PAYWALL"
                    else:
                        failed_cnt += 1
                        status_icon = "❌ Ошибка"
                    log.info("[%d/%d] %s ID=%d: %s", idx, total, status_icon, aid, (atitle or '')[:40])
                    if idx % 100 == 0:
                        log.info("💾 Контрольная точка: %d статей надёжно записано в базу", idx)
                        if checkpoint_callback:
                            try:
                                checkpoint_callback(conn, idx)
                            except Exception as exc:
                                log.error("❌ Ошибка контрольного экспорта после %d статей: %s", idx, exc)
                sys.stdout.flush()

            except Exception as e:
                batch = futures[future]
                failed_cnt += len(batch)
                log.error("❌ Исключение при обработке пакета (IDs %s): %s",
                          [a[0] for a in batch], e)
                sys.stdout.flush()
    except KeyboardInterrupt:
        interrupted = True
        conn.commit()
        for future in futures:
            future.cancel()
        log.warning("⏹ Остановка: %d завершённых статей сохранено; ожидающие задачи отменены", idx)
    finally:
        executor.shutdown(wait=not interrupted, cancel_futures=interrupted)

    if interrupted:
        raise KeyboardInterrupt

    total_time = time.time() - start_time
    avg_speed = total / total_time if total_time > 0 else 0
    log.info("🎉 Суммаризация завершена за %.1f сек (%.1f ст/сек): успешно=%d, SKIP=%d, пейволл=%d, ошибок=%d",
             total_time, avg_speed, success_cnt, skipped_cnt, paywall_cnt, failed_cnt)
    if total_cached_tokens > 0 or local_cache_hits > 0:
        log.info("📊 Экономия токенов: локальных попаданий=%d, серверных кэш-токенов=%d",
                 local_cache_hits, total_cached_tokens)

    return {
        "processed": total,
        "success": success_cnt,
        "skipped": skipped_cnt,
        "paywall": paywall_cnt,
        "failed": failed_cnt,
        "local_cache_hits": local_cache_hits,
        "prompt_tokens": total_prompt_tokens,
        "cached_tokens": total_cached_tokens,
        "completion_tokens": total_completion_tokens
    }


# =================== CLI ===================

def main():
    parser = argparse.ArgumentParser(description="Интеллектуальная суммаризация новостей Испании для NotebookLM с кэшированием")
    parser.add_argument('--db', default=DB_FILE, help="Путь к SQLite базе данных")
    parser.add_argument('--today', action='store_true', help="Обработать только статьи за сегодня")
    parser.add_argument('--since', type=str, default='2026-08-20', help="Начальная дата (YYYY-MM-DD или 'today', по умолчанию '2026-08-20')")
    parser.add_argument('--all-time', action='store_true', help="Обработать всю архивную базу без фильтра по дате")
    parser.add_argument('--limit', type=int, default=None, help="Максимальное количество статей")
    parser.add_argument('--workers', type=int, default=DEFAULT_WORKERS, help="Количество параллельных потоков")
    parser.add_argument('--model', default=ANYMODEL_MODEL, help=f"LLM модель (по умолчанию {ANYMODEL_MODEL})")
    parser.add_argument('--force', action='store_true', help="Принудительно перегенерировать существующие саммари")
    args = parser.parse_args()

    since_date = args.since
    if args.all_time:
        since_date = None
    elif args.today:
        since_date = 'today'

    if not os.path.exists(args.db):
        log.error("❌ База данных не найдена: %s", args.db)
        sys.exit(1)

    with sqlite3.connect(args.db) as conn:
        stats = summarize_pending_news(
            conn=conn,
            limit=args.limit,
            workers=args.workers,
            since_date=since_date,
            model=args.model,
            force_resummarize=args.force
        )

    print("\n" + "=" * 55)
    print("📊 ИТОГ СУММАРИЗАЦИИ НОВОСТЕЙ:")
    print(f"   Всего обработано:         {stats['processed']}")
    print(f"   Успешно создано:          {stats['success']}")
    print(f"   Пропущено (SKIP):         {stats['skipped']}")
    print(f"   Ошибок:                   {stats['failed']}")
    print("   -------------------------------------------------")
    print(f"   ⚡ Локальных попаданий в кэш: {stats['local_cache_hits']} (0 токенов)")
    print(f"   🎯 Серверный кэш промпта:    {stats['cached_tokens']} токенов")
    print(f"   📥 Входных токенов (Prompt): {stats['prompt_tokens']}")
    print(f"   📤 Выходных токенов (Gen):   {stats['completion_tokens']}")
    print("=" * 55)


if __name__ == '__main__':
    main()
