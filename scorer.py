#!/usr/bin/env python3
"""
Модуль редакционной классификации и LLM-скоринга новостей Испании.

Архитектура:
1. LLM как независимый аналитик: оценивает 10 шкал (0-10), штрафы, категории,
   аудиторию, локальность, форматы, причины (why) и редакционные углы (editorial_angles).
2. Python как калькулятор: рассчитывает NEWS SCORE и CONTENT SCORE по взвешенной
   формуле, применяет штрафы и присваивает тиры (TOP, HIGH, GOOD, MAYBE, SKIP).
3. Вход: SQLite baza.db (новости со статусом done), Markdown-дайджесты
   (NewsRAG/NOTEBOOKLM/*.md) или JSON.
4. Выход: отсортированный scored.json и кэширование в таблицу news_scores в baza.db.

Использование:
  python3 scorer.py --limit 50 --out scored.json --show-top
  python3 scorer.py --today --top 20
  python3 scorer.py --input NewsRAG/NOTEBOOKLM/2026-09-02.md --limit 10
  python3 scorer.py --provider gemini --model gemini-flash-lite-latest
"""

import os
import sys
import re
import json
import sqlite3
import logging
import argparse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Literal, Optional, List, Dict, Any

from pydantic import BaseModel, Field, ValidationError
from date_filter import build_since_condition
from telegram_notifier import send_top_articles_to_telegram

# =================== КОНФИГУРАЦИЯ И ОКРУЖЕНИЕ ===================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(SCRIPT_DIR, 'baza.db')
LOG_FILE = os.path.join(SCRIPT_DIR, 'scorer.log')
DEFAULT_OUTPUT_FILE = os.path.join(SCRIPT_DIR, 'scored.json')

def load_env_file():
    """Загружает переменные окружения из .env."""
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

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding='utf-8')
    ]
)
log = logging.getLogger("scorer")

# =================== ВЕСА И ФОРМУЛЫ СКОРИНГА ===================

NEWS_WEIGHTS = {
    "resident_relevance": 0.20,
    "practical_value": 0.20,
    "impact": 0.15,
    "content_potential": 0.10,
    "immigration_relevance": 0.10,
    "novelty": 0.10,
    "discussion": 0.05,
    "urgency": 0.05,
    "emotion": 0.05,
}

CONTENT_WEIGHTS = {
    "content_potential": 0.35,
    "evergreen": 0.25,
    "practical_value": 0.20,
    "discussion": 0.20,
}

TIER_THRESHOLDS = [
    (9.0, "🔥 TOP"),
    (8.0, "🟠 HIGH"),
    (7.0, "🟡 GOOD"),
    (6.0, "⚪ MAYBE"),
    (0.0, "❌ SKIP"),
]

# =================== PYDANTIC СХЕМЫ ДАННЫХ ===================

CategoryType = Literal[
    "housing", "cost_of_living", "immigration", "documents", "taxes",
    "jobs", "salaries", "healthcare", "education", "family", "transport",
    "cars", "banking", "consumer", "politics", "economy", "crime",
    "society", "culture", "travel", "events", "weather", "interesting_spain", "statistics"
]

AudienceType = Literal[
    "resident", "immigrant", "potential_immigrant", "family", "tourist", "general"
]

LocalityType = Literal[
    "national", "regional", "city", "local"
]

FormatType = Literal[
    "news", "breaking", "explainer", "how_to", "numbers", "comparison",
    "guide", "poll", "opinion", "interesting_fact"
]


class ScoresBreakdown(BaseModel):
    resident_relevance: int = Field(default=0, ge=0, le=10, description="Важность для тех, кто уже живёт в Испании")
    immigration_relevance: int = Field(default=0, ge=0, le=10, description="Интерес для иностранцев, NIE/TIE, residencia, переезд")
    practical_value: int = Field(default=0, ge=0, le=10, description="Практическая польза (можно ли применить на практике)")
    impact: int = Field(default=0, ge=0, le=10, description="Количество людей + сила воздействия")
    novelty: int = Field(default=0, ge=0, le=10, description="Новизна информации, а не очередное повторение")
    emotion: int = Field(default=0, ge=0, le=10, description="Эмоциональная составляющая (удивление, возмущение, юмор)")
    discussion: int = Field(default=0, ge=0, le=10, description="Потенциал споров, комментариев и обсуждений")
    urgency: int = Field(default=0, ge=0, le=10, description="Срочность (нужно ли публиковать прямо сейчас)")
    content_potential: int = Field(default=0, ge=0, le=10, description="Потенциал создания качественного контента")
    evergreen: int = Field(default=0, ge=0, le=10, description="Долгосрочная полезность материала")


class PenaltiesBreakdown(BaseModel):
    too_local: float = Field(0.0, ge=0.0, le=5.0, description="Слишком локальное событие (улица, село)")
    repetitive: float = Field(0.0, ge=0.0, le=5.0, description="Заезженная тема без новых фактов")
    clickbait: float = Field(0.0, ge=0.0, le=5.0, description="Кликбейтный заголовок / пустой инфоповод")
    low_credibility: float = Field(0.0, ge=0.0, le=5.0, description="Слухи, сомнительный источник")
    too_political: float = Field(0.0, ge=0.0, le=5.0, description="Чистая политическая перепалка без практического эффекта")


class EditorialClassification(BaseModel):
    scores: ScoresBreakdown
    penalties: PenaltiesBreakdown = Field(default_factory=PenaltiesBreakdown)
    categories: List[str] = Field(default_factory=list, description="Категории из фиксированного списка")
    audience: List[str] = Field(default_factory=list, description="Целевая аудитория")
    locality: str = Field(default="national", description="Масштаб новости")
    formats: List[str] = Field(default_factory=list, description="Рекомендуемые форматы публикаций")
    why: List[str] = Field(default_factory=list, description="Краткие причины оценки")
    editorial_angles: List[str] = Field(default_factory=list, description="Под каким углом подавать новость")
    confidence: float = Field(default=0.9, ge=0.0, le=1.0, description="Уверенность в оценке")


class BatchItemResult(BaseModel):
    id: Optional[int] = None
    scores: ScoresBreakdown
    penalties: PenaltiesBreakdown = Field(default_factory=PenaltiesBreakdown)
    categories: List[str] = Field(default_factory=list)
    audience: List[str] = Field(default_factory=list)
    locality: str = Field(default="national")
    formats: List[str] = Field(default_factory=list)
    why: List[str] = Field(default_factory=list)
    editorial_angles: List[str] = Field(default_factory=list)
    confidence: float = Field(default=0.9, ge=0.0, le=1.0)


class BatchEditorialClassification(BaseModel):
    results: List[BatchItemResult]


class ScoredArticle(BaseModel):
    id: Optional[int] = None
    title: str
    summary: str
    source: Optional[str] = None
    url: Optional[str] = None
    published_at: Optional[str] = None
    country: str = "Spain"
    editorial: EditorialClassification
    news_score: float
    content_score: float
    tier: str
    is_top: bool
    is_evergreen_gem: bool
    is_breaking: bool
    scored_at: str


# =================== СИСТЕМНЫЙ ПРОМПТ РЕДАКЦИОННОГО КЛАССИФИКАТОРА ===================

SCORING_SYSTEM_PROMPT = """Ты — опытный главный редактор крупного Telegram-канала о жизни в Испании.

Твоя аудитория:
- русскоязычные жители Испании (экспаты, резиденты, семьи);
- иностранцы, легализующиеся или живущие в Испании;
- люди, планирующие переезд (digital nomad, no lucrativa, студенты, инвесторы);
- семьи с детьми в Испании.

Твоя задача — разобрать новость по независимым критериям и вернуть строгий JSON.
Ты НЕ считаешь итоговый математический балл. Твоя задача — объективно оценить каждую характеристику по шкале 0–10.

ВАЖНЫЕ ПРАВИЛА:
1. Оценивай новость для НАШЕЙ аудитории (русскоязычные в Испании), а не для абстрактного испанского читателя.
2. Не завышай оценки. 9 и 10 — это исключительные события, влияющие на жизнь сотен тысяч людей.
3. Политическая перепалка партий (PP, PSOE, Vox) БЕЗ изменений законов — это низкий resident_relevance и штраф too_political.
4. Оценивай каждую шкалу НЕЗАВИСИМО от остальных.

ЯКОРНЫЕ ОЦЕНКИ ДЛЯ ШКАЛ (0-10):

1. resident_relevance (важность для тех, кто УЖЕ живёт в Испании):
   - 0–2: не касается жителей (курьёз, местный фестиваль, спорт, отвлечённая политика).
   - 3–4: интересно лишь узкой группе (отдельная профессия, локальный регламент).
   - 5–6: касается заметной группы (автомобилисты региона, школьники автономии).
   - 7–8: касается большого числа жителей (рост тарифов, инфляция, изменения в правилах аренды).
   - 9–10: касается практически всех жителей (налоговая реформа, трудовое законодательство, резкий скачок цен на электричество/продукты).

2. immigration_relevance (визы, ВНЖ, NIE/TIE, гражданство, переезд):
   - 0–2: вообще не связано с иностранцами.
   - 3–4: косвенно может коснуться иностранцев так же, как и испанцев.
   - 5–6: касается экспатов (банковские счета, требования к доходам, аренда без контракта).
   - 7–8: важные процедуры (запись на cita previa, новые требования к документам, правила продления).
   - 9–10: реформа миграционного регламента (Reglamento de Extranjería), амнистия, новые законы о гражданстве или визах кочевников.

3. practical_value (главный критерий: может ли читатель сделать что-то полезное?):
   - 0–2: чисто теоретическая новость или декларации политиков («правительство продолжит работать»).
   - 3–4: фоновые знания («в Испании любят пить кофе по утрам»).
   - 5–6: полезная информация к сведению (сроки подачи декларации IRPF, изменение дат каникул).
   - 7–8: конкретное действие (как получить субсидию на транспорт, как избежать нового штрафа DGT).
   - 9–10: прямая инструкция к выгоде или спасению от крупных штрафов/потерь (как вернуть налог за декрет, новые правила аренды жилья).

4. impact (масштаб людей + сила эффекта):
   - 0–2: один человек, село или закрытие одной лавки.
   - 3–4: город или отдельный небольшой сектор экономики.
   - 5–6: автономия (Мадрид, Каталония, Андалусия) или крупная отрасль.
   - 7–8: миллионы людей в Испании.
   - 9–10: масштабы всей страны, системные изменения.

5. novelty (новизна факта):
   - 0–2: 10-й пересказ одной и той же темы за неделю.
   - 3–4: дежурные сезонные колебания (летом жарко, зимой дожди).
   - 5–6: свежие уточнённые цифры по известному тренду.
   - 7–8: новый прецедент или неожиданное решение суда/министерства.
   - 9–10: сенсационный поворот или ранее не публиковавшаяся информация.

6. emotion (эмоциональный заряд):
   - 0–2: сухой протокол, бюрократический бюллетень.
   - 3–4: спокойный нейтральный тон.
   - 5–6: вызывает любопытство или лёгкое удивление.
   - 7–8: сильное возмущение, сочувствие или радость.
   - 9–10: взрывной эмоциональный триггер, шок, курьёз года.

7. discussion (потенциал для споров и комментариев):
   - 0–2: комментировать нечего («открыли новую станцию метро»).
   - 3–4: пара формальных мнений.
   - 5–6: вызовет интерес и несколько личных историй читателей.
   - 7–8: полярные мнения (окупасы, туристические квартиры, налоги для богатых, школьная программа).
   - 9–10: горячая тема, гарантирующая сотни комментариев и ожесточённые споры.

8. urgency (срочность):
   - 0–2: «вечнозелёная» тема, не горит.
   - 3–4: в течение недели-двух.
   - 5–6: актуально в течение 24–48 часов.
   - 7–8: срочно сегодня (забастовка транспорта завтра, дедлайн подачи документов).
   - 9–10: молния / breaking (ЧП, срочное перекрытие трасс, решение принято 15 минут назад).

9. content_potential (можно ли сделать крутой пост, опрос, инфографику):
   - 0–2: только скучная новостная заметка на 2 абзаца.
   - 3–4: стандартный пост с фото.
   - 5–6: хороший пост с полезными пояснениями.
   - 7–8: отличный материал: сравнение с другими странами, опрос аудитории, карточки.
   - 9–10: флагманская публикация: лонгрид, пошаговый гайд, калькулятор расходов, дискуссионный пост.

10. evergreen (актуальность через месяц / полгода):
   - 0–2: новость-однодневка (ДТП, прогноз погоды на завтра, вчерашний матч).
   - 3–4: актуально до конца текущего месяца.
   - 5–6: актуально несколько месяцев (сезонные скидки, тренд года).
   - 7–8: полезно сохранять в закладки (инструкция по проверке счета, список льгот).
   - 9–10: нестареющий гайд (как работает empadronamiento, как арендовать квартиру в Испании).

ШТРАФЫ (penalties, от 0.0 до 2.0):
- too_local: новость про конкретную деревню, улицу или местное происшествие без значения для страны (например, 1.0–2.0).
- repetitive: тема многократно муссировалась за последние дни без новой фактуры (0.5–1.5).
- clickbait: броский заголовок, за которым нет реального содержания (1.0–2.0).
- low_credibility: слухи, анонимные утечки, жёлтая пресса (1.0–2.0).
- too_political: чистый пиар политиков, взаимные обвинения без реальных последствий для граждан (1.0–1.5).

РАЗРЕШЁННЫЕ ЗНАЧЕНИЯ:
- categories (максимум 3): ["housing", "cost_of_living", "immigration", "documents", "taxes", "jobs", "salaries", "healthcare", "education", "family", "transport", "cars", "banking", "consumer", "politics", "economy", "crime", "society", "culture", "travel", "events", "weather", "interesting_spain", "statistics"]
- audience: ["resident", "immigrant", "potential_immigrant", "family", "tourist", "general"]
- locality: "national" | "regional" | "city" | "local"
- formats: ["news", "breaking", "explainer", "how_to", "numbers", "comparison", "guide", "poll", "opinion", "interesting_fact"]

Формат ответа — ТОЛЬКО чистый валидный JSON следующей структуры:
{
  "scores": {
    "resident_relevance": 9,
    "immigration_relevance": 4,
    "practical_value": 8,
    "impact": 8,
    "novelty": 7,
    "emotion": 5,
    "discussion": 8,
    "urgency": 4,
    "content_potential": 9,
    "evergreen": 7
  },
  "penalties": {
    "too_local": 0.0,
    "repetitive": 0.0,
    "clickbait": 0.0,
    "low_credibility": 0.0,
    "too_political": 0.0
  },
  "categories": ["housing", "cost_of_living"],
  "audience": ["resident", "family"],
  "locality": "national",
  "formats": ["news", "explainer", "numbers"],
  "why": [
    "затрагивает большинство арендаторов в Испании",
    "прямое финансовое влияние на ежемесячные расходы",
    "высокий интерес со стороны семей с детьми"
  ],
  "editorial_angles": [
    "что изменится для арендаторов с 1 числа",
    "сколько теперь придётся доплачивать в среднем по автономиям",
    "что делать, если арендодатель требует больше закона"
  ],
  "confidence": 0.95
}
"""

# =================== КАЛЬКУЛЯТОР БАЛЛОВ В PYTHON ===================

def calculate_article_scores(editorial: EditorialClassification) -> tuple[float, float, str, bool, bool, bool]:
    """
    Вычисляет news_score и content_score на основе оценок LLM и штрафов.
    Возвращает (news_score, content_score, tier, is_top, is_evergreen_gem, is_breaking).
    """
    scores = editorial.scores.model_dump()
    penalties = editorial.penalties.model_dump()

    # 1. Сырой news_score по весам
    raw_news_score = sum(scores[key] * weight for key, weight in NEWS_WEIGHTS.items())

    # 2. Вычитание штрафов
    total_penalty = sum(penalties.values())
    news_score = max(0.0, min(10.0, round(raw_news_score - total_penalty, 2)))

    # 3. Расчёт content_score для поиска виральных тем и гайдов
    content_score = round(sum(scores[key] * weight for key, weight in CONTENT_WEIGHTS.items()), 2)

    # 4. Определение тира
    tier = "❌ SKIP"
    for threshold, tier_name in TIER_THRESHOLDS:
        if news_score >= threshold:
            tier = tier_name
            break

    # 5. Флаги
    is_top = news_score >= 9.0
    is_evergreen_gem = (content_score >= 8.5 or scores["content_potential"] >= 9)
    is_breaking = scores["urgency"] >= 8

    return news_score, content_score, tier, is_top, is_evergreen_gem, is_breaking


# =================== КЛИЕНТЫ К LLM API ===================

import time
import threading

_GEMINI_LOCK = threading.Lock()
_LAST_GEMINI_CALL = 0.0
GEMINI_MIN_INTERVAL = 4.1  # 15 запросов в минуту = 1 запрос каждые 4 сек

def call_gemini_scorer(prompt_payload: str, model_name: str = "gemini-flash-lite-latest") -> Optional[EditorialClassification]:
    """Вызов Gemini API через google.genai с авто-пейсингом (15 RPM) и гарантированным JSON."""
    global _LAST_GEMINI_CALL
    from google import genai
    from google.genai import types

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        log.error("❌ Не задан GEMINI_API_KEY в .env")
        return None

    client = genai.Client(api_key=api_key)

    for attempt in range(1, 4):
        # Контроль частоты запросов для Free Tier (15 RPM)
        with _GEMINI_LOCK:
            now = time.time()
            diff = now - _LAST_GEMINI_CALL
            if diff < GEMINI_MIN_INTERVAL:
                time.sleep(GEMINI_MIN_INTERVAL - diff)
            _LAST_GEMINI_CALL = time.time()

        try:
            response = client.models.generate_content(
                model=model_name,
                contents=[
                    types.Content(role="user", parts=[types.Part.from_text(text=prompt_payload)])
                ],
                config=types.GenerateContentConfig(
                    system_instruction=SCORING_SYSTEM_PROMPT,
                    response_mime_type="application/json",
                    temperature=0.1,
                )
            )
            if not response or not response.text:
                return None

            clean_text = response.text.strip()
            data = json.loads(clean_text)
            return EditorialClassification.model_validate(data)
        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                log.warning("⚠️ Gemini 429 rate limit (попытка %d/3), ждём 8 сек...", attempt)
                time.sleep(8)
                continue
            log.warning("⚠️ Ошибка вызова Gemini scorer: %s", e)
            return None
    return None


def call_openai_compatible_raw(
    prompt_payload: str,
    base_url: str,
    api_key: str,
    model_name: str,
    system_prompt: str = SCORING_SYSTEM_PROMPT
) -> Optional[dict]:
    """Выполняет запрос к OpenAI-совместимому API и возвращает распарсенный JSON dict."""
    import requests

    endpoint = f"{base_url.rstrip('/')}/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": model_name,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt_payload}
        ],
        "temperature": 0.1,
        "response_format": {"type": "json_object"}
    }
    try:
        r = requests.post(endpoint, headers=headers, json=payload, timeout=25)
        if r.status_code != 200:
            log.warning("⚠️ API (%s, %s) вернул статус %d: %s", base_url, model_name, r.status_code, r.text[:200])
            return None
        data = r.json()
        choices = data.get("choices") or []
        if not choices:
            return None
        content = choices[0].get("message", {}).get("content", "")
        
        # Удаляем markdown-блоки ```json ... ``` если модель их добавила
        clean_text = re.sub(r"^```json\s*", "", content.strip(), flags=re.IGNORECASE)
        clean_text = re.sub(r"\s*```$", "", clean_text)
        
        return json.loads(clean_text)
    except Exception as e:
        log.warning("⚠️ Ошибка OpenAI-совместимого вызова (%s): %s", model_name, e)
        return None


def call_openai_compatible_scorer(
    prompt_payload: str,
    base_url: str,
    api_key: str,
    model_name: str
) -> Optional[EditorialClassification]:
    """Вызов OpenAI-совместимого эндпоинта (AnyModel, OpenRouter, Mistral) для одной статьи."""
    parsed_json = call_openai_compatible_raw(prompt_payload, base_url, api_key, model_name)
    if parsed_json:
        try:
            return EditorialClassification.model_validate(parsed_json)
        except Exception as e:
            log.warning("⚠️ Ошибка валидации EditorialClassification: %s", e)
    return None


def score_single_news(
    article: Dict[str, Any],
    provider: str = "auto",
    model: Optional[str] = None
) -> Optional[ScoredArticle]:
    """
    Классифицирует одну новость через LLM и считает баллы в Python.
    """
    article_payload = json.dumps({
        "id": article.get("id"),
        "title": article.get("title", ""),
        "summary": article.get("summary", ""),
        "source": article.get("source", ""),
        "published_at": article.get("published_at") or article.get("published"),
        "url": article.get("url") or article.get("link"),
        "country": "Spain"
    }, ensure_ascii=False, indent=2)

    prompt = f"Оцени следующую новость:\n\n{article_payload}"

    editorial: Optional[EditorialClassification] = None

    # 1. Приоритетный вызов AnyModel (основной провайдер проекта)
    if provider in ("anymodel", "auto") and os.environ.get("ANYMODEL_KEY"):
        target_model = model or os.environ.get("ANYMODEL_MODEL") or "ag/gemini-2.5-flash"
        base_url = os.environ.get("ANYMODEL_BASE_URL", "https://anymodel.org/v1")
        api_key = os.environ["ANYMODEL_KEY"]
        
        editorial = call_openai_compatible_scorer(
            prompt,
            base_url=base_url,
            api_key=api_key,
            model_name=target_model
        )
        # Если модель перегружена, быстрый fallback на вторую бесплатную модель AnyModel
        if not editorial and not model:
            fallback_model = "ag/gemini-3.1-flash-lite-preview"
            editorial = call_openai_compatible_scorer(
                prompt,
                base_url=base_url,
                api_key=api_key,
                model_name=fallback_model
            )

    # 2. Резервный вызов OpenRouter
    if not editorial and (provider in ("openrouter", "auto") and os.environ.get("OPENROUTER_API_KEY")):
        target_model = model or "google/gemini-2.5-flash-lite"
        editorial = call_openai_compatible_scorer(
            prompt,
            base_url="https://openrouter.ai/api/v1",
            api_key=os.environ["OPENROUTER_API_KEY"],
            model_name=target_model
        )

    # 3. Резервный вызов Google AI Studio (Free tier ограничен 15 RPM)
    if not editorial and (provider in ("gemini", "auto") and os.environ.get("GEMINI_API_KEY")):
        target_model = model or "gemini-flash-lite-latest"
        editorial = call_gemini_scorer(prompt, model_name=target_model)

    # 4. Резервный вызов Mistral
    if not editorial and (provider in ("mistral", "auto") and os.environ.get("MISTRAL_API_KEY")):
        target_model = model or "mistral-small-latest"
        editorial = call_openai_compatible_scorer(
            prompt,
            base_url=os.environ.get("MISTRAL_BASE_URL", "https://api.mistral.ai/v1"),
            api_key=os.environ["MISTRAL_API_KEY"],
            model_name=target_model
        )

    if not editorial:
        log.error("❌ Не удалось получить классификацию для статьи id=%s title=%s",
                  article.get("id"), article.get("title", "")[:50])
        return None

    news_score, content_score, tier, is_top, is_evergreen, is_breaking = calculate_article_scores(editorial)

    return ScoredArticle(
        id=article.get("id"),
        title=article.get("title", ""),
        summary=article.get("summary", ""),
        source=article.get("source"),
        url=article.get("url") or article.get("link"),
        published_at=article.get("published_at") or article.get("published"),
        country="Spain",
        editorial=editorial,
        news_score=news_score,
        content_score=content_score,
        tier=tier,
        is_top=is_top,
        is_evergreen_gem=is_evergreen,
        is_breaking=is_breaking,
        scored_at=datetime.now(timezone.utc).isoformat()
    )


# =================== ЗАГРУЗЧИКИ ДАННЫХ ===================

def init_scoring_db(conn: sqlite3.Connection):
    """Инициализирует таблицу news_scores для кэширования оценок."""
    with conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS news_scores (
                news_id INTEGER PRIMARY KEY,
                news_score REAL NOT NULL,
                content_score REAL NOT NULL,
                tier TEXT NOT NULL,
                is_top INTEGER NOT NULL,
                is_evergreen INTEGER NOT NULL,
                scores_json TEXT NOT NULL,
                scored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (news_id) REFERENCES news(id) ON DELETE CASCADE
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_news_scores_tier ON news_scores(tier)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_news_scores_score ON news_scores(news_score DESC)")


def load_articles_from_db(
    conn: sqlite3.Connection,
    limit: Optional[int] = None,
    since_date: Optional[str] = None,
    skip_scored: bool = True,
    article_ids: Optional[List[int]] = None
) -> List[Dict[str, Any]]:
    """Загружает готовые статьи с саммари из базы baza.db."""
    query = """
        SELECT n.id, n.title, n.source, n.link, n.published, n.llm_summary
        FROM news n
    """
    conditions = ["n.summary_status = 'done'", "n.llm_summary IS NOT NULL", "length(n.llm_summary) > 20"]
    params = []

    if article_ids:
        placeholders = ",".join("?" for _ in article_ids)
        conditions.append(f"n.id IN ({placeholders})")
        params.extend(article_ids)
    elif skip_scored:
        query += " LEFT JOIN news_scores s ON n.id = s.news_id"
        conditions.append("s.news_id IS NULL")

    if since_date:
        date_cond, date_params = build_since_condition(since_date)
        if date_cond:
            conditions.append(date_cond)
            params.extend(date_params)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY n.id DESC"

    if limit and limit > 0:
        query += f" LIMIT {int(limit)}"

    cursor = conn.cursor()
    cursor.execute(query, params)
    rows = cursor.fetchall()

    articles = []
    for r in rows:
        articles.append({
            "id": r[0],
            "title": r[1] or "",
            "source": r[2] or "",
            "url": r[3] or "",
            "published_at": r[4],
            "summary": r[5] or "",
            "country": "Spain"
        })
    return articles


def load_articles_from_markdown(file_path: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Парсит дайджест-файл Markdown (например, NewsRAG/NOTEBOOKLM/2026-09-02.md)."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Файл не найден: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Разделяем по заголовкам ## [YYYY-MM-DD] Title
    blocks = re.split(r"\n(?=##\s+\[\d{4}-\d{2}-\d{2}\])", content)
    articles = []

    for block in blocks:
        if not block.strip().startswith("##"):
            continue

        lines = [line.strip() for line in block.strip().split("\n") if line.strip()]
        header = lines[0]
        m_title = re.match(r"##\s+\[([\d\-]+)\]\s+(.*)", header)
        date_str = m_title.group(1) if m_title else None
        title = m_title.group(2) if m_title else header.lstrip("# ")

        art_id = None
        source = None
        url = None
        summary_lines = []

        for line in lines[1:]:
            m_id = re.search(r"\*\*(?:Article\s+ID|ID):\*\*\s*(\d+)", line, re.I)
            m_src = re.search(r"\*\*Source:\*\*\s*([^\s]+)", line, re.I)
            m_url = re.search(r"\*\*Original\s+URL:\*\*\s*(?:\[[^\]]*\]\()?([^\)\s]+)", line, re.I)

            if m_id:
                art_id = int(m_id.group(1))
            elif m_src:
                source = m_src.group(1)
            elif m_url:
                url = m_url.group(1).rstrip(")")
            elif not line.startswith("---") and not line.startswith("- **"):
                summary_lines.append(line)

        summary = " ".join(summary_lines).strip()
        if summary and len(summary) > 20:
            articles.append({
                "id": art_id,
                "title": title,
                "source": source,
                "url": url,
                "published_at": date_str,
                "summary": summary,
                "country": "Spain"
            })

        if limit and len(articles) >= limit:
            break

    return articles


def save_score_to_db(conn: sqlite3.Connection, scored: ScoredArticle):
    """Сохраняет результат скоринга в таблицу news_scores."""
    if not scored.id:
        return
    with conn:
        conn.execute("""
            INSERT OR REPLACE INTO news_scores 
            (news_id, news_score, content_score, tier, is_top, is_evergreen, scores_json, scored_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            scored.id,
            scored.news_score,
            scored.content_score,
            scored.tier,
            1 if scored.is_top else 0,
            1 if scored.is_evergreen_gem else 0,
            scored.editorial.model_dump_json(),
            scored.scored_at
        ))


def score_articles_chunk(
    chunk: List[Dict[str, Any]],
    provider: str = "anymodel",
    model: Optional[str] = None
) -> List[ScoredArticle]:
    """
    Пакетно скорит пачку статей за 1 LLM запрос.
    При ошибке или неполном ответе автоматически падает на индивидуальный скоринг.
    """
    if not chunk:
        return []
    if len(chunk) == 1:
        res = score_single_news(chunk[0], provider, model)
        return [res] if res else []

    compact_list = []
    id_map = {}
    for idx, art in enumerate(chunk):
        art_id = art.get("id") or (idx + 1)
        id_map[art_id] = art
        compact_list.append({
            "id": art_id,
            "title": art.get("title", ""),
            "summary": art.get("summary", ""),
            "source": art.get("source", ""),
            "published_at": art.get("published_at") or art.get("published"),
            "country": "Spain"
        })

    prompt = (
        f"Оцени каждую из следующих {len(compact_list)} новостей независимо для русскоязычной аудитории в Испании.\n"
        f"Верни JSON строго с ключом 'results', содержащим массив объектов с 'id' и оценками для каждой новости:\n\n"
        f"{json.dumps(compact_list, ensure_ascii=False, indent=2)}"
    )

    batch_result = None

    # 1. AnyModel
    if provider in ("anymodel", "auto") and os.environ.get("ANYMODEL_KEY"):
        target_model = model or os.environ.get("ANYMODEL_MODEL") or "ag/gemini-2.5-flash"
        base_url = os.environ.get("ANYMODEL_BASE_URL", "https://anymodel.org/v1")
        raw = call_openai_compatible_raw(prompt, base_url, os.environ["ANYMODEL_KEY"], target_model)
        if raw:
            try:
                batch_result = BatchEditorialClassification.model_validate(raw)
            except Exception:
                pass

    # 2. OpenRouter
    if not batch_result and (provider in ("openrouter", "auto") and os.environ.get("OPENROUTER_API_KEY")):
        target_model = model or "google/gemini-2.5-flash-lite"
        raw = call_openai_compatible_raw(prompt, "https://openrouter.ai/api/v1", os.environ["OPENROUTER_API_KEY"], target_model)
        if raw:
            try:
                batch_result = BatchEditorialClassification.model_validate(raw)
            except Exception:
                pass

    if batch_result and batch_result.results:
        scored_list = []
        found_ids = set()
        for idx_item, item in enumerate(batch_result.results):
            art = id_map.get(item.id)
            if not art and idx_item < len(chunk):
                art = chunk[idx_item]
            if art and art.get("id") not in found_ids:
                found_ids.add(art.get("id"))
                ed = EditorialClassification(
                    scores=item.scores,
                    penalties=item.penalties,
                    categories=item.categories,
                    audience=item.audience,
                    locality=item.locality,
                    formats=item.formats,
                    why=item.why,
                    editorial_angles=item.editorial_angles,
                    confidence=item.confidence
                )
                news_score, content_score, tier, is_top, is_evergreen, is_breaking = calculate_article_scores(ed)
                scored_list.append(ScoredArticle(
                    id=art.get("id"),
                    title=art.get("title", ""),
                    summary=art.get("summary", ""),
                    source=art.get("source"),
                    url=art.get("url") or art.get("link"),
                    published_at=art.get("published_at") or art.get("published"),
                    country="Spain",
                    editorial=ed,
                    news_score=news_score,
                    content_score=content_score,
                    tier=tier,
                    is_top=is_top,
                    is_evergreen_gem=is_evergreen,
                    is_breaking=is_breaking,
                    scored_at=datetime.now(timezone.utc).isoformat()
                ))

        if len(scored_list) == len(chunk):
            return scored_list

        # Если часть статей потерялась в ответе, добираем поштучно
        missing = [art for art in chunk if art.get("id") not in found_ids]
        for m in missing:
            res = score_single_news(m, provider, model)
            if res:
                scored_list.append(res)
        return scored_list

    # Если пакетный вызов целиком не удался, производим fallback поштучно
    fallback = []
    for art in chunk:
        res = score_single_news(art, provider, model)
        if res:
            fallback.append(res)
    return fallback


# =================== ПАКЕТНЫЙ ЗАПУСК ===================

def run_scoring_batch(
    articles: List[Dict[str, Any]],
    provider: str = "anymodel",
    model: Optional[str] = None,
    workers: int = 4,
    batch_size: int = 5,
    conn: Optional[sqlite3.Connection] = None,
    out_file: Optional[str] = None
) -> List[ScoredArticle]:
    """
    Параллельно выполняет скоринг списка новостей пачками (батчами).
    Автоматически сохраняет прогресс в БД и файл каждые 50 статей,
    а также перехватывает Ctrl+C без потери данных.
    """
    scored_results: List[ScoredArticle] = []
    total = len(articles)
    
    # Разбиваем список на батчи по batch_size
    chunks = [articles[i:i + batch_size] for i in range(0, total, batch_size)]
    log.info("📊 Начинаем скоринг %d статей (батчей: %d, размер батча: %d, воркеров: %d, провайдер: %s)...",
             total, len(chunks), batch_size, workers, provider)

    completed = 0
    executor = ThreadPoolExecutor(max_workers=workers)
    try:
        futures = {
            executor.submit(score_articles_chunk, chk, provider, model): chk
            for chk in chunks
        }

        for future in as_completed(futures):
            chk = futures[future]
            try:
                results = future.result()
                for res in results:
                    completed += 1
                    scored_results.append(res)
                    if conn and res.id:
                        save_score_to_db(conn, res)
                    log.info("[%d/%d] %s Score: %.2f | %s",
                             completed, total, res.tier, res.news_score, res.title[:60])
                
                # Каждые 50 статей фиксируем сохранение в БД и JSON
                if completed > 0 and (completed % 50 < len(results) or completed == total):
                    if conn:
                        conn.commit()
                    if out_file:
                        try:
                            with open(out_file, "w", encoding="utf-8") as f:
                                json.dump([a.model_dump() for a in scored_results], f, ensure_ascii=False, indent=2)
                        except Exception:
                            pass
                    log.info("💾 [Сохранение] Прогресс: %d/%d статей зафиксированы в базе данных и %s",
                             completed, total, out_file or "scored.json")
            except Exception as e:
                log.error("❌ Ошибка обработки батча: %s", e)

    except KeyboardInterrupt:
        log.warning("\n⚠️ Скоринг приостановлен (Ctrl+C). Сохраняем уже оцененные статьи...")
        if conn:
            conn.commit()
        if out_file:
            try:
                with open(out_file, "w", encoding="utf-8") as f:
                    json.dump([a.model_dump() for a in scored_results], f, ensure_ascii=False, indent=2)
            except Exception:
                pass
        log.info("💾 Зафиксировано %d статей. Они сохранены в БД и при следующем запуске повторно оцениваться не будут.",
                 len(scored_results))
        executor.shutdown(wait=False, cancel_futures=True)
    else:
        executor.shutdown(wait=True)

    # Сортируем по итоговому news_score от максимального к минимальному
    scored_results.sort(key=lambda x: x.news_score, reverse=True)
    return scored_results


def print_top_summary(scored_articles: List[ScoredArticle], top_n: int = 15):
    """Красивый консольный вывод лучших новостей с обоснованием и углами подачи."""
    print("\n" + "=" * 80)
    print(f"🏆 ТОП-{min(top_n, len(scored_articles))} НОВОСТЕЙ ПО РЕДАКЦИОННОМУ СКОРИНГУ")
    print("=" * 80)

    for i, item in enumerate(scored_articles[:top_n], 1):
        ed = item.editorial
        print(f"\n#{i} {item.tier} [{item.news_score:.1f}/10] {item.title}")
        print(f"   📅 {item.published_at or 'N/A'} | 🌐 {item.source or 'N/A'} | ID: {item.id or 'N/A'}")
        print(f"   🏷 Категории: {', '.join(ed.categories)} | Аудитория: {', '.join(ed.audience)} | Локальность: {ed.locality}")
        print(f"   📊 Оценки: Жизнь={ed.scores.resident_relevance} | Польза={ed.scores.practical_value} | Иммиграция={ed.scores.immigration_relevance} | Масштаб={ed.scores.impact} | Контент={ed.scores.content_potential} | Вечно={ed.scores.evergreen}")
        
        # Штрафы, если были
        active_penalties = {k: v for k, v in ed.penalties.model_dump().items() if v > 0}
        if active_penalties:
            pen_str = ", ".join(f"{k}: -{v}" for k, v in active_penalties.items())
            print(f"   ⚠️ Штрафы: {pen_str}")

        print("   💡 Почему:")
        for w in ed.why:
            print(f"      • {w}")

        print("   🎯 Редакционные углы подачи:")
        for angle in ed.editorial_angles:
            print(f"      → {angle}")

    print("\n" + "=" * 80)


# =================== CLI ТОЧКА ВХОДА ===================

def main():
    parser = argparse.ArgumentParser(description="Редакционный классификатор и LLM-скоринг новостей Испании")
    parser.add_argument("--ids", type=str, default=None, help="Список ID статей через запятую (например: 246290,247063)")
    parser.add_argument("--limit", type=int, default=None, help="Максимум статей для скоринга (по умолчанию все найденные)")
    parser.add_argument("--top", type=int, default=15, help="Сколько лучших показать в консоли")
    parser.add_argument("--since", type=str, default=None, help="Дата начала выборки (YYYY-MM-DD или 'today')")
    parser.add_argument("--today", action="store_true", help="Скорить новости только за сегодня")
    parser.add_argument("--input", type=str, default=None, help="Путь к Markdown-дайджесту или JSON (вместо БД)")
    parser.add_argument("--out", type=str, default=DEFAULT_OUTPUT_FILE, help="Файл для сохранения результата в JSON")
    parser.add_argument("--provider", type=str, default="anymodel", choices=["anymodel", "openrouter", "gemini", "mistral", "auto"], help="LLM провайдер (по умолчанию anymodel)")
    parser.add_argument("--model", type=str, default=None, help="Конкретное имя модели")
    parser.add_argument("--workers", type=int, default=4, help="Число параллельных потоков")
    parser.add_argument("--batch-size", type=int, default=5, help="Сколько статей оценивать в одном запросе к LLM (по умолчанию 5)")
    parser.add_argument("--force-rescore", action="store_true", help="Скорить повторно даже если статья уже есть в news_scores")
    parser.add_argument("--show-top", action="store_true", default=True, help="Вывести сводку ТОП-новостей в консоль")
    parser.add_argument("--telegram", "--tg", action="store_true", help="Отправить отобранные ТОП-новости в Telegram бот")
    parser.add_argument("--tg-chat-id", type=str, default=None, help="ID чата или канала (по умолчанию TELEGA_CHAT_ID из .env)")
    parser.add_argument("--tg-top", type=int, default=10, help="Сколько лучших новостей отправить в Telegram (по умолчанию 10)")
    parser.add_argument("--tg-min-score", type=float, default=6.8, help="Минимальный балл для отправки в Telegram (по умолчанию 6.8)")

    args = parser.parse_args()

    conn = None
    articles = []

    if args.input:
        if args.input.endswith(".md"):
            log.info("📄 Загрузка новостей из Markdown файла: %s", args.input)
            articles = load_articles_from_markdown(args.input, limit=args.limit)
        elif args.input.endswith(".json"):
            log.info("📋 Загрузка новостей из JSON файла: %s", args.input)
            with open(args.input, "r", encoding="utf-8") as f:
                articles = json.load(f)
            if args.limit:
                articles = articles[:args.limit]
        else:
            log.error("❌ Неподдерживаемый формат входного файла: %s", args.input)
            sys.exit(1)
    else:
        log.info("🗄 Подключение к базе данных: %s", DB_FILE)
        conn = sqlite3.connect(DB_FILE)
        init_scoring_db(conn)

        since_val = args.since
        if args.today:
            since_val = "today"

        article_ids = [int(x.strip()) for x in args.ids.split(",") if x.strip().isdigit()] if args.ids else None

        if not args.force_rescore:
            scored_count = conn.execute("SELECT COUNT(*) FROM news_scores").fetchone()[0]
            log.info("🗄 В базе уже зафиксировано %d ранее оцененных статей (они пропускаются).", scored_count)

        articles = load_articles_from_db(
            conn,
            limit=args.limit,
            since_date=since_val,
            skip_scored=not args.force_rescore,
            article_ids=article_ids
        )

    if not articles:
        log.warning("⚠️ Не найдено новых статей для скоринга (все уже оценены или нет подходящих).")
        return

    log.info("Найдено новых статей для обработки: %d", len(articles))

    scored_articles = run_scoring_batch(
        articles=articles,
        provider=args.provider,
        model=args.model,
        workers=args.workers,
        batch_size=args.batch_size,
        conn=conn,
        out_file=args.out
    )

    # Сохранение в JSON
    output_data = [art.model_dump() for art in scored_articles]
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    log.info("💾 Результаты скоринга сохранены в: %s (%d записей)", args.out, len(output_data))

    if args.show_top:
        print_top_summary(scored_articles, top_n=args.top)

    # Отправка отобранных ТОП новостей в Telegram
    if args.telegram or os.environ.get("AUTO_SEND_TELEGRAM") == "1":
        log.info("📲 Запуск отправки ТОП новостей в Telegram...")
        send_top_articles_to_telegram(
            scored_articles=scored_articles,
            token=os.environ.get("TELEGA_BOT"),
            chat_id=args.tg_chat_id,
            top_n=args.tg_top,
            min_score=args.tg_min_score
        )


if __name__ == "__main__":
    main()
