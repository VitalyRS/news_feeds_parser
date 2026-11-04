"""
News Diversity & Expansion
==========================

Независимый модуль: из ранжированного списка новостей собирает выдачу TOP-N
(10 / 20 / 40 / ...), в которой одно событие не занимает большую часть списка,
а если после отсева дублей статей не хватает — автоматически добирает кандидатов
из следующих позиций общего рейтинга.

Конвейер:
    ranked source -> initial pool (N) -> embeddings (CPU) -> EVENT clustering
    -> TOPIC clustering -> selection (event cap + topic cap) -> not enough?
    -> expand pool (+40, +40, +100 ... до max_pool) -> repeat -> final TOP-N

Два уровня схожести:
    * EVENT similarity — «это одно и то же событие» (строгий порог, учитывает
      совпадение имён/чисел/названий и временное окно). Крушение поезда и
      причины крушения — одно событие; авария другого поезда через неделю — другое.
    * TOPIC similarity — «одна широкая тематика» (мягкий порог). Используется
      только для мягкого лимита тематики, чтобы одна тема (железная дорога) не
      забила выдачу, но и не вырезалась целиком.

Исходные рейтинги не изменяются: порядок результата = порядок исходного рейтинга,
объекты возвращаются как есть. Каждое решение (включена/исключена и почему)
записывается в `DiversificationResult.decisions`; `result.explain()` печатает лог.

Использование:
    from news_diversification import diversify

    res = diversify(ranked_articles, target=40)              # список
    res = diversify(lambda off, lim: db_page(off, lim), 20)  # ленивая выдача из БД
    print(res.explain())
    top = res.selected

CLI (проверка качества на реальных данных, напр. scored.json):
    python3 news_diversification.py scored.json --target 40 --backend auto --explain
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import re
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import numpy as np

log = logging.getLogger("news_diversification")

# =================== КОНФИГУРАЦИЯ ===================


@dataclass
class DiversityConfig:
    target: int = 40                    # целевой размер выдачи
    max_per_event: int = 3              # максимум статей об одном событии
    min_events: int = 15                # минимум разных событий
    initial_pool: Optional[int] = None  # первоначальный пул (по умолчанию = target)
    expansion_steps: Tuple[int, ...] = (40, 40, 100)  # +40, +40, +100, далее — последний шаг
    max_pool: int = 300                 # максимальный пул
    max_topic_share: float = 0.25       # мягкий лимит доли одной широкой темы
    # Пороги (косинусная схожесть, зависят от модели — калибруются через CLI)
    event_threshold: Optional[float] = None
    topic_threshold: Optional[float] = None
    event_window_days: float = 4.0      # статьи дальше друг от друга = разные события
    entity_weight: float = 0.25         # вес лексического совпадения имён/чисел
    backend: str = "auto"               # auto | model2vec | wordllama | tfidf
    model_name: str = "minishlab/potion-multilingual-128M"
    allow_topic_relaxation: bool = True   # если не хватает — ослабить лимит темы
    max_event_relaxation: int = 1         # если не хватает — разрешить +1 к лимиту события

    @staticmethod
    def for_target(target: int, **overrides) -> "DiversityConfig":
        """Профиль под размер выдачи: для TOP-10/20 лимиты строже по абсолюту."""
        if target <= 10:
            per_event, min_ev = 1, max(1, math.ceil(target * 0.8))
        elif target <= 20:
            per_event, min_ev = 2, math.ceil(target * 0.5)
        else:
            per_event, min_ev = 3, math.ceil(target * 0.375)  # 40 -> 15
        cfg = DiversityConfig(target=target, max_per_event=per_event, min_events=min_ev)
        for k, v in overrides.items():
            if v is not None:
                setattr(cfg, k, v)
        return cfg


# Пороги по умолчанию для разных backend'ов (event строже topic)
_DEFAULT_THRESHOLDS = {
    "model2vec": (0.62, 0.40),
    "wordllama": (0.62, 0.40),
    "tfidf": (0.30, 0.15),
}

# =================== ЭМБЕДДИНГИ (CPU) ===================


class Embedder:
    name = "base"

    def embed(self, texts: List[str]) -> np.ndarray:  # L2-нормированные строки
        raise NotImplementedError


def _normalize(m: np.ndarray) -> np.ndarray:
    m = np.asarray(m, dtype=np.float32)
    n = np.linalg.norm(m, axis=1, keepdims=True)
    n[n == 0] = 1.0
    return m / n


class _CachedEmbedder(Embedder):
    def __init__(self):
        self._cache: Dict[str, np.ndarray] = {}

    def _encode(self, texts: List[str]) -> np.ndarray:
        raise NotImplementedError

    def embed(self, texts: List[str]) -> np.ndarray:
        missing = [t for t in dict.fromkeys(texts) if t not in self._cache]
        if missing:
            vecs = _normalize(self._encode(missing))
            for t, v in zip(missing, vecs):
                self._cache[t] = v
        return np.stack([self._cache[t] for t in texts])


class Model2VecEmbedder(_CachedEmbedder):
    name = "model2vec"

    def __init__(self, model_name: str):
        super().__init__()
        from model2vec import StaticModel
        self.model = StaticModel.from_pretrained(model_name)

    def _encode(self, texts):
        return self.model.encode(texts)


class WordLlamaEmbedder(_CachedEmbedder):
    name = "wordllama"

    def __init__(self, model_name: str = ""):
        super().__init__()
        from wordllama import WordLlama
        self.model = WordLlama.load()

    def _encode(self, texts):
        return self.model.embed(texts)


class TfidfEmbedder(Embedder):
    """Резервный backend без загрузки моделей: char n-gram TF-IDF (язык-независим)."""
    name = "tfidf"

    def embed(self, texts):
        from sklearn.feature_extraction.text import TfidfVectorizer
        vec = TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 5), sublinear_tf=True, lowercase=True)
        return _normalize(vec.fit_transform(texts).toarray())


def make_embedder(backend: str = "auto", model_name: str = "") -> Embedder:
    order = ["model2vec", "wordllama", "tfidf"] if backend == "auto" else [backend]
    last_err: Optional[Exception] = None
    for b in order:
        try:
            if b == "model2vec":
                return Model2VecEmbedder(model_name)
            if b == "wordllama":
                return WordLlamaEmbedder(model_name)
            if b == "tfidf":
                return TfidfEmbedder()
            raise ValueError(f"unknown backend: {b}")
        except Exception as e:  # нет пакета / нет сети для загрузки модели
            last_err = e
            log.warning("Embedder '%s' недоступен (%s) — пробую следующий", b, e)
    raise RuntimeError(f"Нет доступных embedding backend'ов: {last_err}")


# =================== ДОСТУП К ПОЛЯМ СТАТЬИ ===================

Item = Any  # dict или объект (pydantic ScoredArticle и т.п.)


def _get(item: Item, names: Sequence[str], default=None):
    for n in names:
        if isinstance(item, dict):
            if n in item and item[n] is not None:
                return item[n]
        else:
            v = getattr(item, n, None)
            if v is not None:
                return v
    return default


def _parse_dt(v) -> Optional[datetime]:
    if not v:
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    s = str(v).strip()
    try:
        d = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        try:
            d = parsedate_to_datetime(s)
        except Exception:
            return None
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


_ENTITY_RE = re.compile(r"\b(?:[A-ZÁÉÍÓÚÑÜА-ЯЁ][\wáéíóúñüа-яё]{3,}|\d[\d.,]*)\b")


def _entities(text: str) -> set:
    # Имена собственные/числа; первое слово предложения тоже попадает — допустимый шум.
    return {m.group(0).lower() for m in _ENTITY_RE.finditer(text)}


@dataclass
class _Cand:
    rank: int            # позиция в исходном рейтинге (0-based)
    item: Item
    id: Any
    title: str
    score: Optional[float]
    dt: Optional[datetime]
    event_text: str
    topic_text: str
    ents: set


def _make_cand(rank: int, item: Item) -> _Cand:
    title = str(_get(item, ["title"], "") or "")
    summary = str(_get(item, ["summary", "llm_summary", "description"], "") or "")
    score = _get(item, ["news_score", "score"])
    return _Cand(
        rank=rank, item=item,
        id=_get(item, ["id", "url", "link"], rank),
        title=title,
        score=float(score) if score is not None else None,
        dt=_parse_dt(_get(item, ["published_at", "published", "date"])),
        event_text=f"{title}. {summary[:200]}",
        topic_text=f"{title}. {summary[:600]}",
        ents=_entities(f"{title}. {summary[:200]}"),
    )


# =================== КЛАСТЕРИЗАЦИЯ ===================


def _cluster(dist: np.ndarray, threshold: float) -> List[int]:
    """Agglomerative clustering (average linkage) по матрице расстояний."""
    n = len(dist)
    if n == 1:
        return [0]
    from scipy.cluster.hierarchy import linkage, fcluster
    from scipy.spatial.distance import squareform
    d = np.clip((dist + dist.T) / 2, 0, None)
    np.fill_diagonal(d, 0)
    z = linkage(squareform(d, checks=False), method="average")
    return [int(x) - 1 for x in fcluster(z, t=threshold, criterion="distance")]


def _relabel_by_rank(labels: List[int]) -> List[int]:
    """Метки кластеров нумеруются в порядке лучшего (первого) участника."""
    m: Dict[int, int] = {}
    return [m.setdefault(l, len(m)) for l in labels]


# =================== РЕЗУЛЬТАТ ===================


@dataclass
class Decision:
    rank: int                 # позиция в исходном рейтинге (1-based)
    id: Any
    title: str
    score: Optional[float]
    event: str                # E1, E2 ...
    topic: str                # T1, T2 ...
    status: str               # included | excluded | below_cutoff
    code: str                 # см. ниже
    reason: str               # человекочитаемое объяснение


# codes: included, included_relaxed, event_cap, topic_cap, near_duplicate, below_cutoff


@dataclass
class IterationLog:
    pool_size: int
    selected: int
    events: int
    note: str


@dataclass
class DiversificationResult:
    selected: List[Item]
    decisions: List[Decision]
    iterations: List[IterationLog]
    config: DiversityConfig
    backend: str
    warnings: List[str] = field(default_factory=list)
    pool_size: int = 0
    n_events_selected: int = 0

    @property
    def excluded(self) -> List[Decision]:
        return [d for d in self.decisions if d.status == "excluded"]

    def explain(self, show_below_cutoff: bool = False) -> str:
        c = self.config
        out = [
            f"=== News Diversity: target={c.target}, max_per_event={c.max_per_event}, "
            f"min_events={c.min_events}, backend={self.backend} ===",
        ]
        for i, it in enumerate(self.iterations, 1):
            out.append(f"  iter {i}: pool={it.pool_size} selected={it.selected} events={it.events} — {it.note}")
        out.append(f"Итог: {len(self.selected)}/{c.target} статей, {self.n_events_selected} событий, "
                   f"пул {self.pool_size}")
        for w in self.warnings:
            out.append(f"  ! {w}")
        out.append("")
        for d in self.decisions:
            if d.status == "below_cutoff" and not show_below_cutoff:
                continue
            mark = {"included": "+", "excluded": "-", "below_cutoff": "·"}[d.status]
            sc = f"{d.score:.2f}" if d.score is not None else "n/a"
            out.append(f" {mark} #{d.rank:<4} {sc:>6} {d.event:<5}{d.topic:<5} {d.title[:70]!r} | {d.reason}")
        return "\n".join(out)

    def to_dict(self) -> dict:
        return {
            "config": asdict(self.config), "backend": self.backend, "warnings": self.warnings,
            "pool_size": self.pool_size, "events": self.n_events_selected,
            "iterations": [asdict(i) for i in self.iterations],
            "decisions": [asdict(d) for d in self.decisions],
        }


# =================== ЯДРО ===================

Source = Union[Sequence[Item], Callable[[int, int], Sequence[Item]]]


class NewsDiversifier:
    def __init__(self, config: Optional[DiversityConfig] = None, embedder: Optional[Embedder] = None):
        self.cfg = config or DiversityConfig()
        self.embedder = embedder or make_embedder(self.cfg.backend, self.cfg.model_name)
        ev, tp = _DEFAULT_THRESHOLDS.get(self.embedder.name, (0.62, 0.40))
        self.event_thr = self.cfg.event_threshold if self.cfg.event_threshold is not None else ev
        self.topic_thr = self.cfg.topic_threshold if self.cfg.topic_threshold is not None else tp

    # ---- источник ----
    @staticmethod
    def _fetcher(source: Source) -> Callable[[int, int], Sequence[Item]]:
        if callable(source):
            return source
        data = list(source)
        return lambda off, lim: data[off:off + lim]

    # ---- схожесть ----
    def _analyze(self, cands: List[_Cand]):
        cfg = self.cfg
        n = len(cands)
        e_emb = self.embedder.embed([c.event_text for c in cands])
        t_emb = self.embedder.embed([c.topic_text for c in cands])
        e_cos = np.clip(e_emb @ e_emb.T, 0, 1)
        t_cos = np.clip(t_emb @ t_emb.T, 0, 1)

        ent = np.zeros((n, n), dtype=np.float32)
        for i in range(n):
            for j in range(i + 1, n):
                a, b = cands[i].ents, cands[j].ents
                if a and b:
                    ent[i, j] = ent[j, i] = len(a & b) / len(a | b)
        w = cfg.entity_weight
        e_sim = (1 - w) * e_cos + w * ent  # event similarity

        # временное окно: слишком далёкие по времени статьи — разные события
        for i in range(n):
            for j in range(i + 1, n):
                di, dj = cands[i].dt, cands[j].dt
                if di and dj and abs((di - dj).total_seconds()) > cfg.event_window_days * 86400:
                    e_sim[i, j] = e_sim[j, i] = 0.0

        ev = _relabel_by_rank(_cluster(1 - e_sim, 1 - self.event_thr))
        tp = _relabel_by_rank(_cluster(1 - t_cos, 1 - self.topic_thr))
        return ev, tp, e_sim

    def cluster_events(self, items: Sequence[Item]) -> List[int]:
        """Метки событий (0..k-1, по порядку первого участника) для произвольного списка."""
        if not items:
            return []
        return self._analyze([_make_cand(i, it) for i, it in enumerate(items)])[0]

    # ---- выбор по рейтингу ----
    def _select(self, cands, ev, tp, e_sim, per_event: int, topic_cap: Optional[int],
                relaxed: str = ""):
        cfg = self.cfg
        ev_members: Dict[int, List[int]] = {}   # event -> ранги включённых
        tp_count: Dict[int, int] = {}
        decisions: List[Decision] = []
        chosen = 0
        for i, c in enumerate(cands):
            base = dict(rank=c.rank + 1, id=c.id, title=c.title, score=c.score,
                        event=f"E{ev[i] + 1}", topic=f"T{tp[i] + 1}")
            if chosen >= cfg.target:
                decisions.append(Decision(**base, status="below_cutoff", code="below_cutoff",
                                          reason=f"выдача уже заполнена ({cfg.target})"))
                continue
            inc = ev_members.get(ev[i], [])
            if len(inc) >= per_event:
                ranks = ", ".join(f"#{cands[k].rank + 1}" for k in inc)
                nearest = max(inc, key=lambda k: e_sim[i, k])
                dup = e_sim[i, nearest] >= min(0.95, self.event_thr + 0.2)
                decisions.append(Decision(
                    **base, status="excluded",
                    code="near_duplicate" if dup else "event_cap",
                    reason=(f"{'почти дубль' if dup else 'то же событие'} {base['event']}: лимит "
                            f"{per_event} уже занят лучшими статьями {ranks} "
                            f"(схожесть с #{cands[nearest].rank + 1} = {e_sim[i, nearest]:.2f})")))
                continue
            if topic_cap is not None and tp_count.get(tp[i], 0) >= topic_cap:
                decisions.append(Decision(
                    **base, status="excluded", code="topic_cap",
                    reason=f"широкая тема {base['topic']} уже занимает {topic_cap} мест "
                           f"(мягкий лимит {cfg.max_topic_share:.0%})"))
                continue
            ev_members.setdefault(ev[i], []).append(i)
            tp_count[tp[i]] = tp_count.get(tp[i], 0) + 1
            chosen += 1
            n_in_ev = len(ev_members[ev[i]])
            decisions.append(Decision(
                **base, status="included", code="included_relaxed" if relaxed else "included",
                reason=(f"{n_in_ev}-я лучшая статья события {base['event']}"
                        + (f"; {relaxed}" if relaxed else ""))))
        return decisions, chosen, len(ev_members)

    # ---- основной цикл ----
    def run(self, source: Source) -> DiversificationResult:
        cfg = self.cfg
        fetch = self._fetcher(source)
        topic_cap = max(cfg.max_per_event, math.ceil(cfg.target * cfg.max_topic_share))
        pool_goal = cfg.initial_pool or cfg.target
        cands: List[_Cand] = []
        exhausted = False
        iterations: List[IterationLog] = []
        step_i = 0
        analysis = None

        while True:
            need = min(pool_goal, cfg.max_pool) - len(cands)
            if need > 0 and not exhausted:
                page = list(fetch(len(cands), need))
                cands.extend(_make_cand(len(cands) + k, it) for k, it in enumerate(page))
                if len(page) < need:
                    exhausted = True
            if not cands:
                return DiversificationResult([], [], [], cfg, self.embedder.name,
                                             ["пустой источник"], 0, 0)

            ev, tp, e_sim = analysis = self._analyze(cands)
            decisions, chosen, n_ev = self._select(cands, ev, tp, e_sim, cfg.max_per_event, topic_cap)
            ok = chosen >= cfg.target and n_ev >= cfg.min_events
            can_expand = not exhausted and len(cands) < cfg.max_pool
            iterations.append(IterationLog(
                len(cands), chosen, n_ev,
                "достаточно" if ok else
                (f"мало статей ({chosen}/{cfg.target})" if chosen < cfg.target
                 else f"мало событий ({n_ev}/{cfg.min_events})")
                + (" -> расширяю пул" if can_expand else " -> пул исчерпан")))
            if ok or not can_expand:
                break
            steps = cfg.expansion_steps or (40,)
            pool_goal = len(cands) + steps[min(step_i, len(steps) - 1)]
            step_i += 1

        warnings: List[str] = []
        ev, tp, e_sim = analysis
        best = (decisions, chosen, n_ev)

        # --- финальная стадия, если пул исчерпан, а условия не выполнены ---
        if not ok:
            if chosen < cfg.target:
                for relax_topic, extra in self._relax_plan():
                    d, c, e = self._select(
                        cands, ev, tp, e_sim, cfg.max_per_event + extra,
                        None if relax_topic else topic_cap,
                        relaxed=" и ".join(filter(None, [
                            "лимит темы ослаблен" if relax_topic else "",
                            f"лимит события +{extra}" if extra else ""])))
                    iterations.append(IterationLog(len(cands), c, e,
                                                   f"ослабление: topic={relax_topic}, event+{extra}"))
                    best = (d, c, e)
                    if c >= cfg.target:
                        break
                if best[1] < cfg.target:
                    warnings.append(f"Не удалось набрать {cfg.target}: доступно {best[1]} "
                                    f"(пул {len(cands)}, источник {'исчерпан' if exhausted else 'ограничен max_pool'})")
            if best[1] >= cfg.target and best[2] < cfg.min_events:
                # Событий мало, а статей хватает: ужесточаем лимит на событие.
                for per_event in range(cfg.max_per_event - 1, 0, -1):
                    d, c, e = self._select(cands, ev, tp, e_sim, per_event, topic_cap)
                    iterations.append(IterationLog(len(cands), c, e, f"ужесточение: лимит события={per_event}"))
                    if c >= cfg.target and e >= best[2]:
                        best = (d, c, e)
                    if c >= cfg.target and e >= cfg.min_events:
                        break
                if best[2] < cfg.min_events:
                    warnings.append(f"Разных событий {best[2]} < min_events={cfg.min_events} "
                                    f"(пул ограничен)")

        decisions, chosen, n_ev = best
        by_rank = {c.rank + 1: c for c in cands}
        selected = [by_rank[d.rank].item for d in decisions if d.status == "included"]
        return DiversificationResult(selected, decisions, iterations, cfg, self.embedder.name,
                                     warnings, len(cands), n_ev)

    def _relax_plan(self):
        plan = []
        if self.cfg.allow_topic_relaxation:
            plan.append((True, 0))
        for extra in range(1, self.cfg.max_event_relaxation + 1):
            plan.append((self.cfg.allow_topic_relaxation, extra))
        return plan


# =================== ПУБЛИЧНЫЙ API ===================


def diversify(source: Source, target: int = 40, config: Optional[DiversityConfig] = None,
              embedder: Optional[Embedder] = None, **overrides) -> DiversificationResult:
    """
    source: список статей, УЖЕ отсортированный по рейтингу (лучшие первыми),
            либо callable(offset, limit) -> список (ленивая подгрузка из БД).
    target: размер выдачи (10, 20, 40, ...). Лимиты подбираются профилем for_target.
    overrides: любые поля DiversityConfig (max_per_event=2, backend='tfidf', ...).
    """
    cfg = config or DiversityConfig.for_target(target, **overrides)
    return NewsDiversifier(cfg, embedder).run(source)


# =================== CLI ===================


def _main():
    ap = argparse.ArgumentParser(description="News Diversity & Expansion — проверка на данных")
    ap.add_argument("input", help="JSON-список статей (напр. scored.json)")
    ap.add_argument("--target", type=int, default=40)
    ap.add_argument("--backend", default="auto")
    ap.add_argument("--event-threshold", type=float)
    ap.add_argument("--topic-threshold", type=float)
    ap.add_argument("--max-per-event", type=int)
    ap.add_argument("--explain", action="store_true", help="подробный лог решений")
    ap.add_argument("--json", help="сохранить лог решений в JSON")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    data = json.load(open(a.input, encoding="utf-8"))
    data.sort(key=lambda x: x.get("news_score") or 0, reverse=True)
    res = diversify(data, a.target, backend=a.backend, event_threshold=a.event_threshold,
                    topic_threshold=a.topic_threshold, max_per_event=a.max_per_event)
    print(res.explain() if a.explain else res.explain().split("\n\n")[0])
    if a.json:
        json.dump(res.to_dict(), open(a.json, "w", encoding="utf-8"), ensure_ascii=False, indent=2, default=str)


if __name__ == "__main__":
    _main()
