import json
import os
import re
from pathlib import Path

from dotenv import load_dotenv
import asyncpg
import datetime as dt
from typing import Any

from ai.api import ask_llm

load_dotenv()

ALLOWED_SOURCE = {"videos", "snapshots"}
ALLOWED_ACTION = {"count", "count_distinct", "sum_final", "sum_delta"}
ALLOWED_METRIC = {"videos", "views", "likes", "comments", "reports"}

_METRIC_TO_VIDEOS_COL = {
    "views": "views_count",
    "likes": "likes_count",
    "comments": "comments_count",
    "reports": "reports_count",
}

_METRIC_TO_SNAPSHOTS_COL = {
    "views": ("final_views_count", "delta_views_count"),
    "likes": ("final_likes_count", "delta_likes_count"),
    "comments": ("final_comments_count", "delta_comments_count"),
    "reports": ("final_reports_count", "delta_reports_count"),
}

_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _db_cfg() -> dict:
    return dict(
        host=os.getenv("DB_HOST", "db"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME", "tps"),
        user=os.getenv("DB_USER", "tps"),
        password=os.getenv("DB_PASSWORD", "tps"),
    )


def _coerce_param(v: Any) -> Any:
    """Convert string ISO dates/datetimes to python date/datetime for asyncpg."""
    if isinstance(v, str):
        s = v.strip()
        # YYYY-MM-DD -> date
        if _ISO_DATE_RE.match(s):
            try:
                return dt.date.fromisoformat(s)
            except ValueError:
                pass
        # ISO datetime (optionally with Z)
        try:
            return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            return v
    return v


async def _fetch_one_int(sql: str, params: tuple):
    conn = await asyncpg.connect(**_db_cfg())
    try:
        # FIX: asyncpg expects python date/datetime for date/timestamp params (not strings)
        params = tuple(_coerce_param(p) for p in params)
        val = await conn.fetchval(sql, *params)
        if val is None:
            return 0
        return int(val)
    finally:
        await conn.close()


def _plan_to_sql(plan: dict) -> tuple[str, tuple] | None:
    source = plan.get("source")
    action = plan.get("action")
    metric = plan.get("metric")

    if source not in ALLOWED_SOURCE:
        return None
    if action not in ALLOWED_ACTION:
        return None
    if metric not in ALLOWED_METRIC:
        return None

    creator_id = plan.get("creator_id")
    date_from = plan.get("date_from")
    date_to = plan.get("date_to")
    date = plan.get("date")
    gt = plan.get("gt")

    params: list[Any] = []
    where: list[str] = []

    # normalize date filters
    if isinstance(date, str) and _ISO_DATE_RE.match(date):
        # single day
        date_from = date
        date_to = date

    if creator_id:
        where.append("creator_id = $" + str(len(params) + 1))
        params.append(str(creator_id))

    if source == "videos":
        # filters for videos table
        if metric in _METRIC_TO_VIDEOS_COL and gt is not None:
            col = _METRIC_TO_VIDEOS_COL[metric]
            where.append(f"{col} > $" + str(len(params) + 1))
            params.append(int(gt))

        if date_from:
            where.append("video_created_at >= $" + str(len(params) + 1))
            params.append(date_from)
        if date_to:
            # inclusive end day
            where.append("video_created_at < ($" + str(len(params) + 1) + " + interval '1 day')")
            params.append(date_to)

        base_where = (" WHERE " + " AND ".join(where)) if where else ""

        if action == "count":
            return f"SELECT count(*) FROM videos{base_where};", tuple(params)

        if action == "count_distinct":
            # distinct by video id in videos table is just count(distinct id)
            return f"SELECT count(DISTINCT id) FROM videos{base_where};", tuple(params)

        if action in {"sum_final", "sum_delta"}:
            # no delta columns in videos; only sum final metric
            if metric == "videos":
                return f"SELECT count(*) FROM videos{base_where};", tuple(params)
            col = _METRIC_TO_VIDEOS_COL.get(metric)
            if not col:
                return None
            return f"SELECT COALESCE(sum({col}), 0) FROM videos{base_where};", tuple(params)

        return None

    # snapshots source
    if source == "snapshots":
        # snapshot_date filters
        if date_from:
            where.append("snapshot_date >= $" + str(len(params) + 1))
            params.append(date_from)
        if date_to:
            where.append("snapshot_date <= $" + str(len(params) + 1))
            params.append(date_to)

        # optional "gt" for delta metric only (e.g., delta_views_count > N)
        if metric in _METRIC_TO_SNAPSHOTS_COL and gt is not None:
            _, delta_col = _METRIC_TO_SNAPSHOTS_COL[metric]
            where.append(f"{delta_col} > $" + str(len(params) + 1))
            params.append(int(gt))

        base_where = (" WHERE " + " AND ".join(where)) if where else ""

        if action == "count":
            return f"SELECT count(*) FROM video_snapshots{base_where};", tuple(params)

        if action == "count_distinct":
            return f"SELECT count(DISTINCT video_id) FROM video_snapshots{base_where};", tuple(params)

        if metric == "videos":
            # treat as count of distinct videos in snapshots
            if action == "sum_final":
                return f"SELECT count(DISTINCT video_id) FROM video_snapshots{base_where};", tuple(params)
            if action == "sum_delta":
                return f"SELECT count(DISTINCT video_id) FROM video_snapshots{base_where};", tuple(params)
            return None

        if metric in _METRIC_TO_SNAPSHOTS_COL:
            final_col, delta_col = _METRIC_TO_SNAPSHOTS_COL[metric]
            if action == "sum_final":
                return f"SELECT COALESCE(sum({final_col}), 0) FROM video_snapshots{base_where};", tuple(params)
            if action == "sum_delta":
                return f"SELECT COALESCE(sum({delta_col}), 0) FROM video_snapshots{base_where};", tuple(params)

        return None

    return None


def _clean_llm_json(raw: str) -> str:
    # remove ```json fences if present
    raw = raw.strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\s*```$", "", raw)
    return raw.strip()


def _prompt() -> str:
    # (не даю слишком длинный промпт, но оставляю структуру)
    return (
        "Ты помощник, который извлекает из русскоязычного запроса JSON-план для SQL.\n"
        "Верни ТОЛЬКО JSON без текста.\n\n"
        "Схема плана:\n"
        "{\n"
        '  "source": "videos" | "snapshots",\n'
        '  "action": "count" | "count_distinct" | "sum_final" | "sum_delta",\n'
        '  "metric": "videos" | "views" | "likes" | "comments" | "reports",\n'
        '  "creator_id": string|null,\n'
        '  "date": "YYYY-MM-DD"|null,\n'
        '  "date_from": "YYYY-MM-DD"|null,\n'
        '  "date_to": "YYYY-MM-DD"|null,\n'
        '  "gt": number|null\n'
        "}\n\n"
        "Правила:\n"
        "- source=videos: таблица videos, даты относятся к video_created_at.\n"
        "- source=snapshots: таблица video_snapshots, даты относятся к snapshot_date.\n"
        "- sum_final: суммируй final_* (для snapshots) или *_count (для videos).\n"
        "- sum_delta: суммируй delta_* (только для snapshots).\n"
        "- count_distinct для snapshots: DISTINCT video_id.\n"
    )


async def get_number_from_text(user_text: str) -> int:
    prompt = _prompt()
    raw = await ask_llm(prompt, user_text or "")
    raw = _clean_llm_json(raw)

    try:
        plan = json.loads(raw)
    except Exception:
        # если LLM вернул мусор — безопасный ответ
        return 0

    res = _plan_to_sql(plan)
    if not res:
        return 0
    sql, params = res
    return await _fetch_one_int(sql, params)