import json
import os
import re
import datetime as dt
from pathlib import Path
from typing import Any, Optional, Tuple, List

import asyncpg
from dotenv import load_dotenv

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
    "views": ("views_count", "delta_views_count"),
    "likes": ("likes_count", "delta_likes_count"),
    "comments": ("comments_count", "delta_comments_count"),
    "reports": ("reports_count", "delta_reports_count"),
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


def _read_prompt() -> str:
    p = Path(__file__).with_name("prompt.txt")
    return p.read_text(encoding="utf-8")


def _clean_llm_json(raw: str) -> str:
    raw = raw.strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\s*```$", "", raw)
    return raw.strip()


def _as_date(v: Any) -> Optional[dt.date]:
    if v is None:
        return None
    if isinstance(v, dt.date) and not isinstance(v, dt.datetime):
        return v
    if isinstance(v, str):
        s = v.strip()
        if _ISO_DATE_RE.match(s):
            try:
                return dt.date.fromisoformat(s)
            except ValueError:
                return None
    return None


def _coerce_param(v: Any) -> Any:
    if isinstance(v, str):
        s = v.strip()
        if _ISO_DATE_RE.match(s):
            try:
                return dt.date.fromisoformat(s)
            except ValueError:
                return v
        try:
            return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            return v
    return v


async def _fetch_one_int(sql: str, params: Tuple[Any, ...]) -> int:
    conn = await asyncpg.connect(**_db_cfg())
    try:
        params = tuple(_coerce_param(p) for p in params)
        val = await conn.fetchval(sql, *params)
        return 0 if val is None else int(val)
    finally:
        await conn.close()


def _plan_to_sql(plan: dict) -> Optional[Tuple[str, Tuple[Any, ...]]]:
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
    date = plan.get("date")
    date_from = plan.get("date_from")
    date_to = plan.get("date_to")
    gt = plan.get("gt")

    if isinstance(date, str) and _ISO_DATE_RE.match(date):
        date_from = date
        date_to = date

    d_from = _as_date(date_from)
    d_to = _as_date(date_to)

    where: List[str] = []
    params: List[Any] = []

    if creator_id:
        where.append(f"creator_id = ${len(params) + 1}")
        params.append(str(creator_id))

    if source == "videos":
        if metric in _METRIC_TO_VIDEOS_COL and gt is not None:
            col = _METRIC_TO_VIDEOS_COL[metric]
            where.append(f"{col} > ${len(params) + 1}")
            params.append(int(gt))

        if d_from:
            where.append(f"video_created_at >= ${len(params) + 1}")
            params.append(d_from)

        if d_to:
            end_exclusive = d_to + dt.timedelta(days=1)
            where.append(f"video_created_at < ${len(params) + 1}")
            params.append(end_exclusive)

        base_where = (" WHERE " + " AND ".join(where)) if where else ""

        if action == "count":
            return f"SELECT count(*) FROM videos{base_where};", tuple(params)

        if action == "count_distinct":
            return f"SELECT count(DISTINCT id) FROM videos{base_where};", tuple(params)

        if action == "sum_final":
            if metric == "videos":
                return f"SELECT count(*) FROM videos{base_where};", tuple(params)
            col = _METRIC_TO_VIDEOS_COL.get(metric)
            if not col:
                return None
            return f"SELECT COALESCE(sum({col}), 0) FROM videos{base_where};", tuple(params)

        if action == "sum_delta":
            return None

        return None
    
    if source == "snapshots":
        if d_from:
            where.append(f"created_at::date >= ${len(params) + 1}")
            params.append(d_from)
        if d_to:
            where.append(f"created_at::date <= ${len(params) + 1}")
            params.append(d_to)

        if metric in _METRIC_TO_SNAPSHOTS_COL and gt is not None:
            final_col, delta_col = _METRIC_TO_SNAPSHOTS_COL[metric]
            if action == "sum_delta":
                where.append(f"{delta_col} > ${len(params) + 1}")
            else:
                where.append(f"{final_col} > ${len(params) + 1}")
            params.append(int(gt))

        base_where = (" WHERE " + " AND ".join(where)) if where else ""

        if action == "count":
            return f"SELECT count(*) FROM video_snapshots{base_where};", tuple(params)

        if action == "count_distinct":
            return f"SELECT count(DISTINCT video_id) FROM video_snapshots{base_where};", tuple(params)

        if metric == "videos":
            if action in {"sum_final", "sum_delta"}:
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


async def get_number_from_text(user_text: str) -> int:
    prompt = _read_prompt()
    raw = await ask_llm(prompt, user_text or "")
    raw = _clean_llm_json(raw)

    try:
        plan = json.loads(raw)
    except Exception:
        return 0

    res = _plan_to_sql(plan)
    if not res:
        return 0

    sql, params = res
    return await _fetch_one_int(sql, params)