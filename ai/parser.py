import json
import os
import re
from pathlib import Path
from dotenv import load_dotenv
import asyncpg

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

_METRIC_TO_SNAP_DELTA_COL = {
    "views": "delta_views_count",
    "likes": "delta_likes_count",
    "comments": "delta_comments_count",
    "reports": "delta_reports_count",
}


def _load_prompt() -> str:
    return Path(__file__).with_name("prompt.txt").read_text(encoding="utf-8")


def _extract_json(text: str | None) -> dict | None:
    if not text:
        return None

    s = text.strip()

    if s.startswith("```"):
        s = re.sub(r"^```[a-zA-Z]*", "", s).strip()
        if s.endswith("```"):
            s = s[:-3].strip()

    try:
        return json.loads(s)
    except Exception:
        pass

    start = s.find("{")
    end = s.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None

    chunk = s[start : end + 1]
    try:
        return json.loads(chunk)
    except Exception:
        return None


def _db_cfg():
    return dict(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME", "tps"),
        user=os.getenv("DB_USER", "tps"),
        password=os.getenv("DB_PASSWORD", "tps"),
    )


async def _fetch_one_int(sql: str, params: tuple):
    conn = await asyncpg.connect(**_db_cfg())
    try:
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
    date = plan.get("date")
    date_from = plan.get("date_from")
    date_to = plan.get("date_to")
    gt = plan.get("gt")
    only_pos = plan.get("only_positive_delta")

    where = []
    params = []

    def add(cond: str, *p):
        where.append(cond)
        params.extend(p)

    if source == "videos":
        table = "videos"

        if date:
            add("video_created_at::date = $%d" % (len(params) + 1), date)
        if date_from and date_to:
            add("video_created_at::date BETWEEN $%d AND $%d" % (len(params) + 1, len(params) + 2), date_from, date_to)

        if creator_id:
            add("creator_id = $%d" % (len(params) + 1), creator_id)

        if action in ("sum_final",) and metric == "videos":
            return None

        if action in ("count", "count_distinct"):
            if gt is not None:
                col = _METRIC_TO_VIDEOS_COL.get("views") if metric == "videos" else _METRIC_TO_VIDEOS_COL.get(metric)
                if not col:
                    return None
                add(f"{col} > $%d" % (len(params) + 1), int(gt))

            select = "COUNT(*)" if action == "count" else "COUNT(DISTINCT id)"
            sql = f"SELECT {select} FROM {table}"
        elif action == "sum_final":
            col = _METRIC_TO_VIDEOS_COL.get(metric)
            if not col:
                return None
            sql = f"SELECT COALESCE(SUM({col}), 0) FROM {table}"
        else:
            return None

        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += ";"
        return sql, tuple(params)

    table = "video_snapshots"

    join_videos = bool(creator_id)
    from_clause = table + (" s JOIN videos v ON v.id = s.video_id" if join_videos else "")

    if date:
        col = "s.created_at" if join_videos else "created_at"
        add(f"{col}::date = $%d" % (len(params) + 1), date)
    if date_from and date_to:
        col = "s.created_at" if join_videos else "created_at"
        add(f"{col}::date BETWEEN $%d AND $%d" % (len(params) + 1, len(params) + 2), date_from, date_to)

    if creator_id:
        add("v.creator_id = $%d" % (len(params) + 1), creator_id)

    if metric == "videos":
        return None

    delta_col = _METRIC_TO_SNAP_DELTA_COL[metric]
    col_ref = f"s.{delta_col}" if join_videos else delta_col

    if action == "sum_delta":
        sql = f"SELECT COALESCE(SUM({col_ref}), 0) FROM {from_clause}"
        if gt is not None:
            add(f"{col_ref} > $%d" % (len(params) + 1), int(gt))
    elif action == "count_distinct":
        vid_ref = "s.video_id" if join_videos else "video_id"
        sql = f"SELECT COUNT(DISTINCT {vid_ref}) FROM {from_clause}"
        if only_pos is True:
            add(f"{col_ref} > 0")
        elif gt is not None:
            add(f"{col_ref} > $%d" % (len(params) + 1), int(gt))
    elif action == "count":
        sql = f"SELECT COUNT(*) FROM {from_clause}"
        if only_pos is True:
            add(f"{col_ref} > 0")
        elif gt is not None:
            add(f"{col_ref} > $%d" % (len(params) + 1), int(gt))
    else:
        return None

    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += ";"
    return sql, tuple(params)


async def get_number_from_text(user_text: str) -> int:
    prompt = _load_prompt()
    raw = await ask_llm(prompt, user_text or "")

    plan = _extract_json(raw)
    if not plan:
        return 0

    res = _plan_to_sql(plan)
    if not res:
        return 0

    sql, params = res
    return await _fetch_one_int(sql, params)
