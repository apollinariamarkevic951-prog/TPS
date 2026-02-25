import os
import json
import re
from pathlib import Path
import psycopg2
from dotenv import load_dotenv
from ai.api import ask_llm
load_dotenv()

ALLOWED_TYPES = {
    "count_all_videos",
    "count_videos_metric_gt",
    "sum_metric_growth_on_date",
    "count_distinct_videos_with_new_metric_on_date",
    "count_videos_by_creator_and_period",
}

METRICS = {
    "views": ("views_count", "delta_views_count"),
    "likes": ("likes_count", "delta_likes_count"),
    "comments": ("comments_count", "delta_comments_count"),
    "reports": ("reports_count", "delta_reports_count"),
}


def _load_prompt():
    return Path(__file__).with_name("prompt.txt").read_text(encoding="utf-8")


def _extract_json(text):
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


def _fetch_one_number(sql, params=None):
    conn = psycopg2.connect(
        dbname="tps",
        user="tps",
        password="tps",
        host=os.getenv("DB_HOST", "localhost"),
        port=5432,
        )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or ())
                row = cur.fetchone()
                if not row or row[0] is None:
                    return 0
                return int(row[0])
    finally:
        conn.close()


def _plan_to_sql(plan):
    t = plan.get("type")

    if t == "count_all_videos":
        return "SELECT COUNT(*) FROM videos;", ()

    if t == "count_videos_metric_gt":
        metric = plan.get("metric")
        value_gt = plan.get("value_gt")
        if metric not in METRICS or value_gt is None:
            return None
        col, _ = METRICS[metric]
        return f"SELECT COUNT(*) FROM videos WHERE {col} > %s;", (int(value_gt),)

    if t == "sum_metric_growth_on_date":
        metric = plan.get("metric")
        d = plan.get("date")
        if metric not in METRICS or not d:
            return None
        _, delta_col = METRICS[metric]
        return (
            f"SELECT COALESCE(SUM({delta_col}), 0) "
            "FROM video_snapshots WHERE created_at::date = %s;",
            (d,),
        )

    if t == "count_distinct_videos_with_new_metric_on_date":
        metric = plan.get("metric")
        d = plan.get("date")
        if metric not in METRICS or not d:
            return None
        _, delta_col = METRICS[metric]
        return (
            "SELECT COUNT(DISTINCT video_id) "
            "FROM video_snapshots "
            f"WHERE created_at::date = %s AND {delta_col} > 0;",
            (d,),
        )

    if t == "count_videos_by_creator_and_period":
        creator_id = plan.get("creator_id")
        date_from = plan.get("date_from")
        date_to = plan.get("date_to")
        if not creator_id or not date_from or not date_to:
            return None
        return (
            "SELECT COUNT(*) FROM videos "
            "WHERE creator_id = %s AND video_created_at::date BETWEEN %s AND %s;",
            (creator_id, date_from, date_to),
        )

    return None

def get_number_from_text(user_text):
    prompt = _load_prompt()
    raw = ask_llm(prompt, user_text or "")

    plan = _extract_json(raw)
    if not plan:
        return 0

    if plan.get("type") not in ALLOWED_TYPES:
        return 0

    res = _plan_to_sql(plan)
    if not res:
        return 0

    sql, params = res
    return _fetch_one_number(sql, params)