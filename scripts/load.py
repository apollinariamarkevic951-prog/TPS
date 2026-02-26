import os
import json
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def env_bool(name: str, default: str = "0") -> bool:
    v = os.getenv(name, default).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def main() -> None:
    db_host = os.getenv("DB_HOST", "localhost")
    db_name = os.getenv("DB_NAME", "tps")
    db_user = os.getenv("DB_USER", "tps")
    db_password = os.getenv("DB_PASSWORD", "tps")
    db_port = int(os.getenv("DB_PORT", "5432"))

    json_path = os.getenv("JSON_PATH", "data/videos.json")
    skip_truncate = env_bool("SKIP_TRUNCATE", "0")

    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
    )

    with open(json_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    videos = payload.get("videos") or []

    with conn:
        with conn.cursor() as cur:
            if not skip_truncate:
                cur.execute("TRUNCATE TABLE video_snapshots CASCADE;")
                cur.execute("TRUNCATE TABLE videos CASCADE;")

            video_rows = []
            snapshot_rows = []

            for v in videos:
                video_rows.append(
                    (
                        v["id"],
                        v["creator_id"],
                        parse_ts(v["video_created_at"]),
                        v["views_count"],
                        v["likes_count"],
                        v["comments_count"],
                        v["reports_count"],
                        parse_ts(v["created_at"]),
                        parse_ts(v["updated_at"]),
                    )
                )

                for s in (v.get("snapshots") or []):
                    snapshot_rows.append(
                        (
                            s["id"],
                            s["video_id"],
                            s["views_count"],
                            s["likes_count"],
                            s["comments_count"],
                            s["reports_count"],
                            s["delta_views_count"],
                            s["delta_likes_count"],
                            s["delta_comments_count"],
                            s["delta_reports_count"],
                            parse_ts(s["created_at"]),
                            parse_ts(s["updated_at"]),
                        )
                    )

            if video_rows:
                execute_values(
                    cur,
                    """INSERT INTO videos
                        (id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count, created_at, updated_at)
                        VALUES %s
                        ON CONFLICT (id) DO NOTHING
                    """,
                    video_rows,
                )

            if snapshot_rows:
                execute_values(
                    cur,
                    """INSERT INTO video_snapshots
                        (id, video_id, views_count, likes_count, comments_count, reports_count,
                         delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count,
                         created_at, updated_at)
                        VALUES %s
                        ON CONFLICT (id) DO NOTHING
                    """,
                    snapshot_rows,
                )

    conn.close()
    print(f"Loaded: videos={len(video_rows)}, snapshots={len(snapshot_rows)} from {json_path}")


if __name__ == "__main__":
    main()
