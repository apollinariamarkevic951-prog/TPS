import json
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values


def parse_ts(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def main():
    conn = psycopg2.connect(
        dbname="tps",
        user="tps",
        password="tps",
        host="localhost",
        port=5432,
    )

    with open("data/videos.json", "r", encoding="utf-8") as f:
        payload = json.load(f)

    videos = payload["videos"]

    with conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE video_snapshots CASCADE;")
            cur.execute("TRUNCATE TABLE videos CASCADE;")

            video_rows = []
            snapshot_rows = []

            for v in videos:
                video_rows.append((
                    v["id"],
                    v["creator_id"],
                    parse_ts(v["video_created_at"]),
                    v["views_count"],
                    v["likes_count"],
                    v["comments_count"],
                    v["reports_count"],
                    parse_ts(v["created_at"]),
                    parse_ts(v["updated_at"]),
                ))

                for s in v["snapshots"]:
                    snapshot_rows.append((
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
                    ))

            execute_values(
                cur,
                "INSERT INTO videos (id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count, created_at, updated_at) VALUES %s",
                video_rows,
            )

            execute_values(
                cur,
                "INSERT INTO video_snapshots (id, video_id, views_count, likes_count, comments_count, reports_count, delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count, created_at, updated_at) VALUES %s",
                snapshot_rows,
            )

if __name__ == "__main__":
    main()