import os
import psycopg


def get_conninfo():
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    db = os.getenv("DB_NAME", "tps")
    user = os.getenv("DB_USER", "tps")
    password = os.getenv("DB_PASSWORD", "tps")
    return f"host={host} port={port} dbname={db} user={user} password={password}"


def fetch_one_int(sql, params=None):
    conninfo = get_conninfo()
    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            row = cur.fetchone()

            if row is None or row[0] is None:
                return 0

            return int(row[0])