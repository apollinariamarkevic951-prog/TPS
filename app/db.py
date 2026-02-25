import psycopg2


def get_conn():
    return psycopg2.connect(
        dbname="tps",
        user="tps",
        password="tps",
        host="localhost",
        port=5432,
    )


def fetch_one_number(sql, params=None):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or ())
                row = cur.fetchone()
                if row is None or row[0] is None:
                    return 0
                return int(row[0])
    finally:
        conn.close()