import os
import asyncpg


def _db_cfg():
    return dict(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME", "tps"),
        user=os.getenv("DB_USER", "tps"),
        password=os.getenv("DB_PASSWORD", "tps"),
    )


async def fetch_one_int(sql: str, *params) -> int:
    conn = await asyncpg.connect(**_db_cfg())
    try:
        val = await conn.fetchval(sql, *params)
        if val is None:
            return 0
        return int(val)
    finally:
        await conn.close()
