#PostgreSQL 연결 
import os, asyncpg

DB_URL = os.getenv("DB_URL")  # postgresql://user:pass@host:5432/gifts
POOL_MIN = int(os.getenv("DB_POOL_MIN", "1"))
POOL_MAX = int(os.getenv("DB_POOL_MAX", "10"))
STMT_TIMEOUT_MS = os.getenv("PG_STMT_TIMEOUT_MS")  # "5000" 등

_pool: asyncpg.Pool | None = None

async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        if not DB_URL:
            raise RuntimeError("Missing DB_URL env")
        server_settings = {}
        if STMT_TIMEOUT_MS:
            server_settings["statement_timeout"] = STMT_TIMEOUT_MS
        _pool = await asyncpg.create_pool(
            dsn=DB_URL, min_size=POOL_MIN, max_size=POOL_MAX,
            server_settings=server_settings or None,
        )
    return _pool

async def fetch(q: str, *args):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(q, *args)