#PostgreSQL 연결 
import os, asyncpg
from typing import Any, Iterable

_DB_URL = os.getenv("DB_URL")
_POOL_MIN = int(os.getenv("DB_POOL_MIN", "1"))
_POOL_MAX = int(os.getenv("DB_POOL_MAX", "10"))
_STMT_TIMEOUT = int(os.getenv("PG_STMT_TIMEOUT_MS", "5000"))

_pool: asyncpg.Pool | None = None

async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        if not _DB_URL:
            raise RuntimeError("DB_URL 미설정")
        _pool = await asyncpg.create_pool(
            dsn=_DB_URL,
            min_size=_POOL_MIN,
            max_size=_POOL_MAX,
            statement_cache_size=0,
        )
    return _pool

async def fetch(sql: str, *args: Iterable[Any]):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(sql, *args, timeout=_STMT_TIMEOUT/1000.0)
