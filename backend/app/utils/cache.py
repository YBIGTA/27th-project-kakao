import time, json, hashlib
from typing import Optional
from core.config import config

class BaseCache:
    def get(self, key: str) -> Optional[str]: ...
    def set(self, key: str, value: str, ttl: int): ...

class NoCache(BaseCache):
    def get(self, key: str): return None
    def set(self, key: str, value: str, ttl: int): pass

class MemoryCache(BaseCache):
    def __init__(self):
        self.store = {}

    def get(self, key: str):
        rec = self.store.get(key)
        if not rec: return None
        value, exp = rec
        if exp and exp < time.time():
            del self.store[key]
            return None
        return value

    def set(self, key: str, value: str, ttl: int):
        exp = int(time.time()) + ttl if ttl>0 else None
        self.store[key] = (value, exp)

class RedisCache(BaseCache):
    def __init__(self, url: str):
        import redis
        self.r = redis.from_url(url, decode_responses=True)

    def get(self, key: str):
        return self.r.get(key)

    def set(self, key: str, value: str, ttl: int):
        if ttl>0: self.r.setex(key, ttl, value)
        else: self.r.set(key, value)

def new_cache() -> BaseCache:
    backend = config.CACHE_BACKEND.lower()
    if backend == "redis":
        return RedisCache(config.REDIS_URL)
    elif backend == "memory":
        return MemoryCache()
    return NoCache()

def make_cache_key(prefix: str, payload: dict) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    h = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"{prefix}:{h}"
