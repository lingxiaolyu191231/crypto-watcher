from __future__ import annotations
import os
import redis


_redis = None


def get_redis() -> redis.Redis:
    global _redis
    if _redis is None:
        url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        _redis = redis.from_url(url)
    return _redis
