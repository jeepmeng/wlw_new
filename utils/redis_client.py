# redis_client.py
import redis.asyncio as redis
from config.settings import settings

_redis = None

def get_redis_client() -> redis.Redis:
    global _redis
    if _redis is None:
        redis_url = settings.session_cache.redis_url
        _redis = redis.Redis.from_url(redis_url, decode_responses=True)
    return _redis