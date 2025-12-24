# app/core/redis.py
import redis.asyncio as redis
from app.core.config import env
from app.core.logger import log

class RedisManager:
    def __init__(self):
        self.pool = None
        self.client: redis.Redis = None

    async def connect(self):
        if self.client:
            return
        
        self.pool = redis.ConnectionPool.from_url(
            env.redis_url, 
            encoding="utf-8", 
            decode_responses=True,
            max_connections=20
        )
        self.client = redis.Redis(connection_pool=self.pool)
        
        try:
            await self.client.ping()
            log.info("✅ Redis connected successfully")
        except Exception as e:
            log.critical(f"❌ Failed to connect to Redis: {e}")
            raise e

    async def close(self):
        if self.client:
            await self.client.close()
            log.info("Redis connection closed")

# Синглтон
redis_manager = RedisManager()

# Хелпер для получения клиента (например, в FastAPI Depends)
async def get_redis() -> redis.Redis:
    if not redis_manager.client:
        await redis_manager.connect()
    return redis_manager.client