import asyncio

import asyncpg
import redis.asyncio as aioredis
import src.config as config

class RedisManager:
    def __init__(self):
        self.pool = None

    def get_client(self) -> aioredis.Redis:
        if not self.pool:
            # Create a reusable pool
            self.pool = aioredis.ConnectionPool.from_url(
                config.REDIS_URL,
                max_connections=config.REDIS_MAX_CONNECTIONS,
                decode_responses=True # Automatically decodes bytes to utf-8 strings
            )
        return aioredis.Redis(connection_pool=self.pool)

    async def close(self):
        if self.pool:
            await self.pool.disconnect()
            self.pool = None

class PostgresManager:
    def __init__(self):
        self.pool = None
        self._pool_lock = asyncio.Lock()

    async def get_pool(self) -> asyncpg.Pool:
        if not self.pool:
            async with self._pool_lock:
                # Re-check: another coroutine may have created it while we
                # were waiting on the lock.
                if not self.pool:
                    self.pool = await asyncpg.create_pool(
                        dsn=config.POSTGRES_URL,
                        min_size=1,
                        max_size=config.POSTGRES_POOL_MAX_SIZE,
                    )
        return self.pool

    async def upsert_page(self, url, domain, depth, status_code, content_hash, title, text_content, char_count):
        pool = await self.get_pool()
        await pool.execute(
            """
            INSERT INTO pages (url, domain, depth, status_code, content_hash, title, text_content, char_count)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (url) DO UPDATE SET
                depth = EXCLUDED.depth,
                status_code = EXCLUDED.status_code,
                content_hash = EXCLUDED.content_hash,
                title = EXCLUDED.title,
                text_content = EXCLUDED.text_content,
                char_count = EXCLUDED.char_count,
                last_crawled_at = now()
            """,
            url, domain, depth, status_code, content_hash, title, text_content, char_count,
        )

    async def close(self):
        if self.pool:
            await self.pool.close()
            self.pool = None

redis_manager = RedisManager()
postgres_manager = PostgresManager()