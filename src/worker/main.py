
import asyncio
import logging
import hashlib
import os
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser
import aiohttp
from redis.exceptions import ResponseError

import src.config as config
from src.connection import redis_manager, postgres_manager
from src.pipeline import parser

logger = logging.getLogger(__name__)

REDIS_URL = config.REDIS_URL

class DistributedCrawler:
    def __init__(self, seed_urls, allowed_domains, redis_url, max_workers=5, polite_delay=1.0, depth_limit=3):
        self.seed_urls = seed_urls
        self.allowed_domains = allowed_domains
        self.redis_url = redis_url
        self.max_workers = max_workers
        self.polite_delay = polite_delay
        self.depth_limit = depth_limit

        self.FRONTIER_KEY = "crawler:frontier"
        self.URL_FILTER_KEY = "crawler:bloom:urls"
        self.CONTENT_FILTER_KEY = "crawler:bloom:content"
        self.PAGES_CRAWLED_KEY = "crawler:pages_crawled"
        self.HEARTBEAT_TTL = 30
        # Bloom filter sizing: expected item count and target false-positive rate.
        # False positives are possible (an unseen URL gets treated as seen and
        # skipped) but false negatives are not, which is the standard Bloom
        # filter tradeoff traded for O(1) memory instead of storing raw strings.
        self.BLOOM_CAPACITY = 1_000_000
        self.BLOOM_ERROR_RATE = 0.001
        self.robot_parsers = {}

    async def init_redis(self):
        self.redis = redis_manager.get_client()
        logger.info("Connected to Redis.")
        await self._ensure_bloom_filter(self.URL_FILTER_KEY)
        await self._ensure_bloom_filter(self.CONTENT_FILTER_KEY)

    async def _ensure_bloom_filter(self, key):
        try:
            await self.redis.bf().create(key, self.BLOOM_ERROR_RATE, self.BLOOM_CAPACITY)
        except ResponseError as e:
            if "exists" not in str(e).lower():
                raise

    async def init_postgres(self):
        await postgres_manager.get_pool()
        logger.info("Connected to Postgres.")

    async def get_robots_parser(self, session, domain):
        if domain in self.robot_parsers:
            return self.robot_parsers[domain]

        rp = RobotFileParser()
        robots_url = f"https://{domain}/robots.txt"
        try:
            async with session.get(robots_url, timeout=5) as response:
                if response.status == 200:
                    content = await response.text()
                    rp.parse(content.splitlines())
                else:
                    # Allow all if robots.txt missing
                    rp.parse(["User-agent: *", "Allow: /"])
        except Exception as e:
            logger.error(f"Error occurred while fetching robots.txt for {domain}: {e}")
            rp.parse(["User-agent: *", "Allow: /"])

        self.robot_parsers[domain] = rp
        return rp

    async def is_allowed_by_robots(self, session, url):
        parsed = urlparse(url)
        rp = await self.get_robots_parser(session, parsed.netloc)
        return rp.can_fetch("Googlebot", url)

    async def acquire_domain_lock(self, domain):
        """Cluster-wide per-domain rate limit: only one worker across the
        whole fleet may hold a domain's lock at a time, and it auto-expires
        after polite_delay so the next request waits at least that long."""
        lock_key = f"lock:{domain}"
        ttl_ms = max(int(self.polite_delay * 1000), 1)
        acquired = await self.redis.set(lock_key, "active", nx=True, px=ttl_ms)
        return bool(acquired)

    async def run(self):
        await self.init_redis()
        await self.init_postgres()

        # Enqueue seed URLs with depth 0
        for url in self.seed_urls:
            is_seen = await self.redis.bf().exists(self.URL_FILTER_KEY, url)
            if not is_seen:
                # Format stored in Redis Frontier: "url|depth" -> priority (which is depth)
                await self.redis.zadd(self.FRONTIER_KEY, {f"{url}|0": 0})

        async with aiohttp.ClientSession() as session:
            workers = [
                asyncio.create_task(self.worker(session, i))
                for i in range(self.max_workers)
            ]

            while True:
                frontier_size = await self.redis.zcard(self.FRONTIER_KEY)
                if frontier_size == 0:
                    await asyncio.sleep(5)
                    if await self.redis.zcard(self.FRONTIER_KEY) == 0:
                        break
                await asyncio.sleep(2)

            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

        await redis_manager.close()
        await postgres_manager.close()
        logger.info("Crawler finished execution.")

    async def worker(self, session, worker_id):
        heartbeat_key = f"worker:heartbeat:{os.getpid()}:{worker_id}"
        while True:
            try:
                await self.redis.set(heartbeat_key, "1", ex=self.HEARTBEAT_TTL)
                result = await self.redis.zpopmin(self.FRONTIER_KEY)
                if not result:
                    await asyncio.sleep(1)
                    continue

                payload, priority = result[0]

                url, depth_str = payload.rsplit("|", 1)
                depth = int(depth_str)

                if depth > self.depth_limit:
                    continue

                is_seen = await self.redis.bf().exists(self.URL_FILTER_KEY, url)
                if is_seen:
                    continue

                domain = urlparse(url).netloc
                if not await self.acquire_domain_lock(domain):
                    # Another worker in the cluster is mid-politeness-delay
                    # for this domain. Put the URL back and pick up
                    # something else instead of busy-waiting on it.
                    await self.redis.zadd(self.FRONTIER_KEY, {payload: priority})
                    await asyncio.sleep(0.1)
                    continue

                if not await self.is_allowed_by_robots(session, url):
                    logger.warning(f"Blocked by robots.txt: {url}")
                    continue

                await self.crawl_page(session, url, depth)
                await self.redis.bf().add(self.URL_FILTER_KEY, url)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker {worker_id} ran into an error: {e}")
                # Back off before retrying so a persistent failure (e.g. a
                # connection pool issue) can't spin the loop at full speed.
                await asyncio.sleep(1)

    async def crawl_page(self, session, url, depth):
        logger.info(f"Crawling [Depth {depth}]: {url}")
        try:
            async with session.get(url, timeout=10) as response:
                status_code = response.status
                if status_code != 200 or "text/html" not in response.headers.get("Content-Type", ""):
                    return
                html_content = await response.text()
        except Exception:
            return

        content_hash = hashlib.md5(html_content.encode("utf-8")).hexdigest()
        is_new_content = await self.redis.bf().add(self.CONTENT_FILTER_KEY, content_hash)
        if not is_new_content:
            logger.info(f"Duplicate content ignored: {url}")
            return

        await self.redis.incr(self.PAGES_CRAWLED_KEY)

        soup = parser.parse_html(html_content)
        domain = urlparse(url).netloc
        text_content = parser.extract_text(soup)
        await postgres_manager.upsert_page(
            url=url,
            domain=domain,
            depth=depth,
            status_code=status_code,
            content_hash=content_hash,
            title=parser.extract_title(soup),
            text_content=text_content,
            char_count=len(text_content),
        )

        await self.extract_and_enqueue_links(soup, url, depth)

    async def extract_and_enqueue_links(self, soup, base_url, current_depth):
        for normalized_url in parser.extract_links(soup, base_url, self.allowed_domains):
            is_seen = await self.redis.bf().exists(self.URL_FILTER_KEY, normalized_url)
            if not is_seen:
                next_depth = current_depth + 1
                # Enqueue both the URL and its target depth
                await self.redis.zadd(self.FRONTIER_KEY, {f"{normalized_url}|{next_depth}": next_depth})


class DistributedCrawlerWorker:
    """Runs one DistributedCrawler instance inside its own process, sharing
    the Redis-backed frontier/dedup state with every other worker process
    so the cluster scales horizontally without overlap."""

    def __init__(self, redis_url):
        self.crawler = DistributedCrawler(
            seed_urls=config.SEED_URLS,
            allowed_domains=config.ALLOWED_DOMAINS,
            redis_url=redis_url,
            max_workers=config.MAX_WORKERS,
            polite_delay=config.POLITE_DELAY,
            depth_limit=config.DEPTH_LIMIT,
        )

    async def run(self):
        await self.crawler.run()
