import asyncio
import logging
from src.worker.main import DistributedCrawler
import src.config as config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)

async def main():
    crawler = DistributedCrawler(
        seed_urls=config.SEED_URLS,
        allowed_domains=config.ALLOWED_DOMAINS,
        redis_url=config.REDIS_URL,
        max_workers=config.MAX_WORKERS,
        polite_delay=config.POLITE_DELAY,
        depth_limit=config.DEPTH_LIMIT
    )
    await crawler.run()

if __name__ == "__main__":
    asyncio.run(main())