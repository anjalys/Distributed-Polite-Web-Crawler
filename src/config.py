import os

from dotenv import load_dotenv

load_dotenv()


def _list_env(name, default):
    raw = os.getenv(name)
    if raw is None:
        return default
    return [item.strip() for item in raw.split(",") if item.strip()]


# Configuration settings for the distributed crawler
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://postgres:postgres@localhost:5432/crawler")

# Target crawler settings
SEED_URLS = _list_env("SEED_URLS", ["https://quotes.toscrape.com/"])
ALLOWED_DOMAINS = _list_env("ALLOWED_DOMAINS", ["quotes.toscrape.com"])

# Performance & Politeness tuning
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))
POLITE_DELAY = float(os.getenv("POLITE_DELAY", "1.0"))  # Seconds to wait between requests
DEPTH_LIMIT = int(os.getenv("DEPTH_LIMIT", "3"))
