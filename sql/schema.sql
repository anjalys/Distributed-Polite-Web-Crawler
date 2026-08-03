-- Storage layer for crawled pages.
-- Workers upsert here after a page passes Redis-side dedup, so this table
-- holds one current row per URL (history/versioning is not tracked).

CREATE TABLE IF NOT EXISTS pages (
    id BIGSERIAL PRIMARY KEY,
    url TEXT NOT NULL UNIQUE,
    domain TEXT NOT NULL,
    depth INTEGER NOT NULL,
    status_code INTEGER,
    content_hash TEXT,
    title TEXT,
    text_content TEXT,
    char_count INTEGER,
    first_crawled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_crawled_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pages_domain ON pages (domain);
CREATE INDEX IF NOT EXISTS idx_pages_last_crawled_at ON pages (last_crawled_at);
