# Distributed, Polite Web Crawler

An end-to-end data ingestion platform, not just a scraping. It combines async I/O, multiprocessing, Redis-backed distributed coordination, and a Postgres ETL pipeline to crawl at scale without hammering target sites or losing state on a crash.

## Why This Project

Most scraping scripts are a single loop with a `requests.get()` call. This is the version of that problem  we could actually build at a company with real traffic: state that survives a crashed worker, deduplication that doesn't fall over at scale, rate limiting that's enforced across a whole fleet instead of per-process, and a real ETL path into a queryable store instead of print statements. It's built to demonstrate the same judgment calls that show up in production data infrastructure — decoupling producers from consumers via a shared queue, trading exactness for memory with a bounded false-positive rate, and making coordination atomic instead of hoping workers behave.

## Live Dashboard

![Distributed crawler live monitor](docs/dashboard.png)

*Mid-crawl: 25 concurrent async workers across a 5-process cluster, sharing one Redis-enforced politeness lock on the target domain, with the queue draining in real time.*

## Architecture

```
┌──────────────────┐                  ┌──────────────────────────────┐                  ┌────────────────────────────┐
│ Streamlit UI     │  enqueue seed URL│ REDIS                        │  pop URL, check  │ 5x Worker Processes        │
│ (live dashboard) │  ───────────▶    │ - Frontier (ZSET)            │  ──────────▶     │ (asyncio pool per          │
│                  │                  │ - Bloom filters (BF.*)       │  bloom + lock    │ process, N=MAX_WORKERS)    │
└──────────────────┘  ◀───────────    │ - Domain locks (NX PX)       │  ◀──────────     │                            │
                      poll live KPIs  │                              │  enqueue links   └────────────────────────────┘
                                      └──────────────────────────────┘                                │
                                                                                                      ▼
                                                                                        ┌────────────────────────────┐
                                                                                        │ PostgreSQL                 │
                                                                                        │ pages (atomic UPSERT)      │
                                                                                        └────────────────────────────┘
```

## How It Works

1. **Scale & performance** — `asyncio` + `aiohttp` give non-blocking network I/O within a process; `multiprocessing` (`src/worker/launcher.py`) runs multiple worker processes in parallel to get past the GIL. Each process also runs its own pool of async workers, so the default configuration (5 processes × `MAX_WORKERS=5`) drives up to 25 concurrent fetches, all coordinated through shared Redis state.
2. **Memory-efficient state at scale** — the URL frontier is a Redis Sorted Set (`ZSET`), acting as a shared priority queue so any worker in the cluster can pop the next URL without stepping on another. Visited-URL and seen-content dedup use **RedisBloom** (`BF.ADD` / `BF.EXISTS`) instead of raw string sets, so memory stays flat as the crawl grows into the millions of URLs (at the cost of a small, tunable false-positive rate — an unseen URL is occasionally, rarely, skipped as "already seen"; it never mistakenly re-crawls one that's genuinely done).
3. **Resiliency** — all crawl state (frontier, dedup filters, locks) lives in Redis, not in worker memory. If a worker process or the whole machine dies mid-crawl, restarting picks up exactly where it left off.
4. **Politeness** — `robots.txt` is fetched and cached per process via `urllib.robotparser`. Domain-level rate limiting is enforced with an atomic Redis lock (`SET lock:{domain} active NX PX <delay_ms>`), so no matter how many workers or processes are running, only one request goes out to a given domain per polite-delay window, cluster-wide.
5. **ETL into Postgres** — Extract (async fetch) → Transform (`src/pipeline/parser.py`: strips `<script>`/`<style>`, normalizes whitespace, extracts title + character count) → Load (atomic `INSERT ... ON CONFLICT (url) DO UPDATE` via an `asyncpg` connection pool, so re-crawling a URL updates its row instead of duplicating it).
6. **Live observability** — the Streamlit dashboard (`src/dashboard.py`) polls Redis directly for queue size, pages crawled, worker heartbeats, and active domain locks, without touching the crawl path itself.

## Benchmark

Measured locally with Redis Stack and Postgres 16 — not simulated. Two runs, same 25-worker cluster (5 processes × 5 async workers), different seed sets:

| | Run 1: single-domain | Run 2: multi-domain (28 seeds) |
|---|---|---|
| Target | [quotes.toscrape.com](https://quotes.toscrape.com/) only | 28 distinct domains (`docs.python.org`, `developer.mozilla.org`, `books.toscrape.com`, etc.) |
| `depth_limit` | 3 | 1 |
| Pages crawled | 168 | 210 |
| Wall-clock time | 191.2s | ~140s |
| Throughput | 0.88 pages/sec, flat the entire run | 3.3 pages/sec burst (first ~10s) → 1.5 pages/sec sustained |
| Domain locks in play | 1 | up to 27 |

**Run 1 proves the politeness lock works as designed.** All 25 workers were fighting over one key, `lock:quotes.toscrape.com` — no matter how many workers exist, only one can hold a given domain's lock at a time, so throughput pins near `1 ÷ polite_delay` regardless of worker count. That's not a concurrency ceiling, it's the feature.

**Run 2 proves horizontal scaling works, but reveals a subtler bottleneck.** Spreading the seed across 28 domains means 27 independent lock keys, so workers stop contending and each holds a different domain's lock — hence the ~4x burst in the first 10 seconds. It didn't hold at 4x, though: of the 27 domains actually crawled, only 4 (`books.toscrape.com`, `developer.hashicorp.com`, `click.palletsprojects.com`, `developer.mozilla.org`) had deep enough internal link graphs to keep supplying new URLs. The other 23 exhausted their in-scope links after just the seed page and went idle. So partway through the run, **effective concurrency quietly collapsed from 27 active domains down to 4** — worker count never changed, but the throughput ceiling did, because it's bounded by *domains currently generating backlog*, not the size of the original seed list.

![Dashboard during the multi-domain run](docs/dashboard_multidomain.png)

*25 workers active, 3,532 URLs queued from the link-rich domains, 120 pages crawled at this point in the run.*

**Bloom filter memory, measured directly with Redis `MEMORY USAGE`:**

| Structure | Items | Memory |
|---|---|---|
| Raw Redis Set (actual crawled URLs) | 168 | 18.5 KB (~110 bytes/URL) |
| RedisBloom filter (`crawler:bloom:urls`) | sized for 1,000,000 @ 0.1% FP rate | 1.93 MB (fixed) |

At this tiny scale the raw Set is smaller — the Bloom filter's cost is dominated by its pre-allocated capacity, not by what's actually in it. That crosses over around **~18,000 URLs**, and the gap only widens from there: extrapolating the measured ~110 bytes/URL out to the filter's full 1M-URL capacity puts the raw Set at **~110 MB** versus the Bloom filter's fixed **~1.93 MB** — roughly **57x smaller** at the scale this design actually targets. The trade is a small, tunable false-positive rate (0.1% here): occasionally an unseen URL gets skipped as "already seen," but a truly-seen one is never mistakenly re-crawled.

## Repo Layout

```
.
├── .streamlit/config.toml    # Dashboard theming
├── docs/dashboard.png        # Live dashboard screenshot
├── sql/schema.sql            # Postgres DDL (pages table + indexes)
├── src/
│   ├── config.py              # Env-var-backed settings (.env)
│   ├── connection.py          # Shared async Redis + Postgres connection pools
│   ├── dashboard.py           # Streamlit live systems monitor
│   ├── pipeline/
│   │   └── parser.py          # HTML cleaning & text/title extraction
│   └── worker/
│       ├── main.py            # DistributedCrawler: fetch, dedup, lock, upsert
│       └── launcher.py        # Multiprocessing harness (N worker processes)
├── main.py                    # Single-process entry point
├── docker-compose.yml         # Redis Stack + Postgres for local dev
├── .env.example
└── requirements.txt
```

## Setup

### Prerequisites
* Python 3.11+
* Docker (recommended), or locally installed **Redis Stack** (RedisBloom module — plain Redis won't work) and **PostgreSQL**

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Start infrastructure
```bash
docker compose up -d
```
This starts Redis Stack on `6379` and Postgres on `5432`, and applies `sql/schema.sql` automatically on first boot.

If you're not using Docker, start Redis Stack and Postgres yourself and apply the schema manually:
```bash
psql -U postgres -d crawler -f sql/schema.sql
```

### 3. Configure
```bash
cp .env.example .env
```
Edit `.env` to change seed URLs, allowed domains, worker count, politeness delay, crawl depth, or connection strings. Everything has a sane default if you skip this step.

### 4. Run

Single process (simplest, one asyncio worker pool):
```bash
python main.py
```

Distributed cluster (5 worker processes, matches the architecture above):
```bash
python -m src.worker.launcher
```

Live dashboard:
```bash
streamlit run src/dashboard.py
```
