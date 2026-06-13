# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Shomaj Search is a local, zero-cloud hybrid search engine: a FastAPI backend with a SQLite FTS5
index, an async open-web crawler, and a Manifest V3 browser extension for passive indexing.

## Commands

```bash
# Activate venv (Python 3.14, already provisioned in .venv/)
source .venv/bin/activate

# Install/update dependencies
pip install -r requirements.txt

# Run the server (auto-reload)
uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# Run the full integration test suite (server must already be running)
python verify_stack.py
```

There is no separate unit test runner — `verify_stack.py` is a single self-contained script
(stdlib `urllib` only) that hits the live API and asserts on responses/DB state. It runs
sequentially and prints colored PASS/FAIL per assertion; there is no way to run a single test
in isolation — read the script to find the relevant test block if debugging one area.

API docs are auto-served at `/docs` (Swagger) and `/redoc` once the server is running.

## Architecture

### Data flow — two ingestion paths feed one set of tables

1. **Passive (browser extension)**: `extension/content.js` scrapes the active tab (2.5s debounce
   after DOM settles), `extension/background.js` POSTs to `/api/index` with `{url, title, text,
   links, images, videos}`. Pages indexed this way are tagged `is_private=1`. Any http(s) links
   found on the page are seeded into the crawler `queue` table (depth=1) so the public crawl
   graph grows from pages the user visits.

2. **Active (async crawler)**: `crawler.py`'s `CrawlerEngine` (singleton `engine`, imported
   everywhere) runs `concurrent_workers` asyncio worker loops that pop from the `queue` table,
   fetch via a shared `httpx.AsyncClient`, parse with BeautifulSoup, and write to the same tables
   with `is_private=0` (unless the domain is registered private in `managed_domains`).

Both paths converge on `database.py` helpers (`upsert_index`, `upsert_media`, `upsert_product`,
`enqueue_url`) — any schema/behavior change to indexing must be consistent for both callers.

### Crawler state machine (`crawler.py`)

`CrawlerEngine` is a 4-state machine: `IDLE → RUNNING ↔ PAUSED → STOPPED → IDLE`, driven entirely
through `/api/crawl/*` endpoints in `main.py`. Key points:

- `CrawlerConfig` holds all runtime-tunable settings (delay, depth, worker count, robots.txt
  compliance, etc.), persisted to the `system_settings` table via `load_from_db`/`save_to_db`.
- `/api/crawl/config` updates apply live — `adjust_workers()` scales worker tasks up without a
  restart; workers scale down by self-exiting when `worker_id >= concurrent_workers`.
- Per-domain rate limiting (`_domain_last_fetch`) and `robots.txt` compliance (`_robots_txt_parsers`,
  cached `RobotFileParser` per domain) are both enforced only by the active crawler — extension
  ingestion bypasses both.
- `BLOCKED_DOMAINS` / `BLOCKED_PATTERNS` at the top of `crawler.py` is a hardcoded blocklist
  (social networks, auth/login subdomains, etc.) checked via `engine.is_blocked()`. This blocklist
  applies to the active crawler and to link-seeding from `/api/index` and `/api/crawl/seed` — it
  does NOT prevent the extension from indexing those pages directly (they're stored as
  `is_private=1`).
- `managed_domains` table overrides per-domain behavior: `is_public`/`crawl_enabled`/`priority`,
  configured via `POST /api/domains/{domain}`. Setting `is_public` retroactively rewrites
  `is_private` on existing `crawl_metadata`/`product_index` rows for that domain.
- On each new domain (`depth == 0`), `discover_and_seed_sitemap()` fires as a background task to
  find and parse `sitemap.xml` (including nested sitemap indexes) and bulk-seed the queue.

### Database (`database.py`)

- One SQLite connection per thread (`threading.local`), WAL mode, with the PRAGMA tuning
  documented in README.md. `get_db()` is the only way to get a connection.
- FTS5 virtual tables (`search_index`, `media_fts`, `product_fts`) do not support `ON CONFLICT`,
  so upserts follow a **DELETE-then-INSERT** pattern into the FTS table paired with a real
  `ON CONFLICT ... DO UPDATE` on the corresponding metadata table (`crawl_metadata`,
  `media_index`, `product_index`). When adding new upsert helpers, follow this same pattern.
- Every write helper that can affect search results calls `search_cache.clear()` — if you add a
  new mutation path, make sure it invalidates `cache.search_cache` too (see `cache.py`).
- `init_db()` runs `_SCHEMA_SQL` (all `CREATE ... IF NOT EXISTS`) plus `_safe_add_column()` calls
  for additive migrations on existing DB files — there is no migration framework, so new columns
  must be added via `_safe_add_column` calls in `init_db()`.

### Product extraction (`product_extractor.py`)

`ProductExtractor.extract()` runs a fixed priority chain — JSON-LD → OpenGraph → `<meta>` tags →
Microdata → heuristic CSS/text — and stops at the first strategy that returns a name (heuristic
additionally requires a price or a product-URL pattern match). Prices are normalized via
`parse_price()` into `(numeric, display_text, currency)`, with currency auto-detected from symbols
(৳/Tk/BDT, $, €, £). Only invoked by the crawler when `config.extract_products` is true.

### Search endpoints (`main.py`)

`/api/search`, `/api/search/images`, `/api/search/videos`, `/api/search/products` share a common
pattern:
- Build an FTS5 `MATCH` query by quoting each whitespace-split term and joining with `AND`.
- When `safe_search=True` (default), append `NOT "<term>"` clauses for a hardcoded NSFW wordlist
  *and* add `NOT LIKE '%term%'` filters on the URL column — both layers must stay in sync if the
  wordlist changes.
- Check `search_cache` (simple LRU in `cache.py`, keyed by a tuple of all query params) before
  hitting SQLite, and populate it with the full JSON response on the way out.
- Product search additionally supports `sort` (relevance/price_asc/price_desc) and
  `prioritize_stock` (sorts by an availability `CASE` expression first).

### Media thumbnails (`media_utils.py`)

After indexing an image/video, the crawler fires a background `asyncio.create_task` to download
the source image, downscale it, and re-encode as AVIF under `static/thumbnails/`, then updates
`media_index.thumbnail_url` to the local path. This is best-effort and non-blocking — failures
silently fall back to the original remote URL.

### Browser extension (`extension/`)

MV3, loaded unpacked. `content.js` runs in the page, debounces 2.5s after content settles, then
sends scraped data to `background.js` (service worker), which POSTs to `/api/index` with
exponential-backoff retry. `popup.html` shows connection status to the local backend.
