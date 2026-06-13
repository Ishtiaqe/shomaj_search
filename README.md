# Shomaj Search 🔍

> **Local hybrid search engine** — Your private intelligence engine.
> Future home: [search.shomaj.one](https://shomaj.one)

---

## What is Shomaj Search?

Shomaj Search is a **zero-cloud, privacy-first** search engine that runs entirely on your machine. It combines:

1. **Passive indexing** via a browser extension — every page you visit is indexed privately
2. **Active open-web crawling** — an async worker that autonomously discovers and indexes public web content
3. **SQLite FTS5** full-text search with native BM25 relevance scoring
4. **FastAPI backend** — lightweight, async, zero idle RAM overhead
5. **Web dashboard** — real-time crawler control & search interface

---

## Architecture

```
Browser Extension (MV3)
  └─ content.js  ──────────────────────────────────────────────────────┐
  └─ background.js ──→  POST /api/index  ──→  SQLite FTS5              │
                                               (is_private=1)          │
                                               + seeds public queue    │
                                                                       │
Active Crawler (asyncio)                                               │
  └─ crawler.py  ──→  pulls from queue ──→  fetches + extracts ──→  search_index (is_private=0)
                       (blocklist enforced)    (BeautifulSoup)

FastAPI (main.py)
  ├─ GET  /                     → Search/Control Dashboard
  ├─ GET  /api/search?q=...     → FTS5 BM25 search
  ├─ POST /api/index            → Extension ingestion
  ├─ POST /api/crawl/start      → Start crawler
  ├─ POST /api/crawl/pause      → Pause crawler
  ├─ POST /api/crawl/stop       → Stop + reset crawler
  ├─ POST /api/crawl/config     → Update delay/depth
  ├─ POST /api/crawl/seed       → Seed URLs into queue
  ├─ GET  /api/crawl/status     → State + queue stats
  ├─ GET  /api/stats            → Index stats
  └─ GET  /health               → Health check
```

---

## Quick Start

### 1. Backend Setup

```bash
cd shomaj_search

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Start the server
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Open [http://localhost:8000](http://localhost:8000) in your browser.

### 2. Browser Extension

Load the `extension/` directory as an **unpacked extension**:

- **Chrome / Brave:** `chrome://extensions` → Enable Developer Mode → Load Unpacked → select `extension/`
- **Firefox:** `about:debugging` → This Firefox → Load Temporary Add-on → select `extension/manifest.json`

### 3. Run Tests

```bash
# With the server running in another terminal:
python verify_stack.py
```

---

## File Structure

```
shomaj_search/
├── database.py        # SQLite WAL + FTS5 schema + helpers
├── crawler.py         # Async crawler engine + state machine
├── main.py            # FastAPI app + all API routes
├── index.html         # Search/control dashboard UI
├── verify_stack.py    # 54-assertion integration test suite
├── requirements.txt   # Python dependencies
└── extension/
    ├── manifest.json  # MV3 manifest (Chrome/Brave/Firefox)
    ├── content.js     # Page scraper (2.5s debounce)
    ├── background.js  # Relay service worker
    ├── popup.html     # Extension popup with status
    └── icons/         # 16x16, 48x48, 128x128 PNGs
```

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/search?q=query&limit=20&offset=0` | FTS5 BM25 search |
| `POST` | `/api/index` | Extension ingestion (`url`, `title`, `text`, `links`) |
| `POST` | `/api/crawl/start` | Start/resume crawler |
| `POST` | `/api/crawl/pause` | Pause without queue flush |
| `POST` | `/api/crawl/stop` | Stop + reset to IDLE |
| `POST` | `/api/crawl/config` | Update `delay_seconds`, `max_depth` |
| `POST` | `/api/crawl/seed` | Seed `urls[]` into queue |
| `GET` | `/api/crawl/status` | State + queue stats |
| `GET` | `/api/stats` | Index aggregate stats |
| `GET` | `/health` | Health check |

---

## SQLite Optimisations

| PRAGMA | Value | Purpose |
|--------|-------|---------|
| `journal_mode` | `WAL` | Concurrent reads never block writes |
| `synchronous` | `NORMAL` | fsync only on checkpoints (safe with WAL) |
| `cache_size` | `-64000` | Up to 64 MB page cache, released when idle |
| `temp_store` | `MEMORY` | Temp tables in RAM not on disk |
| `mmap_size` | `268435456` | 256 MB memory-mapped I/O for large DBs |

---

## Domain Blocklist (Active Crawler)

The active crawler **never** fetches from:
`facebook.com`, `messenger.com`, `whatsapp.com`, `instagram.com`,
`twitter.com`, `x.com`, `drive.google.com`, `mail.google.com`,
`docs.google.com`, `accounts.google.com`, `localhost`, `127.0.0.1`,
`linkedin.com`, `tiktok.com`, `pinterest.com`, `snapchat.com`,
`reddit.com`, `youtube.com`, `netflix.com`, `amazon.com`, `ebay.com`

Wildcard patterns blocked: `accounts.*`, `login.*`, `signin.*`, `auth.*`, `sso.*`

> **Note:** Pages from these domains are still indexed if sent by the browser extension (they're flagged `is_private=1`). The blocklist only restricts the autonomous crawler.

---

## Future Roadmap (search.shomaj.one)

- [ ] HTTPS / TLS with Let's Encrypt
- [ ] User authentication for multi-user deployments
- [ ] Vector embeddings for semantic search (hybrid BM25 + cosine)
- [ ] Scheduled crawl jobs via cron
- [ ] Export / import index
- [ ] Mobile-responsive PWA dashboard
