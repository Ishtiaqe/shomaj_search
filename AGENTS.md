# AGENTS.md — Shomaj Search: Agent Resumption & Progress Tracker

> **Purpose:** This file is read by AI agents at the start of every new session.
> It contains the full current state of the project, completed work, bugs, and
> exactly what to implement next. When resuming, read this file first.

---

## 🔑 Project Identity

| Key | Value |
|-----|-------|
| **Project Name** | Shomaj Search |
| **Domain** | shomaj.one → future: search.shomaj.one |
| **GitHub Repo** | https://github.com/Ishtiaqe/shomaj_search |
| **Local Path** | /mnt/storage/Projects/shomaj_search |
| **Python Venv** | .venv/ (Python 3.14, pip-installed) |
| **Server Port** | 8000 |
| **DB File** | shomaj_search.db (SQLite WAL, FTS5) |
| **Git User** | Ishtiaqe Hanif <ishtiaqe.hanif@gigalogy.com> |
| **GitHub User** | Ishtiaqe |

---

## 🗂️ Session Log

### Session 1 — 2026-06-14 (Initial Build)
**Agent:** Antigravity (Claude Sonnet 4.6 Thinking)

**Accomplished:**
- Built complete backend: `database.py`, `crawler.py`, `main.py`
- Built browser extension (MV3): `manifest.json`, `content.js`, `background.js`, `popup.html`
- Built search dashboard: `index.html` (glassmorphism dark UI)
- Built integration tests: `verify_stack.py` (54/54 passing)
- Created `README.md`
- Installed deps via `.venv`
- Confirmed server runs at `localhost:8000`

**Session 2 — 2026-06-14 (Media Support + GitHub + Progress Tracking)**
**Agent:** Antigravity (Claude Sonnet 4.6 Thinking)

**Accomplished:**
- Created AGENTS.md (this file)
- Added media (image/video) metadata schema to database
- Added `/api/search/images` and `/api/search/videos` endpoints  
- Enhanced UI: search history, filter tabs (Web/Images/Videos/Private), autocomplete
- Added Google-like UX: instant search, keyboard nav, result snippets with time
- Published repo to GitHub: https://github.com/Ishtiaqe/shomaj_search
- Committed all work with structured commit messages

**Session 3 — 2026-06-14 (Caching + Safe Search + Robots.txt Compliance)**
**Agent:** Antigravity (Gemini 3.5 Flash High)

**Accomplished:**
- Fixed SQLite FTS5 product search query `no such column: f` error by passing table names directly to `bm25` and `MATCH`.
- Implemented `robots.txt` compliance in the active crawler, using cached in-memory `RobotFileParser` parsed from asynchronous fetches.
- Implemented dynamic cache system (`cache.py`) with LRU logic for all search endpoints (Web, Images, Videos, Products), providing sub-millisecond responses.
- Implemented Safe Search toggle (`safe_search` parameter) filtering adult content via both FTS5 keyword exclusion and URL pattern exclusions in database queries.
- Updated `index.html` with configuration elements and bindings to control `respect_robots_txt` in the browser dashboard.
- Extended the integration test suite in `verify_stack.py` to cover robots.txt config updating, caching invalidation on writes, and Safe Search filtering (all 74/74 assertions passing).

**Session 4 — 2026-06-14 (Persistent Config + AVIF Thumbnails + Localisation + UX)**
**Agent:** Antigravity (Gemini 3.5 Flash High)

**Accomplished:**
- Implemented persistent settings database schema in `database.py` (`system_settings` table) and loaded/saved crawler configs to/from SQLite.
- Integrated background task processing in `main.py` (`BackgroundTasks`) to scale, compress, and save crawled and extension-ingested images/videos as local AVIF thumbnails.
- Mounted `/static` directory in FastAPI to serve the generated AVIF thumbnails.
- Refactored `index.html` to disable the "search-as-you-type" behaviour, performing queries only on Enter or search button click.
- Formatted dates to Bangladeshi local standard (`dd-mm-yyyy`) and currency values to Bangladeshi Taka (৳/BDT), including USD/EUR conversion rates.
- Enabled image thumbnails in web search cards, rendering them with a premium flexbox layout.
- Added and registered a new configuration persistence test `test_config_persistence` in `verify_stack.py` (now 78/78 assertions passing).

---

## 📁 File Inventory

| File | Status | Description |
|------|--------|-------------|
| `database.py` | ✅ Complete | SQLite WAL+FTS5, connection factory, upsert, queue helpers, media tables |
| `cache.py` | ✅ Complete | Fast in-memory LRU search results cache with invalidation |
| `crawler.py` | ✅ Complete | 4-state async engine, blocklist, rate limiting, BS4 extraction, robots.txt compliance |
| `main.py` | ✅ Complete | FastAPI, 10+ routes, search, index, crawler control, media search, caching, Safe Search |
| `index.html` | ✅ Complete | Glassmorphism dashboard, search history, filter tabs, image/video search, robots.txt toggle |
| `verify_stack.py` | ✅ Complete | 78 assertions, all passing |
| `media_utils.py` | ✅ Complete | Downscaling, YouTube extraction and AVIF compression utilities |
| `product_extractor.py` | ✅ Complete | E-commerce schema extraction (JSON-LD, OpenGraph, Microdata) |
| `requirements.txt` | ✅ Complete | fastapi, uvicorn, httpx, beautifulsoup4, python-multipart, pillow, pillow-avif-plugin |
| `README.md` | ✅ Complete | Full docs |
| `AGENTS.md` | ✅ Complete | This file |
| `extension/manifest.json` | ✅ Complete | MV3, Chrome/Brave/Firefox |
| `extension/content.js` | ✅ Complete | 2.5s debounce, link extraction |
| `extension/background.js` | ✅ Complete | Fire-and-forget relay, exponential backoff |
| `extension/popup.html` | ✅ Complete | Status indicator, backend link |
| `extension/icons/*.png` | ✅ Complete | 16px, 48px, 128px icons |

---

## 🏗️ Architecture Summary

```
Browser (User)
  │
  ├─ Visits page → Extension content.js (2.5s debounce)
  │                  → background.js → POST /api/index
  │                                         ↓
  │                                   SQLite FTS5 (is_private=1)
  │                                   + seeds queue (public links)
  │
  └─ Opens http://localhost:8000
       ├─ Search Dashboard (index.html)
       ├─ GET /api/search?q=...      → FTS5 BM25 (web pages)
       ├─ GET /api/search/images?q=  → SQLite media_index (images)
       └─ GET /api/search/videos?q=  → SQLite media_index (videos)

Active Crawler (background asyncio task)
  └─ crawler.py pulls from queue table
  └─ fetches HTTP, extracts text + links + media URLs
  └─ writes to search_index + media_index
  └─ respects blocklist + per-domain rate limiting
```

---

## 🗃️ Database Schema (Current)

```sql
-- Full-text search (web pages)
CREATE VIRTUAL TABLE search_index USING fts5(
    url UNINDEXED, title, clean_content,
    tokenize='porter unicode61'
);

-- Page metadata
CREATE TABLE crawl_metadata (
    url TEXT PRIMARY KEY,
    domain TEXT NOT NULL,
    last_scanned INTEGER NOT NULL,
    is_private INTEGER DEFAULT 0
);

-- Crawler work queue
CREATE TABLE queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT UNIQUE NOT NULL,
    depth INTEGER DEFAULT 0,
    status TEXT DEFAULT 'pending'   -- pending|completed|failed
);

-- Media metadata (images and videos — no local storage, URL references only)
CREATE TABLE media_index (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    media_url TEXT UNIQUE NOT NULL,     -- direct URL to the media file
    page_url TEXT NOT NULL,              -- page where it was found
    media_type TEXT NOT NULL,            -- 'image' | 'video'
    title TEXT DEFAULT '',               -- alt text / video title
    description TEXT DEFAULT '',         -- surrounding text / caption
    domain TEXT NOT NULL,
    width INTEGER DEFAULT 0,             -- image width (if known)
    height INTEGER DEFAULT 0,            -- image height (if known)
    duration_seconds REAL DEFAULT 0,     -- video duration (if known)
    thumbnail_url TEXT DEFAULT '',       -- video thumbnail / image itself
    format TEXT DEFAULT '',              -- jpg, png, mp4, webm, etc.
    file_size_bytes INTEGER DEFAULT 0,   -- if known from Content-Length
    is_private INTEGER DEFAULT 0,        -- 1 if from browser extension
    llm_description TEXT DEFAULT '',     -- FUTURE: LLM-generated description
    llm_tags TEXT DEFAULT '',            -- FUTURE: LLM-generated comma-sep tags
    indexed_at INTEGER NOT NULL
);

-- Full-text search over media
CREATE VIRTUAL TABLE media_fts USING fts5(
    media_url UNINDEXED,
    title,
    description,
    llm_description,
    llm_tags,
    tokenize='porter unicode61'
);
```

---

## 🌐 API Surface (Current)

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| GET | `/` | — | Dashboard HTML |
| GET | `/health` | — | Health check |
| GET | `/api/search` | — | FTS5 web search (`q`, `limit`, `offset`) |
| GET | `/api/search/images` | — | Image search (`q`, `limit`, `offset`) |
| GET | `/api/search/videos` | — | Video search (`q`, `limit`, `offset`) |
| POST | `/api/index` | — | Extension ingestion (`url`,`title`,`text`,`links`,`images`,`videos`) |
| GET | `/api/stats` | — | Index statistics |
| GET | `/api/history` | — | Search history (server-side log) |
| POST | `/api/crawl/start` | — | Start/resume crawler |
| POST | `/api/crawl/pause` | — | Pause crawler |
| POST | `/api/crawl/stop` | — | Stop + reset crawler |
| POST | `/api/crawl/config` | — | Update `delay_seconds`, `max_depth` |
| POST | `/api/crawl/seed` | — | Seed URLs into queue |
| GET | `/api/crawl/status` | — | State + queue stats |
| GET | `/docs` | — | Swagger UI |

---

## ✅ Completed Features

- [x] SQLite FTS5 with WAL, BM25 scoring
- [x] 4-state crawler state machine (IDLE/RUNNING/PAUSED/STOPPED)
- [x] Domain blocklist (20+ domains + pattern matching)
- [x] Per-domain rate limiting
- [x] Browser extension MV3 (Chrome/Brave/Firefox)
- [x] Content script with 2.5s debounce
- [x] Background service worker with exponential backoff retry
- [x] Passive ingestion (`POST /api/index`)
- [x] Active web crawler (asyncio + httpx)
- [x] Glassmorphism dark dashboard
- [x] Pagination
- [x] Filter tabs (All / Web / Images / Videos / Private)
- [x] Search history (localStorage)
- [x] Autocomplete / instant search
- [x] Crawler control panel (start/pause/stop/config/seed)
- [x] Real-time stats polling (5s interval)
- [x] Media (image/video) metadata schema
- [x] Image search endpoint
- [x] Video search endpoint
- [x] GitHub repository published
- [x] Media extraction in crawler (parsing `<img>` tags and `<video>` sources)
- [x] Extension media extraction (forwarding extracted media arrays)
- [x] Safe Search toggle (excluding adult terms via FTS5 and URL wildcard filters)
- [x] Search result caching (sub-millisecond fast LRU memory cache with write-invalidation)
- [x] `robots.txt` compliance (checking disallowed directories, cached asynchronously)
- [x] Sitemap.xml discovery (seed crawl queue from robots.txt and sitemap files)

---

## 🚧 In Progress / Partial

*None — all core roadmap features fully verified and integrated.*

---

## 📋 Planned / Not Yet Implemented

### High Priority

*None — all high-priority features completed!*

### Medium Priority  
- [ ] **Related searches** — suggest related queries based on index content
- [ ] **Date range filter** — filter results by `last_scanned` date
- [ ] **Domain filter** — restrict search to specific domain (site: operator)
- [ ] **File type filter** — search only PDF, DOC, etc.
- [ ] **Infinite scroll** — replace pagination with infinite scroll on results
- [ ] **Dark/light mode toggle**
- [ ] **Export search results** — CSV/JSON download

### Low Priority / Future
- [ ] **LLM metadata generation** — pipe media through LLM to generate descriptions and tags
- [ ] **Vector embeddings** — semantic search alongside BM25
- [ ] **HTTPS / TLS** — for search.shomaj.one deployment
- [ ] **Multi-user auth** — protect private index per user
- [ ] **Mobile PWA** — service worker for offline dashboard
- [ ] **Browser history sync** — import browser history for bulk indexing
- [ ] **Scheduled crawl jobs** — cron-based recrawl of stale pages
- [ ] **Spelling correction** — "Did you mean" suggestions

---

## 🐛 Known Issues / Bugs

| ID | Severity | Description | Status |
|----|----------|-------------|--------|
| BUG-01 | Low | BM25 score shows `0.0` when all results match with equal frequency | Open — cosmetic only, ranking still correct |
| BUG-02 | Low | Test suite test-13 (seed) uses timestamped URLs — generates queue noise on each test run | Acceptable tradeoff |

---

## 🔧 Environment & Setup

```bash
# Start server
cd /mnt/storage/Projects/shomaj_search
source .venv/bin/activate
uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# Run tests
python verify_stack.py

# Access dashboard
open http://localhost:8000

# GitHub
gh repo view Ishtiaqe/shomaj_search
```

---

## 💬 Questions to Ask User on Resume

1. Should the extension also extract and send `<img>` and `<video>` URLs automatically?
2. Do you want `robots.txt` compliance enforced in the active crawler?
3. Should search history be server-side (persisted across devices) or client-side only (localStorage)?
4. For `search.shomaj.one` deployment: do you want a reverse proxy config (nginx/caddy)?
5. What LLM provider should be used for future media description generation (Gemini, OpenAI, local)?
6. Should the search engine be publicly accessible, or strictly local/private?

---

## 📦 Git Commit History (Latest First)

| Hash | Message |
|------|--------|
| `8b03842` | feat: initial full-stack implementation of Shomaj Search |

---

*Last updated: 2026-06-14 by Antigravity agent (Session 2)*
