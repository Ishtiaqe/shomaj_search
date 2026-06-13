"""
main.py — Shomaj Search
FastAPI application: API routes for search, crawler control,
passive ingestion from the browser extension, and CORS middleware.
"""

import logging
import re
import time
from contextlib import asynccontextmanager
from typing import Any, Optional
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

from crawler import CrawlerState, engine
from database import (
    enqueue_url, get_db, init_db, upsert_index,
    upsert_media, log_search_history, upsert_product,
    upsert_managed_domain, get_domain_config, get_all_domains,
    submit_feedback,
)
from cache import search_cache
from media_utils import process_media_thumbnail_bg


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("shomaj.api")


# ---------------------------------------------------------------------------
# Application lifespan — initialise DB on startup
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    engine.load_config()  # Load configuration from database
    logger.info("[API] Shomaj Search backend is ready.")
    yield
    logger.info("[API] Shutting down.")


# ---------------------------------------------------------------------------
# FastAPI instance
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Shomaj Search API",
    description=(
        "Local hybrid search engine with active open-web crawler, "
        "passive browser-extension indexing, and SQLite FTS5 search."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# CORS — allow the extension and local dashboard to call the API
# ---------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],     # Extension origins are chrome-extension:// etc.
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve static files (thumbnails)
import os
os.makedirs("static/thumbnails", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------
class IndexPayload(BaseModel):
    """Payload sent by the browser extension content/background scripts."""
    url:    str        = Field(..., description="Full URL of the page")
    title:  str        = Field("",  description="Page <title> text")
    text:   str        = Field("",  description="Visible body text extracted by the extension")
    links:  list[str]  = Field(default_factory=list, description="hrefs of text-containing anchors")
    images: list[dict] = Field(
        default_factory=list,
        description="Image metadata: [{url, alt, width, height, title}]"
    )
    videos: list[dict] = Field(
        default_factory=list,
        description="Video metadata: [{url, title, thumbnail_url, duration_seconds}]"
    )

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        parsed = urlparse(v)
        if parsed.scheme not in ("http", "https", "chrome", "chrome-extension",
                                  "moz-extension", "file"):
            raise ValueError(f"Unsupported URL scheme: {parsed.scheme!r}")
        return v.strip()


class CrawlConfigPayload(BaseModel):
    """Runtime configuration update for the active crawler."""
    # Pacing
    delay_seconds:  Optional[float] = Field(None, ge=0.0,  le=60.0,  description="Seconds between requests per domain")
    max_depth:      Optional[int]   = Field(None, ge=0,    le=20,    description="Max hop depth from seed")
    request_timeout: Optional[float]= Field(None, ge=5.0,  le=120.0, description="HTTP timeout in seconds")
    max_content_mb: Optional[float] = Field(None, ge=0.5,  le=50.0,  description="Max response size in MB")
    # Concurrency
    concurrent_workers: Optional[int] = Field(None, ge=1, le=20,    description="Number of parallel workers")
    # Behaviour
    follow_sitemaps:    Optional[bool] = Field(None, description="Auto-discover and parse sitemap.xml")
    extract_products:   Optional[bool] = Field(None, description="Extract structured product data")
    max_pages_per_domain: Optional[int]= Field(None, ge=0, description="0 = unlimited")
    retry_failed:       Optional[bool] = Field(None, description="Re-queue previously failed URLs")
    respect_robots_txt:  Optional[bool] = Field(None, description="Comply with robots.txt Disallow rules")


class SeedPayload(BaseModel):
    """Seed one or more URLs into the crawler queue."""
    urls:  list[str] = Field(..., min_length=1)
    depth: int       = Field(0, ge=0, le=10)


class DomainConfigPayload(BaseModel):
    """Create or update a domain's managed configuration."""
    is_public:     int   = Field(1, ge=0, le=1,  description="1=public (crawlable), 0=private")
    crawl_enabled: int   = Field(1, ge=0, le=1,  description="1=allow active crawler")
    priority:      int   = Field(5, ge=1, le=10, description="Crawl priority 1-10")
    sitemap_url:   str   = Field("",             description="Override sitemap URL")
    notes:         str   = Field("",             description="Human-readable notes")


class FeedbackPayload(BaseModel):
    """Payload to submit user feedback for search relevance/categorization."""
    url:           str   = Field(..., description="Target result URL")
    query:         str   = Field(..., description="User search query")
    feedback_type: str   = Field(..., description="'relevance_vote' or 'product_vote'")
    vote:          int   = Field(..., ge=-1, le=1, description="1 for up/plus, -1 for down/minus")


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
def _domain_from_url(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().lstrip("www.") or "unknown"
    except Exception:
        return "unknown"


def ok(data: Any = None, message: str = "ok") -> JSONResponse:
    return JSONResponse({"status": "ok", "message": message, "data": data})


# ---------------------------------------------------------------------------
# ── Crawler Control Endpoints ──────────────────────────────────────────────
# ---------------------------------------------------------------------------

@app.post("/api/crawl/start", tags=["Crawler Control"])
async def crawl_start():
    """
    Starts or resumes the background crawler.
    Transitions: IDLE → RUNNING or PAUSED → RUNNING.
    """
    result = await engine.start()
    return ok({"result": result, "state": engine.state.value})


@app.post("/api/crawl/pause", tags=["Crawler Control"])
async def crawl_pause():
    """
    Pauses the crawler without flushing the discovery queue.
    Transition: RUNNING → PAUSED.
    """
    result = await engine.pause()
    return ok({"result": result, "state": engine.state.value})


@app.post("/api/crawl/stop", tags=["Crawler Control"])
async def crawl_stop():
    """
    Stops the crawler, cancels the worker task, and resets to IDLE.
    The persistent SQLite queue is NOT flushed (items remain as 'pending').
    In-memory domain rate-limit timestamps are cleared.
    """
    result = await engine.stop()
    return ok({"result": result, "state": engine.state.value})


@app.post("/api/crawl/config", tags=["Crawler Control"])
async def crawl_config(payload: CrawlConfigPayload):
    """
    Dynamically updates crawler configuration at runtime.
    Changes take effect on the next crawler iteration — no restart required.
    All parameters are optional; only provided ones are changed.
    """
    engine.config.update(**payload.model_dump(exclude_none=True))
    await engine.adjust_workers()
    engine.save_config()  # Persist crawler config to DB!
    return ok(engine.config.to_dict())


@app.get("/api/crawl/status", tags=["Crawler Control"])
async def crawl_status():
    """
    Returns current crawler state, items processed, and queue statistics.
    """
    return ok(engine.status())


@app.post("/api/crawl/seed", tags=["Crawler Control"])
async def crawl_seed(payload: SeedPayload):
    """
    Manually seeds one or more URLs into the persistent crawler queue.
    Useful for bootstrapping the crawler with known starting points.
    """
    conn = get_db()
    added = 0
    skipped = 0
    for url in payload.urls:
        url = url.strip()
        if not url:
            continue
        if engine.is_blocked(url):
            skipped += 1
            continue
        if enqueue_url(conn, url, payload.depth):
            added += 1
        else:
            skipped += 1
    conn.commit()
    return ok({"added": added, "skipped": skipped})


# ---------------------------------------------------------------------------
# ── Passive Ingestion (Browser Extension) ─────────────────────────────────
# ---------------------------------------------------------------------------

@app.post("/api/index", tags=["Indexing"])
async def index_page(payload: IndexPayload, background_tasks: BackgroundTasks):
    """
    Accepts page data from the browser extension.

    - Writes the page to search_index with is_private=1.
    - Persists image and video metadata from payload.images / payload.videos.
    - Extracts valid public http/https links from payload.links and seeds
      the active crawler queue (depth=1) so the open-web graph expands.
    - Private pages (chrome://, file://, extension://) are indexed but
      their links are NOT forwarded to the public crawler queue.
    """
    conn = get_db()
    domain = _domain_from_url(payload.url)

    # Determine if this is a truly private/local URL
    parsed = urlparse(payload.url)
    is_web_page = parsed.scheme in ("http", "https")

    # Sanitise text (limit to 200 KB)
    clean_text = payload.text[:200_000].strip()
    title      = (payload.title or payload.url)[:512].strip()

    try:
        upsert_index(
            conn=conn,
            url=payload.url,
            title=title,
            clean_content=clean_text,
            domain=domain,
            is_private=1,
        )

        # Seed public links into crawler queue
        seeded = 0
        if is_web_page:
            for href in payload.links:
                href = href.strip()
                if not href:
                    continue
                parsed_href = urlparse(href)
                if parsed_href.scheme not in ("http", "https"):
                    continue
                if engine.is_blocked(href):
                    continue
                if enqueue_url(conn, href, 1):
                    seeded += 1

        # Persist image metadata
        images_saved = 0
        for img in payload.images[:200]:  # cap at 200 images per page
            img_url = str(img.get("url", "")).strip()
            if not img_url or not img_url.startswith(("http://", "https://")):
                continue
            img_domain = _domain_from_url(img_url)
            ext = img_url.rsplit(".", 1)[-1].lower().split("?")[0] if "." in img_url else ""
            upsert_media(
                conn=conn,
                media_url=img_url,
                page_url=payload.url,
                media_type="image",
                title=str(img.get("alt", img.get("title", "")))[:512],
                description=str(img.get("description", ""))[:1024],
                domain=img_domain,
                width=int(img.get("width", 0) or 0),
                height=int(img.get("height", 0) or 0),
                thumbnail_url=img_url,  # image IS the thumbnail
                fmt=ext,
                is_private=1,
            )
            background_tasks.add_task(process_media_thumbnail_bg, img_url, img_url)
            images_saved += 1

        # Persist video metadata
        videos_saved = 0
        for vid in payload.videos[:50]:  # cap at 50 videos per page
            vid_url = str(vid.get("url", "")).strip()
            if not vid_url or not vid_url.startswith(("http://", "https://")):
                continue
            vid_domain = _domain_from_url(vid_url)
            ext = vid_url.rsplit(".", 1)[-1].lower().split("?")[0] if "." in vid_url else ""
            upsert_media(
                conn=conn,
                media_url=vid_url,
                page_url=payload.url,
                media_type="video",
                title=str(vid.get("title", ""))[:512],
                description=str(vid.get("description", ""))[:1024],
                domain=vid_domain,
                duration_seconds=float(vid.get("duration_seconds", 0) or 0),
                thumbnail_url=str(vid.get("thumbnail_url", ""))[:1024],
                fmt=ext,
                is_private=1,
            )
            background_tasks.add_task(process_media_thumbnail_bg, vid_url, str(vid.get("thumbnail_url", "")))
            videos_saved += 1

        conn.commit()
        logger.info(
            "[API] Extension indexed → %s (links=%d images=%d videos=%d)",
            payload.url, seeded, images_saved, videos_saved,
        )
        return ok({
            "indexed": True,
            "links_seeded": seeded,
            "images_saved": images_saved,
            "videos_saved": videos_saved,
        })

    except Exception as exc:
        conn.rollback()
        logger.error("[API] Failed to index %s: %s", payload.url, exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# ── Search Endpoint ────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

@app.get("/api/search", tags=["Search"])
async def search(
    q:      str = Query(..., min_length=1, max_length=500, description="Search query"),
    limit:  int = Query(20,  ge=1, le=100,  description="Max results to return"),
    offset: int = Query(0,   ge=0,           description="Pagination offset"),
    private_only: bool = Query(False, description="Only return private (extension-indexed) pages"),
    domain: str = Query("",  description="Filter by domain (site: operator)"),
    safe_search: bool = Query(True, description="Filter adult/NSFW content"),
    start_date: Optional[int] = Query(None, description="Start UNIX timestamp"),
    end_date: Optional[int] = Query(None, description="End UNIX timestamp"),
):
    """
    Full-text search using SQLite FTS5 MATCH with native bm25() scoring.

    Returns ranked results with url, title, snippet, and score.
    Supports domain filtering and private-only toggle.
    Search queries are logged to search_history.
    """
    q_clean = q.strip()
    if not q_clean:
        raise HTTPException(status_code=400, detail="Query cannot be empty.")

    # Cache lookup
    cache_key = ("web", q_clean, limit, offset, private_only, domain.strip(), safe_search, start_date, end_date)
    cached_res = search_cache.get(cache_key)
    if cached_res is not None:
        return cached_res

    terms = q_clean.split()
    fts_query = " AND ".join(f'"{re.sub(chr(34), "", t)}"' for t in terms)

    if safe_search:
        exclude_terms = ["porn", "xxx", "sex", "naked", "nude", "adult", "pornography", "erotic", "nsfw", "milf", "ebony"]
        fts_query = f"({fts_query}) " + " ".join(f'NOT "{w}"' for w in exclude_terms)

    conn = get_db()

    # Build optional WHERE clauses for metadata filters
    meta_filters = []
    meta_params  = []
    if private_only:
        meta_filters.append("m.is_private = 1")
    if domain.strip():
        meta_filters.append("m.domain = ?")
        meta_params.append(domain.strip().lstrip("www."))
    if safe_search:
        meta_filters.append("m.url NOT LIKE '%porn%' AND m.url NOT LIKE '%sex%' AND m.url NOT LIKE '%xxx%'")
    if start_date is not None:
        meta_filters.append("m.last_scanned >= ?")
        meta_params.append(start_date)
    if end_date is not None:
        meta_filters.append("m.last_scanned <= ?")
        meta_params.append(end_date)

    meta_where = ("AND " + " AND ".join(meta_filters)) if meta_filters else ""

    try:
        sql = f"""
            SELECT
                s.url,
                s.title,
                snippet(search_index, 2, '<mark>', '</mark>', '\u2026', 40) AS snippet,
                (bm25(search_index, 10.0, 1.0) - (
                    COALESCE((SELECT vote * 2.0 FROM result_feedback WHERE url = s.url AND query = ? AND feedback_type = 'relevance_vote'), 0) +
                    COALESCE((SELECT SUM(vote) * 0.5 FROM result_feedback WHERE url = s.url AND feedback_type = 'relevance_vote'), 0) +
                    COALESCE((SELECT COUNT(*) * 0.2 FROM result_clicks WHERE url = s.url AND query = ?), 0)
                )) AS score,
                m.domain,
                m.is_private,
                m.last_scanned,
                (SELECT thumbnail_url FROM media_index WHERE page_url = s.url AND media_type = 'image' LIMIT 1) AS image_url,
                (SELECT COUNT(*) FROM result_feedback WHERE url = s.url AND feedback_type = 'relevance_vote' AND vote = 1) AS upvotes,
                (SELECT COUNT(*) FROM result_feedback WHERE url = s.url AND feedback_type = 'relevance_vote' AND vote = -1) AS downvotes
            FROM search_index s
            JOIN crawl_metadata m ON m.url = s.url
            WHERE search_index MATCH ?
            {meta_where}
            ORDER BY score ASC
            LIMIT ? OFFSET ?
        """
        rows = conn.execute(
            sql, [q_clean, q_clean] + [fts_query] + meta_params + [limit, offset]
        ).fetchall()

        results = [
            {
                "url":          row["url"],
                "title":        row["title"],
                "snippet":      row["snippet"],
                "score":        round(-row["score"], 4),
                "domain":       row["domain"],
                "is_private":   bool(row["is_private"]),
                "last_scanned": row["last_scanned"],
                "image_url":    row["image_url"],
                "upvotes":      row["upvotes"],
                "downvotes":    row["downvotes"],
            }
            for row in rows
        ]

        count_sql = f"""
            SELECT COUNT(*) AS cnt
            FROM search_index s
            JOIN crawl_metadata m ON m.url = s.url
            WHERE search_index MATCH ?
            {meta_where}
        """
        total_hits = conn.execute(
            count_sql, [fts_query] + meta_params
        ).fetchone()["cnt"]

        # Log to server-side search history
        try:
            log_search_history(conn, q_clean, total_hits)
            conn.commit()
        except Exception:
            pass  # history logging must never break the search response

        response_data = ok({
            "query":        q,
            "fts_query":    fts_query,
            "total_hits":   total_hits,
            "returned":     len(results),
            "offset":       offset,
            "results":      results,
        })
        search_cache.set(cache_key, response_data)
        return response_data

    except Exception as exc:
        logger.warning("[API] Search error for %r: %s", q, exc)
        raise HTTPException(status_code=400, detail=f"Search error: {exc}")


# ---------------------------------------------------------------------------
# ── Media Search Endpoints ─────────────────────────────────────────────────
# ---------------------------------------------------------------------------

@app.get("/api/search/images", tags=["Search"])
async def search_images(
    q:      str = Query(..., min_length=1, max_length=500),
    limit:  int = Query(24, ge=1, le=100),
    offset: int = Query(0,  ge=0),
    domain: str = Query("", description="Filter by source domain"),
    safe_search: bool = Query(True, description="Filter adult/NSFW content"),
    start_date: Optional[int] = Query(None, description="Start UNIX timestamp"),
    end_date: Optional[int] = Query(None, description="End UNIX timestamp"),
):
    """
    Searches image metadata using FTS5 over title, description, and
    future LLM-generated tags/descriptions.
    Returns image URL, thumbnail, dimensions, and source page.
    """
    q_clean = q.strip()
    if not q_clean:
        raise HTTPException(status_code=400, detail="Query cannot be empty.")

    cache_key = ("images", q_clean, limit, offset, domain.strip(), safe_search, start_date, end_date)
    cached_res = search_cache.get(cache_key)
    if cached_res is not None:
        return cached_res

    terms     = q_clean.split()
    fts_query = " AND ".join(f'"{re.sub(chr(34), "", t)}"' for t in terms)

    if safe_search:
        exclude_terms = ["porn", "xxx", "sex", "naked", "nude", "adult", "pornography", "erotic", "nsfw", "milf", "ebony"]
        fts_query = f"({fts_query}) " + " ".join(f'NOT "{w}"' for w in exclude_terms)

    conn      = get_db()

    extra_filters = []
    params        = [fts_query]

    if domain.strip():
        extra_filters.append("m.domain = ?")
        params.append(domain.strip().lstrip("www."))
    if safe_search:
        extra_filters.append("m.media_url NOT LIKE '%porn%' AND m.media_url NOT LIKE '%sex%' AND m.media_url NOT LIKE '%xxx%'")
    if start_date is not None:
        extra_filters.append("m.indexed_at >= ?")
        params.append(start_date)
    if end_date is not None:
        extra_filters.append("m.indexed_at <= ?")
        params.append(end_date)

    extra_where = ("AND " + " AND ".join(extra_filters)) if extra_filters else ""

    try:
        select_params = [q_clean, q_clean] + params + [limit, offset]
        rows = conn.execute(
            f"""
            SELECT
                m.media_url, m.page_url, m.title, m.description,
                m.domain, m.width, m.height, m.format,
                m.thumbnail_url, m.is_private, m.indexed_at,
                m.llm_description, m.llm_tags,
                (bm25(media_fts, 10.0, 2.0, 2.0, 5.0) - (
                    COALESCE((SELECT vote * 2.0 FROM result_feedback WHERE url = m.media_url AND query = ? AND feedback_type = 'relevance_vote'), 0) +
                    COALESCE((SELECT SUM(vote) * 0.5 FROM result_feedback WHERE url = m.media_url AND feedback_type = 'relevance_vote'), 0) +
                    COALESCE((SELECT COUNT(*) * 0.2 FROM result_clicks WHERE url = m.media_url AND query = ?), 0)
                )) AS score,
                (SELECT COUNT(*) FROM result_feedback WHERE url = m.media_url AND feedback_type = 'relevance_vote' AND vote = 1) AS upvotes,
                (SELECT COUNT(*) FROM result_feedback WHERE url = m.media_url AND feedback_type = 'relevance_vote' AND vote = -1) AS downvotes
            FROM media_fts f
            JOIN media_index m ON m.media_url = f.media_url
            WHERE media_fts MATCH ? AND m.media_type = 'image'
            {extra_where}
            ORDER BY score ASC
            LIMIT ? OFFSET ?
            """,
            select_params,
        ).fetchall()

        total = conn.execute(
            f"""SELECT COUNT(*) AS cnt FROM media_fts f
                JOIN media_index m ON m.media_url = f.media_url
                WHERE media_fts MATCH ? AND m.media_type = 'image' {extra_where}""",
            [fts_query] + params[1:],
        ).fetchone()["cnt"]

        results = [
            {
                "media_url":      row["media_url"],
                "page_url":       row["page_url"],
                "title":          row["title"],
                "description":    row["description"],
                "domain":         row["domain"],
                "width":          row["width"],
                "height":         row["height"],
                "format":         row["format"],
                "thumbnail_url":  row["thumbnail_url"],
                "is_private":     bool(row["is_private"]),
                "indexed_at":     row["indexed_at"],
                "llm_description": row["llm_description"],
                "llm_tags":       row["llm_tags"],
                "score":          round(-row["score"], 4) if row["score"] is not None else 0.0,
                "upvotes":        row["upvotes"],
                "downvotes":      row["downvotes"],
            }
            for row in rows
        ]
        response_data = ok({"query": q, "total_hits": total, "returned": len(results),
                           "offset": offset, "results": results})
        search_cache.set(cache_key, response_data)
        return response_data

    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Image search error: {exc}")


@app.get("/api/search/videos", tags=["Search"])
async def search_videos(
    q:      str  = Query(..., min_length=1, max_length=500),
    limit:  int  = Query(12, ge=1, le=100),
    offset: int  = Query(0,  ge=0),
    domain: str  = Query("", description="Filter by source domain"),
    min_duration: float = Query(0.0, ge=0.0, description="Minimum duration in seconds"),
    max_duration: float = Query(0.0, ge=0.0, description="Maximum duration in seconds (0=any)"),
    safe_search: bool = Query(True, description="Filter adult/NSFW content"),
    start_date: Optional[int] = Query(None, description="Start UNIX timestamp"),
    end_date: Optional[int] = Query(None, description="End UNIX timestamp"),
):
    """
    Searches video metadata using FTS5 over title, description, and
    future LLM-generated tags.
    Returns video URL, thumbnail, duration, and source page.
    """
    q_clean = q.strip()
    if not q_clean:
        raise HTTPException(status_code=400, detail="Query cannot be empty.")

    cache_key = ("videos", q_clean, limit, offset, domain.strip(), min_duration, max_duration, safe_search, start_date, end_date)
    cached_res = search_cache.get(cache_key)
    if cached_res is not None:
        return cached_res

    terms     = q_clean.split()
    fts_query = " AND ".join(f'"{re.sub(chr(34), "", t)}"' for t in terms)

    if safe_search:
        exclude_terms = ["porn", "xxx", "sex", "naked", "nude", "adult", "pornography", "erotic", "nsfw", "milf", "ebony"]
        fts_query = f"({fts_query}) " + " ".join(f'NOT "{w}"' for w in exclude_terms)

    conn      = get_db()

    extra_filters = []
    extra_params  = []
    if domain.strip():
        extra_filters.append("m.domain = ?")
        extra_params.append(domain.strip().lstrip("www."))
    if min_duration > 0:
        extra_filters.append("m.duration_seconds >= ?")
        extra_params.append(min_duration)
    if max_duration > 0:
        extra_filters.append("m.duration_seconds <= ?")
        extra_params.append(max_duration)
    if safe_search:
        extra_filters.append("m.media_url NOT LIKE '%porn%' AND m.media_url NOT LIKE '%sex%' AND m.media_url NOT LIKE '%xxx%'")
    if start_date is not None:
        extra_filters.append("m.indexed_at >= ?")
        extra_params.append(start_date)
    if end_date is not None:
        extra_filters.append("m.indexed_at <= ?")
        extra_params.append(end_date)

    extra_where = ("AND " + " AND ".join(extra_filters)) if extra_filters else ""

    try:
        select_params = [q_clean, q_clean] + [fts_query] + extra_params + [limit, offset]
        rows = conn.execute(
            f"""
            SELECT
                m.media_url, m.page_url, m.title, m.description,
                m.domain, m.duration_seconds, m.format,
                m.thumbnail_url, m.is_private, m.indexed_at,
                m.llm_description, m.llm_tags,
                (bm25(media_fts, 10.0, 2.0, 2.0, 5.0) - (
                    COALESCE((SELECT vote * 2.0 FROM result_feedback WHERE url = m.media_url AND query = ? AND feedback_type = 'relevance_vote'), 0) +
                    COALESCE((SELECT SUM(vote) * 0.5 FROM result_feedback WHERE url = m.media_url AND feedback_type = 'relevance_vote'), 0) +
                    COALESCE((SELECT COUNT(*) * 0.2 FROM result_clicks WHERE url = m.media_url AND query = ?), 0)
                )) AS score,
                (SELECT COUNT(*) FROM result_feedback WHERE url = m.media_url AND feedback_type = 'relevance_vote' AND vote = 1) AS upvotes,
                (SELECT COUNT(*) FROM result_feedback WHERE url = m.media_url AND feedback_type = 'relevance_vote' AND vote = -1) AS downvotes
            FROM media_fts f
            JOIN media_index m ON m.media_url = f.media_url
            WHERE media_fts MATCH ? AND m.media_type = 'video'
            {extra_where}
            ORDER BY score ASC
            LIMIT ? OFFSET ?
            """,
            select_params,
        ).fetchall()

        count_params = [fts_query] + extra_params
        total = conn.execute(
            f"""SELECT COUNT(*) AS cnt FROM media_fts f
                JOIN media_index m ON m.media_url = f.media_url
                WHERE media_fts MATCH ? AND m.media_type = 'video' {extra_where}""",
            count_params,
        ).fetchone()["cnt"]

        results = [
            {
                "media_url":       row["media_url"],
                "page_url":        row["page_url"],
                "title":           row["title"],
                "description":     row["description"],
                "domain":          row["domain"],
                "duration_seconds": row["duration_seconds"],
                "format":          row["format"],
                "thumbnail_url":   row["thumbnail_url"],
                "is_private":      bool(row["is_private"]),
                "indexed_at":      row["indexed_at"],
                "llm_description": row["llm_description"],
                "llm_tags":        row["llm_tags"],
                "score":          round(-row["score"], 4) if row["score"] is not None else 0.0,
                "upvotes":        row["upvotes"],
                "downvotes":      row["downvotes"],
            }
            for row in rows
        ]
        response_data = ok({"query": q, "total_hits": total, "returned": len(results),
                           "offset": offset, "results": results})
        search_cache.set(cache_key, response_data)
        return response_data

    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Video search error: {exc}")


# ---------------------------------------------------------------------------
# ── Search History ─────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

@app.get("/api/history", tags=["Search"])
async def get_search_history(
    limit: int = Query(20, ge=1, le=100),
):
    """
    Returns the most recent server-side search history entries.
    Ordered by most-recent first.
    """
    conn = get_db()
    rows = conn.execute(
        """
        SELECT query, result_count, searched_at,
               COUNT(*) OVER (PARTITION BY query) AS frequency
        FROM search_history
        ORDER BY searched_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return ok([
        {
            "query":        row["query"],
            "result_count": row["result_count"],
            "searched_at":  row["searched_at"],
            "frequency":    row["frequency"],
        }
        for row in rows
    ])


@app.delete("/api/history", tags=["Search"])
async def clear_search_history():
    """Clears all server-side search history."""
    conn = get_db()
    conn.execute("DELETE FROM search_history")
    conn.commit()
    return ok({"cleared": True})


# ---------------------------------------------------------------------------
# ── Database Stats ─────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

@app.get("/api/stats", tags=["System"])
async def stats():
    """Returns aggregate statistics about the index."""
    conn = get_db()
    total_docs = conn.execute(
        "SELECT COUNT(*) AS cnt FROM crawl_metadata"
    ).fetchone()["cnt"]

    private_docs = conn.execute(
        "SELECT COUNT(*) AS cnt FROM crawl_metadata WHERE is_private = 1"
    ).fetchone()["cnt"]

    public_docs = total_docs - private_docs

    top_domains = conn.execute(
        """
        SELECT domain, COUNT(*) AS cnt
        FROM crawl_metadata
        GROUP BY domain
        ORDER BY cnt DESC
        LIMIT 10
        """
    ).fetchall()

    total_images = conn.execute(
        "SELECT COUNT(*) AS cnt FROM media_index WHERE media_type = 'image'"
    ).fetchone()["cnt"]

    total_videos = conn.execute(
        "SELECT COUNT(*) AS cnt FROM media_index WHERE media_type = 'video'"
    ).fetchone()["cnt"]

    return ok({
        "total_indexed":   total_docs,
        "private_indexed": private_docs,
        "public_indexed":  public_docs,
        "images_indexed":  total_images,
        "videos_indexed":  total_videos,
        "top_domains":     [{"domain": r["domain"], "count": r["cnt"]} for r in top_domains],
    })


@app.get("/api/domains", tags=["Domains"])
async def get_domains():
    """
    Returns all known domains, their page counts, and their crawl configuration settings.
    """
    conn = get_db()
    domains = get_all_domains(conn)
    return ok(domains)


@app.post("/api/domains/{domain}", tags=["Domains"])
async def configure_domain(domain: str, payload: DomainConfigPayload):
    """
    Configures public/private status and crawler settings for a domain.
    """
    conn = get_db()
    dom = domain.strip().lower().lstrip("www.")
    if not dom:
        raise HTTPException(status_code=400, detail="Invalid domain")
    
    upsert_managed_domain(
        conn=conn,
        domain=dom,
        is_public=payload.is_public,
        crawl_enabled=payload.crawl_enabled,
        priority=payload.priority,
        sitemap_url=payload.sitemap_url,
        notes=payload.notes,
    )
    conn.commit()
    return ok({"domain": dom, "is_public": payload.is_public, "crawl_enabled": payload.crawl_enabled})


@app.get("/api/search/products", tags=["Search"])
async def search_products(
    q:                str  = Query(..., min_length=1, max_length=500),
    limit:            int  = Query(20, ge=1, le=100),
    offset:           int  = Query(0, ge=0),
    sort:             str  = Query("relevance", description="relevance | price_asc | price_desc"),
    prioritize_stock: bool = Query(True, description="Prioritize ready stock (in_stock first)"),
    domain:           str  = Query("", description="Filter by source domain"),
    safe_search:      bool = Query(True, description="Filter adult/NSFW content"),
    start_date: Optional[int] = Query(None, description="Start UNIX timestamp"),
    end_date: Optional[int] = Query(None, description="End UNIX timestamp"),
):
    """
    Searches products index using FTS5 over name, description, brand.
    Allows sorting by price or relevance, and prioritizing stock availability.
    """
    q_clean = q.strip()
    if not q_clean:
        raise HTTPException(status_code=400, detail="Query cannot be empty.")

    cache_key = ("products", q_clean, limit, offset, sort, prioritize_stock, domain.strip(), safe_search, start_date, end_date)
    cached_res = search_cache.get(cache_key)
    if cached_res is not None:
        return cached_res

    terms     = q_clean.split()
    fts_query = " AND ".join(f'"{re.sub(chr(34), "", t)}"' for t in terms)

    if safe_search:
        exclude_terms = ["porn", "xxx", "sex", "naked", "nude", "adult", "pornography", "erotic", "nsfw", "milf", "ebony"]
        fts_query = f"({fts_query}) " + " ".join(f'NOT "{w}"' for w in exclude_terms)

    conn      = get_db()

    extra_filters = []
    extra_params  = [fts_query]

    if domain.strip():
        extra_filters.append("p.domain = ?")
        extra_params.append(domain.strip().lstrip("www."))
    if safe_search:
        extra_filters.append("p.url NOT LIKE '%porn%' AND p.url NOT LIKE '%sex%' AND p.url NOT LIKE '%xxx%'")
    if start_date is not None:
        extra_filters.append("p.extracted_at >= ?")
        extra_params.append(start_date)
    if end_date is not None:
        extra_filters.append("p.extracted_at <= ?")
        extra_params.append(end_date)

    extra_where = ("AND " + " AND ".join(extra_filters)) if extra_filters else ""

    # Sort criteria
    sort_clauses = []
    
    if prioritize_stock:
        sort_clauses.append("""
            CASE p.availability
                WHEN 'in_stock' THEN 0
                WHEN 'preorder' THEN 1
                WHEN 'out_of_stock' THEN 2
                WHEN 'discontinued' THEN 3
                ELSE 4
            END ASC
        """)
    
    if sort == "price_asc":
        sort_clauses.append("CASE WHEN p.price IS NULL THEN 1 ELSE 0 END ASC")
        sort_clauses.append("p.price ASC")
    elif sort == "price_desc":
        sort_clauses.append("CASE WHEN p.price IS NULL THEN 1 ELSE 0 END ASC")
        sort_clauses.append("p.price DESC")
    else:  # relevance
        sort_clauses.append("score ASC")

    order_by_clause = "ORDER BY " + ", ".join(sort_clauses)

    try:
        select_params = [q_clean, q_clean] + extra_params + [limit, offset]
        rows = conn.execute(
            f"""
            SELECT
                p.url, p.name, p.description, p.brand, p.sku, p.price,
                p.price_text, p.currency, p.availability, p.image_url,
                p.domain, p.is_private, p.schema_type, p.extracted_at,
                (bm25(product_fts, 10.0, 2.0, 1.0) - (
                    COALESCE((SELECT vote * 2.0 FROM result_feedback WHERE url = p.url AND query = ? AND feedback_type = 'relevance_vote'), 0) +
                    COALESCE((SELECT SUM(vote) * 0.5 FROM result_feedback WHERE url = p.url AND feedback_type = 'relevance_vote'), 0) +
                    COALESCE((SELECT SUM(vote) * 5.0 FROM result_feedback WHERE url = p.url AND feedback_type = 'product_vote'), 0) +
                    COALESCE((SELECT COUNT(*) * 0.2 FROM result_clicks WHERE url = p.url AND query = ?), 0)
                )) AS score,
                (SELECT COUNT(*) FROM result_feedback WHERE url = p.url AND feedback_type = 'relevance_vote' AND vote = 1) AS upvotes,
                (SELECT COUNT(*) FROM result_feedback WHERE url = p.url AND feedback_type = 'relevance_vote' AND vote = -1) AS downvotes,
                (SELECT COUNT(*) FROM result_feedback WHERE url = p.url AND feedback_type = 'product_vote' AND vote = 1) AS product_plus,
                (SELECT COUNT(*) FROM result_feedback WHERE url = p.url AND feedback_type = 'product_vote' AND vote = -1) AS product_minus
            FROM product_fts f
            JOIN product_index p ON p.url = f.url
            WHERE product_fts MATCH ?
            {extra_where}
            {order_by_clause}
            LIMIT ? OFFSET ?
            """,
            select_params,
        ).fetchall()

        total = conn.execute(
            f"""
            SELECT COUNT(*) AS cnt
            FROM product_fts f
            JOIN product_index p ON p.url = f.url
            WHERE product_fts MATCH ?
            {extra_where}
            """,
            [fts_query] + extra_params[1:],
        ).fetchone()["cnt"]

        results = [
            {
                "url":          row["url"],
                "name":         row["name"],
                "description":  row["description"],
                "brand":        row["brand"],
                "sku":          row["sku"],
                "price":        row["price"],
                "price_text":   row["price_text"],
                "currency":     row["currency"],
                "availability": row["availability"],
                "image_url":    row["image_url"],
                "domain":       row["domain"],
                "is_private":   bool(row["is_private"]),
                "schema_type":  row["schema_type"],
                "extracted_at": row["extracted_at"],
                "score":        round(-row["score"], 4),
                "upvotes":      row["upvotes"],
                "downvotes":    row["downvotes"],
                "product_plus":  row["product_plus"],
                "product_minus": row["product_minus"],
            }
            for row in rows
        ]

        response_data = ok({
            "query":        q,
            "total_hits":   total,
            "returned":     len(results),
            "offset":       offset,
            "results":      results,
        })
        search_cache.set(cache_key, response_data)
        return response_data

    except Exception as exc:
        logger.warning("[API] Product search error for %r: %s", q, exc)
        raise HTTPException(status_code=400, detail=f"Product search error: {exc}")


@app.post("/api/feedback", tags=["Search"])
async def submit_search_feedback(payload: FeedbackPayload):
    """
    Submits user feedback (upvote/downvote/product identification) for a result.
    Applies bias adjustments to subsequent search relevance calculations.
    """
    if payload.feedback_type not in ("relevance_vote", "product_vote"):
        raise HTTPException(status_code=400, detail="Invalid feedback type")
    if payload.vote not in (1, -1):
        raise HTTPException(status_code=400, detail="Vote must be 1 or -1")
    
    conn = get_db()
    try:
        submit_feedback(
            conn=conn,
            url=payload.url,
            query=payload.query.strip(),
            feedback_type=payload.feedback_type,
            vote=payload.vote,
        )
        conn.commit()
        return ok({"submitted": True})
    except Exception as exc:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(exc))


class ClickPayload(BaseModel):
    """Payload to register a click on a search result URL."""
    url:   str = Field(..., description="Clicked result URL")
    query: str = Field(..., description="The query used for the search")


@app.post("/api/click", tags=["Search"])
async def register_click(payload: ClickPayload):
    """
    Registers a click on a search result URL, tracking query-specific popularity.
    Clears cache for the query.
    """
    conn = get_db()
    try:
        now = int(time.time())
        conn.execute(
            "INSERT INTO result_clicks (url, query, clicked_at) VALUES (?, ?, ?)",
            (payload.url, payload.query.strip(), now)
        )
        conn.commit()
        # Invalidate dynamic cache
        search_cache.clear()
        return ok({"clicked": True})
    except Exception as exc:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/trends", tags=["System"])
async def get_trends(
    days: int = Query(7, ge=1, le=365, description="Timeframe in days"),
    limit: int = Query(10, ge=1, le=100, description="Max results to return"),
):
    """
    Returns trending search queries and clicked URLs for a given timeframe.
    """
    conn = get_db()
    now = int(time.time())
    start_time = now - days * 86400
    
    # 1. Top clicked queries
    queries = conn.execute(
        """
        SELECT query, COUNT(*) AS click_count
        FROM result_clicks
        WHERE clicked_at >= ?
        GROUP BY query
        ORDER BY click_count DESC, query ASC
        LIMIT ?
        """,
        (start_time, limit)
    ).fetchall()
    
    # 2. Top clicked URLs with titles
    urls = conn.execute(
        """
        SELECT c.url, COUNT(*) AS click_count,
               COALESCE(
                   (SELECT title FROM search_index WHERE url = c.url),
                   (SELECT name FROM product_index WHERE url = c.url),
                   (SELECT title FROM media_index WHERE media_url = c.url),
                   c.url
               ) AS title
        FROM result_clicks c
        WHERE c.clicked_at >= ?
        GROUP BY c.url
        ORDER BY click_count DESC, c.url ASC
        LIMIT ?
        """,
        (start_time, limit)
    ).fetchall()
    
    return ok({
        "timeframe_days": days,
        "queries": [{"query": r["query"], "clicks": r["click_count"]} for r in queries],
        "urls": [{"url": r["url"], "title": r["title"], "clicks": r["click_count"]} for r in urls]
    })


# ---------------------------------------------------------------------------
# ── Static file serving — dashboard ───────────────────────────────────────
# ---------------------------------------------------------------------------

@app.get("/", include_in_schema=False)
async def dashboard():
    """Serves the main search/control dashboard HTML file."""
    import os
    html_path = os.path.join(os.path.dirname(__file__), "index.html")
    if os.path.exists(html_path):
        return FileResponse(html_path, media_type="text/html")
    return JSONResponse(
        {"error": "Dashboard not found. Place index.html in the same directory."},
        status_code=404,
    )


# ---------------------------------------------------------------------------
# ── Health check ──────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

@app.get("/health", tags=["System"])
async def health():
    """Lightweight health-check endpoint."""
    return {"status": "healthy", "service": "shomaj-search", "ts": int(time.time())}
