"""
database.py — Shomaj Search
SQLite connection factory with WAL mode, FTS5 schema, and
optimised PRAGMA settings for low-RAM, high-throughput indexing.
"""

import sqlite3
import threading
import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_PATH = Path(os.environ.get("SHOMAJ_DB", "shomaj_search.db"))

# One connection per thread (sqlite3 is not thread-safe by default)
_local = threading.local()


# ---------------------------------------------------------------------------
# Connection factory
# ---------------------------------------------------------------------------
def get_db() -> sqlite3.Connection:
    """
    Returns a thread-local sqlite3 connection.
    Creates and configures one on first access per thread.
    Row factory is set to sqlite3.Row for dict-like access.
    """
    if not hasattr(_local, "conn") or _local.conn is None:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        conn.row_factory = sqlite3.Row

        # ------------------------------------------------------------------
        # WAL mode: concurrent readers never block the writer.
        # synchronous=NORMAL is safe with WAL (fsync on checkpoints only).
        # cache_size=-64000 = up to 64 MB page cache (released when idle).
        # ------------------------------------------------------------------
        conn.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            PRAGMA cache_size=-64000;
            PRAGMA temp_store=MEMORY;
            PRAGMA mmap_size=268435456;
        """)

        _local.conn = conn

    return _local.conn


# ---------------------------------------------------------------------------
# Schema initialisation
# ---------------------------------------------------------------------------
_SCHEMA_SQL = """
-- -----------------------------------------------------------------------
-- FTS5 Virtual Table — primary search index (web pages)
-- Porter stemmer + unicode61 tokenizer for broad language coverage.
-- url is UNINDEXED because we never full-text-search on URLs themselves.
-- -----------------------------------------------------------------------
CREATE VIRTUAL TABLE IF NOT EXISTS search_index USING fts5(
    url        UNINDEXED,
    title,
    clean_content,
    tokenize = 'porter unicode61'
);

-- -----------------------------------------------------------------------
-- Crawl metadata — tracks every URL that has ever been indexed.
-- is_private = 1  → came via browser extension (never re-crawled actively)
-- is_private = 0  → came from active open-web crawler
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS crawl_metadata (
    url          TEXT    PRIMARY KEY,
    domain       TEXT    NOT NULL,
    last_scanned INTEGER NOT NULL,
    is_private   INTEGER NOT NULL DEFAULT 0
);

-- -----------------------------------------------------------------------
-- Crawler queue — persistent work queue for the active crawler.
-- status: 'pending' | 'completed' | 'failed'
-- depth: hop count from the seed URL (0 = seed)
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS queue (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    url    TEXT    UNIQUE NOT NULL,
    depth  INTEGER NOT NULL DEFAULT 0,
    status TEXT    NOT NULL DEFAULT 'pending'
);

-- Index for fast pending-queue pulls
CREATE INDEX IF NOT EXISTS idx_queue_status_depth
    ON queue (status, depth);

-- Index for fast metadata domain lookups
CREATE INDEX IF NOT EXISTS idx_metadata_domain
    ON crawl_metadata (domain);

-- -----------------------------------------------------------------------
-- Media Index — images and videos discovered during crawling.
-- NO media files are stored locally. Only metadata + URLs are stored.
-- media_type: 'image' | 'video'
--
-- Future fields:
--   llm_description — LLM-generated description of the media content
--   llm_tags        — comma-separated LLM-generated tags
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS media_index (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    media_url         TEXT    UNIQUE NOT NULL,
    page_url          TEXT    NOT NULL,
    media_type        TEXT    NOT NULL CHECK(media_type IN ('image','video')),
    title             TEXT    NOT NULL DEFAULT '',
    description       TEXT    NOT NULL DEFAULT '',
    domain            TEXT    NOT NULL DEFAULT '',
    width             INTEGER NOT NULL DEFAULT 0,
    height            INTEGER NOT NULL DEFAULT 0,
    duration_seconds  REAL    NOT NULL DEFAULT 0,
    thumbnail_url     TEXT    NOT NULL DEFAULT '',
    format            TEXT    NOT NULL DEFAULT '',
    file_size_bytes   INTEGER NOT NULL DEFAULT 0,
    is_private        INTEGER NOT NULL DEFAULT 0,
    llm_description   TEXT    NOT NULL DEFAULT '',
    llm_tags          TEXT    NOT NULL DEFAULT '',
    indexed_at        INTEGER NOT NULL
);

-- Index for media type + domain lookups
CREATE INDEX IF NOT EXISTS idx_media_type
    ON media_index (media_type, domain);

CREATE INDEX IF NOT EXISTS idx_media_page
    ON media_index (page_url);

-- -----------------------------------------------------------------------
-- Media FTS — full-text search over image/video metadata
-- -----------------------------------------------------------------------
CREATE VIRTUAL TABLE IF NOT EXISTS media_fts USING fts5(
    media_url    UNINDEXED,
    page_url     UNINDEXED,
    media_type   UNINDEXED,
    title,
    description,
    llm_description,
    llm_tags,
    tokenize = 'porter unicode61'
);

-- -----------------------------------------------------------------------
-- Search History — server-side log of all search queries
-- (client also maintains localStorage history, this is the persistent copy)
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS search_history (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    query      TEXT    NOT NULL,
    result_count INTEGER NOT NULL DEFAULT 0,
    searched_at  INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_history_time
    ON search_history (searched_at DESC);
"""


def init_db() -> None:
    """
    Creates all tables and indexes.
    Safe to call multiple times (all statements are CREATE IF NOT EXISTS).
    """
    conn = get_db()
    conn.executescript(_SCHEMA_SQL)
    conn.commit()
    print(f"[DB] Schema initialised → {DB_PATH.resolve()}")


# ---------------------------------------------------------------------------
# Helpers — Web pages
# ---------------------------------------------------------------------------
def upsert_index(
    conn: sqlite3.Connection,
    url: str,
    title: str,
    clean_content: str,
    domain: str,
    is_private: int,
) -> None:
    """
    Inserts or replaces a document in search_index + crawl_metadata atomically.
    FTS5 does not support ON CONFLICT, so we DELETE then INSERT.
    """
    now = int(__import__("time").time())

    # Remove existing FTS5 row (if any) — avoids duplicate results
    conn.execute("DELETE FROM search_index WHERE url = ?", (url,))

    # Insert fresh FTS5 document
    conn.execute(
        "INSERT INTO search_index (url, title, clean_content) VALUES (?, ?, ?)",
        (url, title, clean_content),
    )

    # Upsert metadata
    conn.execute(
        """
        INSERT INTO crawl_metadata (url, domain, last_scanned, is_private)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(url) DO UPDATE SET
            last_scanned = excluded.last_scanned,
            is_private   = excluded.is_private
        """,
        (url, domain, now, is_private),
    )


# ---------------------------------------------------------------------------
# Helpers — Media
# ---------------------------------------------------------------------------
def upsert_media(
    conn: sqlite3.Connection,
    media_url: str,
    page_url: str,
    media_type: str,
    title: str = "",
    description: str = "",
    domain: str = "",
    width: int = 0,
    height: int = 0,
    duration_seconds: float = 0.0,
    thumbnail_url: str = "",
    fmt: str = "",
    file_size_bytes: int = 0,
    is_private: int = 0,
) -> None:
    """
    Inserts or updates a media record in media_index and media_fts.
    """
    import time as _time
    now = int(_time.time())

    conn.execute(
        """
        INSERT INTO media_index
            (media_url, page_url, media_type, title, description, domain,
             width, height, duration_seconds, thumbnail_url, format,
             file_size_bytes, is_private, indexed_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(media_url) DO UPDATE SET
            title             = excluded.title,
            description       = excluded.description,
            width             = excluded.width,
            height            = excluded.height,
            duration_seconds  = excluded.duration_seconds,
            thumbnail_url     = excluded.thumbnail_url,
            format            = excluded.format,
            file_size_bytes   = excluded.file_size_bytes,
            indexed_at        = excluded.indexed_at
        """,
        (
            media_url, page_url, media_type, title, description, domain,
            width, height, duration_seconds, thumbnail_url, fmt,
            file_size_bytes, is_private, now,
        ),
    )

    # Sync FTS index — delete old row then re-insert
    conn.execute("DELETE FROM media_fts WHERE media_url = ?", (media_url,))
    conn.execute(
        """
        INSERT INTO media_fts
            (media_url, page_url, media_type, title, description, llm_description, llm_tags)
        VALUES (?, ?, ?, ?, ?, '', '')
        """,
        (media_url, page_url, media_type, title, description),
    )


def log_search_history(conn: sqlite3.Connection, query: str, result_count: int) -> None:
    """Records a search query to the server-side history table."""
    import time as _time
    conn.execute(
        "INSERT INTO search_history (query, result_count, searched_at) VALUES (?, ?, ?)",
        (query, result_count, int(_time.time())),
    )


# ---------------------------------------------------------------------------
# Helpers — Queue
# ---------------------------------------------------------------------------
def enqueue_url(conn: sqlite3.Connection, url: str, depth: int) -> bool:
    """
    Adds a URL to the crawler queue.
    Returns True if newly inserted, False if already present.
    Uses INSERT OR IGNORE so duplicates are silently dropped.
    """
    cursor = conn.execute(
        "INSERT OR IGNORE INTO queue (url, depth, status) VALUES (?, ?, 'pending')",
        (url, depth),
    )
    return cursor.rowcount == 1


def pop_pending_url(conn: sqlite3.Connection) -> dict | None:
    """
    Atomically fetches the next pending URL from the queue (shallowest first),
    marks it as 'in-progress' by setting status='completed' optimistically
    (rolled back on failure via the crawler).
    Returns a dict with 'id', 'url', 'depth' or None if queue is empty.
    """
    row = conn.execute(
        """
        SELECT id, url, depth FROM queue
        WHERE status = 'pending'
        ORDER BY depth ASC, id ASC
        LIMIT 1
        """
    ).fetchone()

    if row is None:
        return None

    conn.execute(
        "UPDATE queue SET status = 'completed' WHERE id = ?", (row["id"],)
    )

    return {"id": row["id"], "url": row["url"], "depth": row["depth"]}


def mark_url_failed(conn: sqlite3.Connection, queue_id: int) -> None:
    """Marks a queue entry as failed so it is not retried by default."""
    conn.execute("UPDATE queue SET status = 'failed' WHERE id = ?", (queue_id,))


def get_queue_stats(conn: sqlite3.Connection) -> dict:
    """Returns counts by status for the crawler queue."""
    rows = conn.execute(
        "SELECT status, COUNT(*) AS cnt FROM queue GROUP BY status"
    ).fetchall()
    return {row["status"]: row["cnt"] for row in rows}
