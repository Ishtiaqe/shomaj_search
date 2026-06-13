"""
crawler.py — Shomaj Search
Async active open-web crawler with a 4-state thread-safe state machine.

States: IDLE → RUNNING ↔ PAUSED → STOPPED

The crawler runs as a persistent asyncio background task.  It pulls URLs
from the SQLite queue, fetches them via httpx, extracts clean text with
BeautifulSoup, and writes results back to the database.

Domain pacing (rate limiting) is enforced per-domain.
Domain blocklist prevents crawling private/social-media domains.
"""

import asyncio
import logging
import re
import time
from enum import Enum
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from database import (
    enqueue_url,
    get_db,
    get_queue_stats,
    mark_url_failed,
    pop_pending_url,
    upsert_index,
)

logger = logging.getLogger("shomaj.crawler")

# ---------------------------------------------------------------------------
# State Machine
# ---------------------------------------------------------------------------
class CrawlerState(str, Enum):
    IDLE    = "IDLE"
    RUNNING = "RUNNING"
    PAUSED  = "PAUSED"
    STOPPED = "STOPPED"


# ---------------------------------------------------------------------------
# Domain blocklist (active crawler only — extension data bypasses this)
# ---------------------------------------------------------------------------
BLOCKED_DOMAINS: set[str] = {
    "facebook.com",
    "messenger.com",
    "whatsapp.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "drive.google.com",
    "mail.google.com",
    "docs.google.com",
    "sheets.google.com",
    "slides.google.com",
    "accounts.google.com",
    "signin.google.com",
    "login.microsoftonline.com",
    "localhost",
    "127.0.0.1",
    "::1",
    "linkedin.com",
    "tiktok.com",
    "pinterest.com",
    "snapchat.com",
    "reddit.com",
    "youtube.com",
    "netflix.com",
    "amazon.com",
    "ebay.com",
}

# Regex for "accounts.*" style wildcards
BLOCKED_PATTERNS: list[re.Pattern] = [
    re.compile(r"^accounts\.", re.IGNORECASE),
    re.compile(r"^login\.",    re.IGNORECASE),
    re.compile(r"^signin\.",   re.IGNORECASE),
    re.compile(r"^auth\.",     re.IGNORECASE),
    re.compile(r"^sso\.",      re.IGNORECASE),
]

# Allowed URL schemes
ALLOWED_SCHEMES = {"http", "https"}

# Maximum content size to download (bytes) — skip huge assets
MAX_CONTENT_BYTES = 5 * 1024 * 1024  # 5 MB

# ---------------------------------------------------------------------------
# Crawler Configuration (mutable at runtime)
# ---------------------------------------------------------------------------
class CrawlerConfig:
    def __init__(self) -> None:
        self.delay_seconds: float = 1.5   # inter-request pacing per domain
        self.max_depth: int = 3           # maximum hop depth from seeds

    def update(self, delay_seconds: Optional[float] = None, max_depth: Optional[int] = None) -> None:
        if delay_seconds is not None:
            self.delay_seconds = max(0.0, float(delay_seconds))
        if max_depth is not None:
            self.max_depth = max(0, int(max_depth))


# ---------------------------------------------------------------------------
# Singleton Crawler Engine
# ---------------------------------------------------------------------------
class CrawlerEngine:
    """
    Singleton class that owns the crawler state machine and background task.
    Access via the module-level `engine` instance.
    """

    def __init__(self) -> None:
        self.state: CrawlerState = CrawlerState.IDLE
        self.config: CrawlerConfig = CrawlerConfig()
        self._lock: asyncio.Lock = asyncio.Lock()

        # Metrics
        self._items_processed: int = 0
        self._started_at: Optional[float] = None

        # Per-domain last-fetch timestamps for pacing
        self._domain_last_fetch: dict[str, float] = {}

        # Background task handle
        self._task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # State transition helpers
    # ------------------------------------------------------------------
    async def start(self) -> str:
        """Transitions to RUNNING. Spawns the worker task if not alive."""
        async with self._lock:
            if self.state in (CrawlerState.IDLE, CrawlerState.PAUSED):
                self.state = CrawlerState.RUNNING
                self._started_at = self._started_at or time.time()
                if self._task is None or self._task.done():
                    self._task = asyncio.create_task(
                        self._worker_loop(), name="crawler-worker"
                    )
                    logger.info("[Crawler] Worker task spawned.")
                return "started"
            elif self.state == CrawlerState.RUNNING:
                return "already_running"
            else:
                return f"cannot_start_from_{self.state.value}"

    async def pause(self) -> str:
        """Suspends loop iteration without flushing the queue."""
        async with self._lock:
            if self.state == CrawlerState.RUNNING:
                self.state = CrawlerState.PAUSED
                logger.info("[Crawler] Paused.")
                return "paused"
            return f"not_running (state={self.state.value})"

    async def stop(self) -> str:
        """Halts the worker task and resets to IDLE. Flushes in-memory state."""
        async with self._lock:
            if self.state in (CrawlerState.RUNNING, CrawlerState.PAUSED):
                self.state = CrawlerState.STOPPED
                if self._task and not self._task.done():
                    self._task.cancel()
                    try:
                        await asyncio.wait_for(asyncio.shield(self._task), timeout=5.0)
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        pass
                # Reset to IDLE so it can be restarted cleanly
                self.state = CrawlerState.IDLE
                self._task = None
                self._domain_last_fetch.clear()
                self._items_processed = 0
                self._started_at = None
                logger.info("[Crawler] Stopped and reset to IDLE.")
                return "stopped"
            return f"not_active (state={self.state.value})"

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------
    def status(self) -> dict:
        conn = get_db()
        stats = get_queue_stats(conn)
        return {
            "state":           self.state.value,
            "items_processed": self._items_processed,
            "queue_pending":   stats.get("pending", 0),
            "queue_completed": stats.get("completed", 0),
            "queue_failed":    stats.get("failed", 0),
            "delay_seconds":   self.config.delay_seconds,
            "max_depth":       self.config.max_depth,
            "uptime_seconds":  round(time.time() - self._started_at, 1)
                               if self._started_at else 0,
        }

    # ------------------------------------------------------------------
    # Domain utilities
    # ------------------------------------------------------------------
    @staticmethod
    def _extract_domain(url: str) -> Optional[str]:
        try:
            parsed = urlparse(url)
            return parsed.netloc.lower().lstrip("www.")
        except Exception:
            return None

    @staticmethod
    def is_blocked(url: str) -> bool:
        """Returns True if the URL matches any blocklist rule."""
        try:
            parsed = urlparse(url)
            scheme = parsed.scheme.lower()
            if scheme not in ALLOWED_SCHEMES:
                return True

            host = parsed.netloc.lower().lstrip("www.")

            # Exact match
            if host in BLOCKED_DOMAINS:
                return True

            # Pattern match (accounts.*, login.*, etc.)
            for pattern in BLOCKED_PATTERNS:
                if pattern.match(host):
                    return True

            # Sub-domain match (e.g. mail.facebook.com)
            for blocked in BLOCKED_DOMAINS:
                if host.endswith("." + blocked):
                    return True

        except Exception:
            return True

        return False

    async def _wait_for_domain_rate(self, domain: str) -> None:
        """Sleeps until the per-domain pacing interval has elapsed."""
        last = self._domain_last_fetch.get(domain, 0.0)
        elapsed = time.time() - last
        wait = self.config.delay_seconds - elapsed
        if wait > 0:
            await asyncio.sleep(wait)
        self._domain_last_fetch[domain] = time.time()

    # ------------------------------------------------------------------
    # HTML extraction
    # ------------------------------------------------------------------
    @staticmethod
    def _extract_text(html: str) -> tuple[str, list[str]]:
        """
        Parses HTML with BeautifulSoup, strips noise tags, and returns:
          - clean text body (str)
          - list of absolute-ish href URLs found in <a> tags with text

        Script, style, nav, header, footer, aside, and noscript tags are
        removed before text extraction to minimise noise.
        """
        soup = BeautifulSoup(html, "html.parser")

        # Remove noisy structural/scripting elements
        for tag in soup.find_all(
            ["script", "style", "nav", "header", "footer", "aside", "noscript", "iframe"]
        ):
            tag.decompose()

        # Extract clean text
        text = soup.get_text(separator=" ", strip=True)
        # Collapse runs of whitespace
        text = re.sub(r"\s{2,}", " ", text).strip()
        # Truncate to 200 KB to keep DB rows lean
        text = text[:200_000]

        # Extract links that have non-empty anchor text
        links: list[str] = []
        for a in soup.find_all("a", href=True):
            href = a.get("href", "").strip()
            anchor_text = a.get_text(strip=True)
            if href and anchor_text:
                links.append(href)

        return text, links

    # ------------------------------------------------------------------
    # Worker Loop
    # ------------------------------------------------------------------
    async def _worker_loop(self) -> None:
        """
        Infinite async loop:
        1. If PAUSED → sleep 1 s.
        2. If STOPPED / IDLE → exit.
        3. Pop next pending URL from DB queue.
        4. Fetch + extract + index.
        5. Enqueue child links within depth limit.
        6. Respect domain pacing.
        """
        logger.info("[Crawler] Worker loop started.")

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(15.0, connect=5.0),
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "ShomajBot/1.0 (search.shomaj.one; "
                    "educational crawler; +https://shomaj.one)"
                )
            },
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
        ) as client:

            consecutive_empty = 0  # tracks how many loops found empty queue

            while True:
                # ---- State checks ----------------------------------------
                if self.state == CrawlerState.PAUSED:
                    await asyncio.sleep(1.0)
                    continue

                if self.state in (CrawlerState.STOPPED, CrawlerState.IDLE):
                    logger.info("[Crawler] Worker loop exiting (state=%s).", self.state)
                    break

                # ---- Pull next URL from DB queue --------------------------
                conn = get_db()
                item = pop_pending_url(conn)

                if item is None:
                    consecutive_empty += 1
                    if consecutive_empty >= 3:
                        logger.debug("[Crawler] Queue empty — sleeping 10 s.")
                        await asyncio.sleep(10.0)
                        consecutive_empty = 0
                    else:
                        await asyncio.sleep(2.0)
                    conn.commit()
                    continue

                consecutive_empty = 0
                url: str   = item["url"]
                depth: int = item["depth"]

                # ---- Blocklist guard -------------------------------------
                if self.is_blocked(url):
                    logger.debug("[Crawler] Blocked → %s", url)
                    conn.commit()
                    continue

                # ---- Depth guard -----------------------------------------
                if depth > self.config.max_depth:
                    logger.debug("[Crawler] Max depth exceeded → %s", url)
                    conn.commit()
                    continue

                # ---- Domain pacing ---------------------------------------
                domain = self._extract_domain(url) or "unknown"
                await self._wait_for_domain_rate(domain)

                # ---- Fetch -----------------------------------------------
                try:
                    logger.info("[Crawler] Fetching (%d) → %s", depth, url)
                    response = await client.get(url)
                    response.raise_for_status()

                    content_type = response.headers.get("content-type", "")
                    if "text/html" not in content_type:
                        logger.debug("[Crawler] Skipping non-HTML → %s", url)
                        conn.commit()
                        continue

                    if len(response.content) > MAX_CONTENT_BYTES:
                        logger.debug("[Crawler] Content too large → %s", url)
                        conn.commit()
                        continue

                    html = response.text
                    title_match = re.search(
                        r"<title[^>]*>(.*?)</title>", html,
                        re.IGNORECASE | re.DOTALL
                    )
                    title = title_match.group(1).strip() if title_match else url
                    title = re.sub(r"\s+", " ", title)[:512]

                    clean_text, links = self._extract_text(html)

                    # ---- Persist ------------------------------------------
                    upsert_index(
                        conn=conn,
                        url=url,
                        title=title,
                        clean_content=clean_text,
                        domain=domain,
                        is_private=0,
                    )

                    # ---- Enqueue child links ------------------------------
                    if depth < self.config.max_depth:
                        queued = 0
                        for href in links:
                            child_url = urljoin(url, href).split("#")[0]
                            # Validate and blocklist check
                            parsed_child = urlparse(child_url)
                            if parsed_child.scheme not in ALLOWED_SCHEMES:
                                continue
                            if self.is_blocked(child_url):
                                continue
                            if enqueue_url(conn, child_url, depth + 1):
                                queued += 1
                        logger.debug(
                            "[Crawler] Enqueued %d child links from %s", queued, url
                        )

                    conn.commit()
                    self._items_processed += 1
                    logger.info(
                        "[Crawler] ✓ Indexed → %s (total=%d)", url, self._items_processed
                    )

                except asyncio.CancelledError:
                    # Task was cancelled — mark URL as failed and exit
                    mark_url_failed(conn, item["id"])
                    conn.commit()
                    logger.info("[Crawler] Task cancelled mid-fetch.")
                    raise

                except httpx.HTTPStatusError as exc:
                    logger.warning(
                        "[Crawler] HTTP %d → %s", exc.response.status_code, url
                    )
                    mark_url_failed(conn, item["id"])
                    conn.commit()

                except Exception as exc:
                    logger.warning("[Crawler] Error fetching %s: %s", url, exc)
                    mark_url_failed(conn, item["id"])
                    conn.commit()

        logger.info("[Crawler] Worker loop exited cleanly.")


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
engine = CrawlerEngine()
