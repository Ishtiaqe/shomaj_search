"""
crawler.py — Shomaj Search
Async active open-web crawler with a 4-state thread-safe state machine.

States: IDLE → RUNNING ↔ PAUSED → STOPPED (auto-resets to IDLE)

Architecture:
  - Multiple concurrent async workers (configurable, default 3).
  - Each worker independently pops from the SQLite queue.
  - All workers share one httpx.AsyncClient (connection pooling).
  - Product data extracted via ProductExtractor for e-commerce pages.
  - Sitemap.xml discovery seeds deep page sets automatically.
  - Per-domain rate limiting (domain pacing).
  - Domain blocklist (auth pages, social networks, etc.).
"""

import asyncio
import logging
import re
import time
from enum import Enum
from typing import Optional
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
from xml.etree import ElementTree as ET

import httpx
from bs4 import BeautifulSoup

from database import (
    enqueue_url,
    get_db,
    get_queue_stats,
    mark_url_failed,
    mark_domain_crawled,
    pop_pending_url,
    upsert_index,
    upsert_product,
    upsert_managed_domain,
    get_domain_config,
    upsert_media,
    get_system_setting,
    set_system_setting,
)
from product_extractor import ProductExtractor
from media_utils import process_media_thumbnail_bg

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
    "facebook.com", "messenger.com", "whatsapp.com",
    "instagram.com", "twitter.com", "x.com",
    "drive.google.com", "mail.google.com", "docs.google.com",
    "sheets.google.com", "slides.google.com",
    "accounts.google.com", "signin.google.com",
    "login.microsoftonline.com",
    "localhost", "127.0.0.1", "::1",
    "linkedin.com", "tiktok.com", "pinterest.com",
    "snapchat.com", "reddit.com",
    "youtube.com",   # video platform — content not indexable as text
    "netflix.com",
    "amazon.com",    # ToS prohibits scraping; use their Product API instead
    "ebay.com",
}

BLOCKED_PATTERNS: list[re.Pattern] = [
    re.compile(r"^accounts\.",  re.IGNORECASE),
    re.compile(r"^login\.",     re.IGNORECASE),
    re.compile(r"^signin\.",    re.IGNORECASE),
    re.compile(r"^auth\.",      re.IGNORECASE),
    re.compile(r"^sso\.",       re.IGNORECASE),
    re.compile(r"^oauth\.",     re.IGNORECASE),
]

ALLOWED_SCHEMES = {"http", "https"}

# Extensions to skip (binary assets, archives, etc.)
SKIP_EXTENSIONS = {
    ".pdf", ".zip", ".rar", ".gz", ".tar", ".7z",
    ".exe", ".dmg", ".apk", ".ipa",
    ".mp3", ".mp4", ".avi", ".mkv", ".mov", ".wav",
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico", ".bmp",
    ".css", ".js", ".woff", ".woff2", ".ttf", ".eot",
    ".xml",  # handled via sitemap; don't double-fetch
}

# ---------------------------------------------------------------------------
# Crawler Configuration (fully mutable at runtime)
# ---------------------------------------------------------------------------
class CrawlerConfig:
    def __init__(self) -> None:
        # Crawl pacing
        self.delay_seconds: float = 1.5     # min seconds between requests to same domain
        self.max_depth: int       = 3       # max hop depth from seed URL
        self.request_timeout: float = 20.0  # HTTP read timeout per request (seconds)
        self.max_content_mb: float = 5.0    # max response body to download (MB)

        # Concurrency
        self.concurrent_workers: int = 3    # number of parallel async worker coroutines

        # Crawler behaviour
        self.follow_sitemaps: bool    = True   # auto-discover and parse sitemap.xml
        self.extract_products: bool   = True   # run ProductExtractor on every HTML page
        self.max_pages_per_domain: int = 0     # 0 = unlimited
        self.retry_failed: bool       = False  # re-queue failed URLs
        self.respect_robots_txt: bool  = True   # comply with robots.txt Disallow rules

    def update(self, **kwargs) -> None:
        """
        Applies keyword-argument overrides.
        Unknown keys are silently ignored.
        Numeric types are coerced and clamped to sensible ranges.
        """
        if (v := kwargs.get("delay_seconds")) is not None:
            self.delay_seconds = max(0.0, float(v))
        if (v := kwargs.get("max_depth")) is not None:
            self.max_depth = max(0, int(v))
        if (v := kwargs.get("request_timeout")) is not None:
            self.request_timeout = max(5.0, min(120.0, float(v)))
        if (v := kwargs.get("max_content_mb")) is not None:
            self.max_content_mb = max(0.5, min(50.0, float(v)))
        if (v := kwargs.get("concurrent_workers")) is not None:
            self.concurrent_workers = max(1, min(20, int(v)))
        if (v := kwargs.get("follow_sitemaps")) is not None:
            self.follow_sitemaps = bool(v)
        if (v := kwargs.get("extract_products")) is not None:
            self.extract_products = bool(v)
        if (v := kwargs.get("max_pages_per_domain")) is not None:
            self.max_pages_per_domain = max(0, int(v))
        if (v := kwargs.get("retry_failed")) is not None:
            self.retry_failed = bool(v)
        if (v := kwargs.get("respect_robots_txt")) is not None:
            self.respect_robots_txt = bool(v)

    def to_dict(self) -> dict:
        return {
            "delay_seconds":       self.delay_seconds,
            "max_depth":           self.max_depth,
            "request_timeout":     self.request_timeout,
            "max_content_mb":      self.max_content_mb,
            "concurrent_workers":  self.concurrent_workers,
            "follow_sitemaps":     self.follow_sitemaps,
            "extract_products":    self.extract_products,
            "max_pages_per_domain": self.max_pages_per_domain,
            "retry_failed":        self.retry_failed,
            "respect_robots_txt":  self.respect_robots_txt,
        }

    def load_from_db(self, conn) -> None:
        """Loads configuration from sqlite system_settings table."""
        self.delay_seconds = float(get_system_setting(conn, "delay_seconds", str(self.delay_seconds)))
        self.max_depth = int(get_system_setting(conn, "max_depth", str(self.max_depth)))
        self.request_timeout = float(get_system_setting(conn, "request_timeout", str(self.request_timeout)))
        self.max_content_mb = float(get_system_setting(conn, "max_content_mb", str(self.max_content_mb)))
        self.concurrent_workers = int(get_system_setting(conn, "concurrent_workers", str(self.concurrent_workers)))
        self.follow_sitemaps = get_system_setting(conn, "follow_sitemaps", str(self.follow_sitemaps)) == "True"
        self.extract_products = get_system_setting(conn, "extract_products", str(self.extract_products)) == "True"
        self.max_pages_per_domain = int(get_system_setting(conn, "max_pages_per_domain", str(self.max_pages_per_domain)))
        self.retry_failed = get_system_setting(conn, "retry_failed", str(self.retry_failed)) == "True"
        self.respect_robots_txt = get_system_setting(conn, "respect_robots_txt", str(self.respect_robots_txt)) == "True"

    def save_to_db(self, conn) -> None:
        """Saves current configuration to sqlite system_settings table."""
        set_system_setting(conn, "delay_seconds", str(self.delay_seconds))
        set_system_setting(conn, "max_depth", str(self.max_depth))
        set_system_setting(conn, "request_timeout", str(self.request_timeout))
        set_system_setting(conn, "max_content_mb", str(self.max_content_mb))
        set_system_setting(conn, "concurrent_workers", str(self.concurrent_workers))
        set_system_setting(conn, "follow_sitemaps", str(self.follow_sitemaps))
        set_system_setting(conn, "extract_products", str(self.extract_products))
        set_system_setting(conn, "max_pages_per_domain", str(self.max_pages_per_domain))
        set_system_setting(conn, "retry_failed", str(self.retry_failed))
        set_system_setting(conn, "respect_robots_txt", str(self.respect_robots_txt))


# ---------------------------------------------------------------------------
# Singleton Crawler Engine
# ---------------------------------------------------------------------------
class CrawlerEngine:
    """
    Manages the crawler state machine and N concurrent async worker tasks.
    Access via the module-level `engine` instance.
    """

    def __init__(self) -> None:
        self.state:  CrawlerState  = CrawlerState.IDLE
        self.config: CrawlerConfig = CrawlerConfig()
        self._lock:  asyncio.Lock  = asyncio.Lock()

        # Aggregate metrics
        self._items_processed: int          = 0
        self._items_failed: int             = 0
        self._products_extracted: int       = 0
        self._started_at: Optional[float]   = None

        # Per-domain last-fetch timestamps for rate limiting
        self._domain_last_fetch: dict[str, float] = {}
        # Per-domain page counts (for max_pages_per_domain enforcement)
        self._domain_page_count: dict[str, int]   = {}

        # Worker tasks (list supports N workers)
        self._tasks: list[asyncio.Task] = []

        # Shared HTTP client (created on first start, closed on stop)
        self._client: Optional[httpx.AsyncClient] = None

        # Product extractor (stateless, shared across workers)
        self._extractor: ProductExtractor = ProductExtractor()

        # Cache of parsed robots.txt rules per domain
        self._robots_txt_parsers: dict[str, Optional[RobotFileParser]] = {}

    def load_config(self) -> None:
        """Loads configuration from the database system_settings table."""
        try:
            conn = get_db()
            self.config.load_from_db(conn)
            logger.info("[Crawler] Configuration loaded from database.")
        except Exception as e:
            logger.debug("[Crawler] Failed to load config from DB: %s", e)

    def save_config(self) -> None:
        """Saves configuration to the database system_settings table."""
        try:
            conn = get_db()
            self.config.save_to_db(conn)
            conn.commit()
            logger.info("[Crawler] Configuration saved to database.")
        except Exception as e:
            logger.warning("[Crawler] Failed to save config to DB: %s", e)

    # ------------------------------------------------------------------
    # State transitions
    # ------------------------------------------------------------------

    async def start(self) -> str:
        """Transitions to RUNNING. Spawns up to `concurrent_workers` worker tasks."""
        async with self._lock:
            if self.state not in (CrawlerState.IDLE, CrawlerState.PAUSED):
                return f"cannot_start_from_{self.state.value}"

            self.state = CrawlerState.RUNNING
            self._started_at = self._started_at or time.time()

            # Initialise shared client if needed
            if self._client is None or self._client.is_closed:
                self._client = httpx.AsyncClient(
                    follow_redirects=True,
                    timeout=httpx.Timeout(self.config.request_timeout),
                    headers={
                        "User-Agent": (
                            "Mozilla/5.0 (compatible; ShomajBot/1.0; "
                            "+http://shomaj.one/bot)"
                        )
                    },
                    limits=httpx.Limits(
                        max_connections=self.config.concurrent_workers * 3,
                        max_keepalive_connections=self.config.concurrent_workers,
                    ),
                )

            # Prune finished tasks
            self._tasks = [t for t in self._tasks if not t.done()]

            # Spawn additional workers up to the configured count
            needed = self.config.concurrent_workers - len(self._tasks)
            for i in range(max(0, needed)):
                worker_id = len(self._tasks)
                task = asyncio.create_task(
                    self._worker_loop(worker_id),
                    name=f"crawler-worker-{worker_id}",
                )
                self._tasks.append(task)

            logger.info(
                "[Crawler] Started — %d workers active.",
                len(self._tasks),
            )
            return "started"

    async def adjust_workers(self) -> None:
        """Dynamically adjusts the number of active worker tasks to match concurrent_workers configuration."""
        async with self._lock:
            if self.state != CrawlerState.RUNNING:
                return
            
            # Prune finished tasks first
            self._tasks = [t for t in self._tasks if not t.done()]
            
            # If we need more workers, spawn them
            needed = self.config.concurrent_workers - len(self._tasks)
            if needed > 0:
                for _ in range(needed):
                    worker_id = len(self._tasks)
                    task = asyncio.create_task(
                        self._worker_loop(worker_id),
                        name=f"crawler-worker-{worker_id}",
                    )
                    self._tasks.append(task)
                logger.info("[Crawler] Scaled up dynamically to %d workers.", len(self._tasks))

    async def pause(self) -> str:
        """Suspends all workers without flushing the queue."""
        async with self._lock:
            if self.state == CrawlerState.RUNNING:
                self.state = CrawlerState.PAUSED
                logger.info("[Crawler] Paused.")
                return "paused"
            return f"not_running (state={self.state.value})"

    async def stop(self) -> str:
        """Cancels all workers, closes the HTTP client, and resets to IDLE."""
        async with self._lock:
            if self.state == CrawlerState.IDLE:
                return "already_idle"

            self.state = CrawlerState.STOPPED
            for task in self._tasks:
                task.cancel()
            self._tasks.clear()

            if self._client and not self._client.is_closed:
                await self._client.aclose()
            self._client = None

            self._domain_last_fetch.clear()
            self._domain_page_count.clear()
            self._robots_txt_parsers.clear()
            self._started_at = None
            self.state = CrawlerState.IDLE
            logger.info("[Crawler] Stopped and reset to IDLE.")
            return "stopped"

    def status(self) -> dict:
        """Returns a status snapshot for the API."""
        alive_workers = sum(1 for t in self._tasks if not t.done())
        qs = get_queue_stats(get_db())
        return {
            "state":              self.state.value,
            "active_workers":     alive_workers,
            "configured_workers": self.config.concurrent_workers,
            "items_processed":    self._items_processed,
            "items_failed":       self._items_failed,
            "products_extracted": self._products_extracted,
            "queue_pending":      qs.get("pending",   0),
            "queue_completed":    qs.get("completed", 0),
            "queue_failed":       qs.get("failed",    0),
            "delay_seconds":      self.config.delay_seconds,
            "max_depth":          self.config.max_depth,
            "concurrent_workers": self.config.concurrent_workers,
            "uptime_seconds":     int(time.time() - self._started_at)
                                  if self._started_at else 0,
        }

    def is_blocked(self, url: str) -> bool:
        """Returns True if the URL's domain is in the blocklist."""
        try:
            host = urlparse(url).hostname or ""
            host = host.lstrip("www.")
        except Exception:
            return True

        if host in BLOCKED_DOMAINS:
            return True
        for pattern in BLOCKED_PATTERNS:
            if pattern.search(host):
                return True
        return False

    # ------------------------------------------------------------------
    # Domain-level helpers
    # ------------------------------------------------------------------

    async def _wait_for_domain_rate_limit(self, domain: str) -> None:
        """Enforces per-domain minimum delay between requests."""
        last = self._domain_last_fetch.get(domain, 0.0)
        wait = self.config.delay_seconds - (time.time() - last)
        if wait > 0:
            await asyncio.sleep(wait)
        self._domain_last_fetch[domain] = time.time()

    async def _is_allowed_by_robots(self, domain: str, url: str) -> bool:
        """
        Checks if the URL is allowed to be crawled by robots.txt.
        Caches the parsed robots.txt rules for each domain.
        """
        if not self.config.respect_robots_txt:
            return True

        if domain in self._robots_txt_parsers:
            parser = self._robots_txt_parsers[domain]
            if parser is None:
                return True
            try:
                return parser.can_fetch("ShomajBot", url)
            except Exception:
                return True

        # Fetch and parse robots.txt
        try:
            parsed_url = urlparse(url)
            scheme = parsed_url.scheme or "http"
            robots_url = f"{scheme}://{domain}/robots.txt"
        except Exception:
            return True

        logger.info("[Robots.txt] Fetching rules for %s", domain)
        parser = RobotFileParser()
        try:
            # Respect rate limits for robots.txt fetch
            await self._wait_for_domain_rate_limit(domain)

            client = self._client
            if client is None or client.is_closed:
                return True

            r = await client.get(robots_url, timeout=10.0)
            self._domain_last_fetch[domain] = time.time()

            if r.status_code == 200:
                parser.parse(r.text.splitlines())
                self._robots_txt_parsers[domain] = parser
                return parser.can_fetch("ShomajBot", url)
            elif r.status_code in (401, 403):
                parser.parse(["User-agent: *", "Disallow: /"])
                self._robots_txt_parsers[domain] = parser
                return False
            else:
                self._robots_txt_parsers[domain] = None
                return True
        except Exception as exc:
            logger.debug("[Robots.txt] Failed to fetch robots.txt for %s: %s", domain, exc)
            self._robots_txt_parsers[domain] = None
            return True

    def _domain_page_limit_reached(self, domain: str) -> bool:
        """Returns True if this domain has hit max_pages_per_domain."""
        limit = self.config.max_pages_per_domain
        if limit <= 0:
            return False
        return self._domain_page_count.get(domain, 0) >= limit

    def _domain_from_url(self, url: str) -> str:
        try:
            return (urlparse(url).hostname or "").lstrip("www.")
        except Exception:
            return ""

    def _has_skip_extension(self, url: str) -> bool:
        """Returns True for URLs pointing to binary assets."""
        path = urlparse(url).path.lower().split("?")[0]
        for ext in SKIP_EXTENSIONS:
            if path.endswith(ext):
                return True
        return False

    # ------------------------------------------------------------------
    # Sitemap Discovery
    # ------------------------------------------------------------------

    async def discover_and_seed_sitemap(
        self, base_url: str, domain: str, max_urls: int = 5000
    ) -> int:
        """
        Discovers sitemap.xml for a domain, parses it (including sitemap indexes),
        and seeds all discovered URLs into the crawl queue.

        Returns the number of new URLs seeded.
        """
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                follow_redirects=True,
                timeout=httpx.Timeout(15.0),
                headers={"User-Agent": "Mozilla/5.0 (compatible; ShomajBot/1.0)"},
            )

        client = self._client
        sitemap_candidates: list[str] = []

        # Check robots.txt for Sitemap directives first
        try:
            robots_url = urljoin(base_url, "/robots.txt")
            r = await client.get(robots_url, timeout=10.0)
            if r.status_code == 200:
                for line in r.text.splitlines():
                    if line.lower().startswith("sitemap:"):
                        sm_url = line.split(":", 1)[1].strip()
                        if sm_url.startswith("http"):
                            sitemap_candidates.append(sm_url)
        except Exception:
            pass

        # Fallback candidates
        for path in ["/sitemap.xml", "/sitemap_index.xml", "/sitemaps.xml",
                     "/sitemap/sitemap.xml", "/sitemap1.xml"]:
            candidate = urljoin(base_url, path)
            if candidate not in sitemap_candidates:
                sitemap_candidates.append(candidate)

        all_urls: list[str] = []
        for sm_url in sitemap_candidates:
            try:
                urls = await self._parse_sitemap(client, sm_url, depth=0)
                all_urls.extend(urls)
                if all_urls:
                    break  # stop at first working sitemap
            except Exception as exc:
                logger.debug("[Sitemap] Failed %s: %s", sm_url, exc)

        if not all_urls:
            logger.info("[Sitemap] No sitemap found for %s", domain)
            return 0

        # Seed into queue (up to max_urls)
        conn = get_db()
        seeded = 0
        for url in all_urls[:max_urls]:
            url = url.strip()
            if not url or self.is_blocked(url) or self._has_skip_extension(url):
                continue
            if enqueue_url(conn, url, depth=1):
                seeded += 1
        conn.commit()

        # Update domain metadata
        try:
            mark_domain_crawled(conn, domain)
            conn.commit()
        except Exception:
            pass

        logger.info("[Sitemap] Seeded %d URLs for %s", seeded, domain)
        return seeded

    async def _parse_sitemap(
        self, client: httpx.AsyncClient, url: str, depth: int = 0
    ) -> list[str]:
        """Parses a sitemap URL. Recursively handles sitemap index files."""
        if depth > 3:  # guard against deeply nested indexes
            return []

        resp = await client.get(url, timeout=15.0)
        if resp.status_code != 200:
            return []

        content = resp.text
        urls: list[str] = []

        try:
            # Strip namespace for simpler parsing
            content_clean = re.sub(r'\s*xmlns[^"]*"[^"]*"', "", content)
            root = ET.fromstring(content_clean)

            # Sitemap index — contains <sitemap><loc> child sitemaps
            for sitemap_el in root.findall(".//sitemap/loc"):
                child_url = (sitemap_el.text or "").strip()
                if child_url:
                    child_urls = await self._parse_sitemap(client, child_url, depth + 1)
                    urls.extend(child_urls)

            # Regular sitemap — contains <url><loc> page URLs
            for url_el in root.findall(".//url/loc"):
                page_url = (url_el.text or "").strip()
                if page_url:
                    urls.append(page_url)

        except ET.ParseError:
            # Some sitemaps are malformed — try regex fallback
            urls.extend(re.findall(r"<loc>\s*(https?://[^\s<]+)\s*</loc>", content))

        return urls

    # ------------------------------------------------------------------
    # Worker Loop
    # ------------------------------------------------------------------

    async def _worker_loop(self, worker_id: int = 0) -> None:
        """
        Independent async worker. Pops URLs from the queue and processes them.
        Multiple instances run concurrently (one per configured worker slot).
        """
        logger.info("[Worker-%d] Started.", worker_id)
        conn = get_db()

        while True:
            # Yield control to event loop between iterations
            await asyncio.sleep(0)

            # Check if scaled down dynamically
            if worker_id >= self.config.concurrent_workers:
                logger.info("[Worker-%d] Exiting dynamically (scaled down).", worker_id)
                break

            if self.state == CrawlerState.PAUSED:
                await asyncio.sleep(1.0)
                continue

            if self.state not in (CrawlerState.RUNNING,):
                break

            # Pop a URL from the shared queue
            item = pop_pending_url(conn)
            if item is None:
                # Queue is empty — idle sleep before retrying
                await asyncio.sleep(2.0)
                continue

            url   = item["url"]
            depth = item["depth"]
            qid   = item["id"]
            domain = self._domain_from_url(url)

            # Skip if URL doesn't qualify
            if not url or self.is_blocked(url) or self._has_skip_extension(url):
                continue

            if depth > self.config.max_depth:
                continue

            if self._domain_page_limit_reached(domain):
                logger.debug("[Worker-%d] Domain limit reached: %s", worker_id, domain)
                continue

            # Check if domain is managed and disabled
            try:
                dom_cfg = get_domain_config(conn, domain)
                if dom_cfg and not dom_cfg.get("crawl_enabled", 1):
                    logger.debug("[Worker-%d] Crawl disabled for %s", worker_id, domain)
                    continue
            except Exception:
                pass

            # Check robots.txt compliance
            if not await self._is_allowed_by_robots(domain, url):
                logger.info("[Worker-%d] Blocked by robots.txt: %s", worker_id, url)
                continue

            # Rate limiting — wait if we hit this domain too recently
            await self._wait_for_domain_rate_limit(domain)

            if self._client is None or self._client.is_closed:
                await asyncio.sleep(1.0)
                continue

            # Fetch & process the URL
            try:
                await self._fetch_and_index(conn, url, domain, depth, worker_id)
                self._items_processed += 1
                self._domain_page_count[domain] = (
                    self._domain_page_count.get(domain, 0) + 1
                )
            except asyncio.CancelledError:
                raise  # propagate cancellation
            except Exception as exc:
                logger.warning("[Worker-%d] Error on %s: %s", worker_id, url, exc)
                mark_url_failed(conn, qid)
                conn.commit()
                self._items_failed += 1

        logger.info("[Worker-%d] Exiting.", worker_id)

    # ------------------------------------------------------------------
    # Fetch + Index
    # ------------------------------------------------------------------

    async def _fetch_and_index(
        self,
        conn,
        url: str,
        domain: str,
        depth: int,
        worker_id: int,
    ) -> None:
        """
        Fetches a single URL, extracts text + product data + links,
        and writes everything to the database.
        """
        max_bytes = int(self.config.max_content_mb * 1024 * 1024)

        # Streaming fetch to enforce content-size limit
        html = ""
        content_type = ""
        try:
            async with self._client.stream(
                "GET", url,
                timeout=httpx.Timeout(self.config.request_timeout),
            ) as resp:
                if resp.status_code not in (200, 203):
                    return

                content_type = resp.headers.get("content-type", "")
                if "html" not in content_type.lower():
                    return  # skip non-HTML (images, APIs, etc.)

                chunks = []
                total = 0
                async for chunk in resp.aiter_bytes(chunk_size=65536):
                    total += len(chunk)
                    if total > max_bytes:
                        break
                    chunks.append(chunk)
                html = b"".join(chunks).decode("utf-8", errors="replace")
        except httpx.HTTPError as e:
            logger.debug("[Worker-%d] HTTP error %s: %s", worker_id, url, e)
            return

        if not html.strip():
            return

        soup = BeautifulSoup(html, "html.parser")

        # --- Text extraction ---
        for tag in soup(["script", "style", "noscript", "nav",
                         "header", "footer", "aside"]):
            tag.decompose()

        title_tag = soup.find("title")
        title = (title_tag.get_text(strip=True) if title_tag else domain)[:512]
        clean_text = " ".join(soup.get_text(separator=" ").split())[:200_000]

        # Determine if domain was registered as public via managed_domains
        dom_cfg = get_domain_config(conn, domain)
        is_public_domain = dom_cfg.get("is_public", 1) if dom_cfg else 1
        is_private_page  = 0 if is_public_domain else 1

        # Write to FTS index
        upsert_index(conn, url, title, clean_text, domain, is_private_page)

        # --- Media extraction ---
        images_found = 0
        videos_found = 0
        try:
            # Parse images
            for img_tag in soup.find_all("img", src=True):
                src = img_tag["src"].strip()
                if not src:
                    continue
                abs_img_url = urljoin(url, src)
                if not abs_img_url.startswith(("http://", "https://")):
                    continue
                img_domain = self._domain_from_url(abs_img_url)
                alt = (img_tag.get("alt") or img_tag.get("title") or "").strip()
                width = 0
                height = 0
                try:
                    if "width" in img_tag.attrs:
                        width = int(re.sub(r"\D", "", str(img_tag["width"])))
                    if "height" in img_tag.attrs:
                        height = int(re.sub(r"\D", "", str(img_tag["height"])))
                except Exception:
                    pass
                
                ext = abs_img_url.rsplit(".", 1)[-1].lower().split("?")[0] if "." in abs_img_url else ""
                upsert_media(
                    conn=conn,
                    media_url=abs_img_url,
                    page_url=url,
                    media_type="image",
                    title=alt[:512],
                    description="",
                    domain=img_domain,
                    width=width,
                    height=height,
                    thumbnail_url=abs_img_url,
                    fmt=ext,
                    is_private=is_private_page,
                )
                asyncio.create_task(process_media_thumbnail_bg(abs_img_url, abs_img_url))
                images_found += 1
                if images_found >= 100:
                    break

            # Parse videos
            for vid_tag in soup.find_all(["video", "iframe"], src=True):
                src = vid_tag["src"].strip()
                if not src:
                    continue
                abs_vid_url = urljoin(url, src)
                if not abs_vid_url.startswith(("http://", "https://")):
                    continue
                is_video = False
                title = ""
                thumbnail = ""
                
                if vid_tag.name == "video":
                    is_video = True
                    title = (vid_tag.get("title") or "").strip()
                    thumbnail = (vid_tag.get("poster") or "").strip()
                    if thumbnail:
                        thumbnail = urljoin(url, thumbnail)
                elif vid_tag.name == "iframe":
                    if any(x in abs_vid_url for x in ("youtube.com", "youtu.be", "vimeo.com", "dailymotion.com")):
                        is_video = True
                        title = (vid_tag.get("title") or "").strip()
                
                if is_video:
                    vid_domain = self._domain_from_url(abs_vid_url)
                    ext = abs_vid_url.rsplit(".", 1)[-1].lower().split("?")[0] if "." in abs_vid_url else ""
                    upsert_media(
                        conn=conn,
                        media_url=abs_vid_url,
                        page_url=url,
                        media_type="video",
                        title=title[:512],
                        description="",
                        domain=vid_domain,
                        thumbnail_url=thumbnail[:1024],
                        fmt=ext,
                        is_private=is_private_page,
                    )
                    asyncio.create_task(process_media_thumbnail_bg(abs_vid_url, thumbnail))
                    videos_found += 1
                    if videos_found >= 10:
                        break
        except Exception as exc:
            logger.debug("[Worker-%d] Media extract failed %s: %s", worker_id, url, exc)

        # --- Product extraction ---
        products_found = 0
        if self.config.extract_products:
            try:
                product = self._extractor.extract(html, url)
                if product.is_product_page and product.name:
                    upsert_product(
                        conn=conn,
                        url=url,
                        name=product.name,
                        description=product.description,
                        brand=product.brand,
                        sku=product.sku,
                        price=product.price,
                        price_text=product.price_text,
                        currency=product.currency or "BDT",
                        availability=product.availability,
                        image_url=product.image_url,
                        domain=domain,
                        is_private=is_private_page,
                        schema_type=product.schema_type,
                        raw_schema=product.raw_schema,
                    )
                    products_found = 1
                    self._products_extracted += 1
            except Exception as exc:
                logger.debug("[Worker-%d] Product extract failed %s: %s",
                             worker_id, url, exc)

        # --- Sitemap discovery for new domains ---
        if self.config.follow_sitemaps and depth == 0:
            known_domain_keys = {self._domain_from_url(u)
                                 for u in [url]}
            for kd in known_domain_keys:
                if kd not in self._domain_page_count:
                    # New domain — try to find sitemap
                    asyncio.create_task(
                        self.discover_and_seed_sitemap(
                            base_url=f"{urlparse(url).scheme}://{urlparse(url).netloc}",
                            domain=kd,
                        ),
                        name=f"sitemap-{kd}",
                    )

        # --- Link extraction (enqueue children) ---
        links_added = 0
        if depth < self.config.max_depth:
            for a_tag in soup.find_all("a", href=True):
                href = a_tag["href"].strip()
                if not href:
                    continue
                abs_url = urljoin(url, href)
                parsed_href = urlparse(abs_url)
                if parsed_href.scheme not in ALLOWED_SCHEMES:
                    continue
                if self.is_blocked(abs_url) or self._has_skip_extension(abs_url):
                    continue
                if enqueue_url(conn, abs_url, depth + 1):
                    links_added += 1

        conn.commit()

        logger.info(
            "[Worker-%d] ✓ Indexed → %s (products=%d media=%d links+=%d total=%d)",
            worker_id, url, products_found, images_found + videos_found, links_added, self._items_processed,
        )


# ---------------------------------------------------------------------------
# Module-level singleton — import this everywhere
# ---------------------------------------------------------------------------
engine: CrawlerEngine = CrawlerEngine()
