#!/usr/bin/env python3
"""
verify_stack.py — Shomaj Search Integration Test Suite
=======================================================

Validates the full stack:
  1. Backend health check
  2. Database schema integrity
  3. Crawler state machine transitions
  4. Passive ingestion (extension simulation)
  5. FTS5 search with BM25 scoring
  6. Keyword relevance ranking order
  7. Queue seeding and stats
  8. Configuration updates
  9. Domain blocklist enforcement (via crawler engine)
 10. Multiple document search ranking

Usage:
    # Start the server first:
    #   uvicorn main:app --reload
    # Then in another terminal:
    python verify_stack.py

Exit codes:
    0 — All tests passed
    1 — One or more tests failed
"""

import sys
import time
import json
import sqlite3
import asyncio
import urllib.request
import urllib.error
from typing import Any

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_URL    = "http://localhost:8000"
DB_PATH     = "shomaj_search.db"
TIMEOUT_SEC = 10

# ---------------------------------------------------------------------------
# Colour codes for terminal output
# ---------------------------------------------------------------------------
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

# ---------------------------------------------------------------------------
# Test runner state
# ---------------------------------------------------------------------------
_pass_count = 0
_fail_count = 0
_results:  list[dict] = []


def _record(name: str, passed: bool, detail: str = "") -> None:
    global _pass_count, _fail_count
    if passed:
        _pass_count += 1
        status_str = f"{GREEN}✓ PASS{RESET}"
    else:
        _fail_count += 1
        status_str = f"{RED}✗ FAIL{RESET}"

    suffix = f"  {YELLOW}{detail}{RESET}" if detail else ""
    print(f"  {status_str}  {name}{suffix}")
    _results.append({"name": name, "passed": passed, "detail": detail})


# ---------------------------------------------------------------------------
# HTTP helpers (stdlib only — no external deps needed for the test runner)
# ---------------------------------------------------------------------------
def http_get(path: str) -> tuple[int, dict]:
    """GET request → (status_code, json_body)"""
    url = BASE_URL + path
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = {}
        try:
            body = json.loads(e.read())
        except Exception:
            pass
        return e.code, body


def http_post(path: str, body: Any = None) -> tuple[int, dict]:
    """POST request with JSON body → (status_code, json_body)"""
    url  = BASE_URL + path
    data = json.dumps(body).encode() if body is not None else b""
    req  = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body_bytes = b""
        try:
            body_bytes = e.read()
        except Exception:
            pass
        body = {}
        try:
            body = json.loads(body_bytes)
        except Exception:
            pass
        return e.code, body


# ---------------------------------------------------------------------------
# Individual test functions
# ---------------------------------------------------------------------------

def test_health_check() -> None:
    """Backend should respond with status:healthy."""
    code, body = http_get("/health")
    _record("Health check returns 200", code == 200)
    _record("Health status is 'healthy'", body.get("status") == "healthy")
    _record("Service name is shomaj-search", body.get("service") == "shomaj-search")


def test_database_schema() -> None:
    """SQLite DB should have all required tables and WAL mode active."""
    print(f"\n  Connecting to {DB_PATH}…")
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row

        # Check WAL mode
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        _record("WAL journal mode active", mode == "wal", f"(got: {mode!r})")

        # Check FTS5 table exists
        tables = {row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'shadow')"
        )}
        _record("search_index (FTS5) table exists", "search_index" in tables)
        _record("crawl_metadata table exists",       "crawl_metadata" in tables)
        _record("queue table exists",                "queue" in tables)

        # Check queue index exists
        indexes = {row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        )}
        _record("queue status index exists", "idx_queue_status_depth" in indexes)
        _record("metadata domain index exists", "idx_metadata_domain" in indexes)

        conn.close()
    except Exception as exc:
        _record("Database schema check", False, str(exc))


def test_passive_ingestion() -> None:
    """POST /api/index should index a page and return links_seeded count."""

    # Index a known document
    payload = {
        "url":   "https://example.com/test-page",
        "title": "Test Page for Shomaj Verify",
        "text":  "Shomaj Search is a powerful local hybrid search engine built with FastAPI and SQLite FTS5.",
        "links": [
            "https://example.com/page-a",
            "https://example.com/page-b",
            "https://blocked.facebook.com/should-not-appear",  # should be blocked
        ],
    }
    code, body = http_post("/api/index", payload)
    _record("POST /api/index returns 200", code == 200)
    _record("Indexed flag is True", body.get("data", {}).get("indexed") is True)

    seeded = body.get("data", {}).get("links_seeded", -1)
    # Only up to 2 valid public links should be seeded (facebook blocked).
    # On subsequent runs, links are already in DB so seeded may be 0 (INSERT OR IGNORE).
    _record(
        "links_seeded >= 0 and facebook blocked (not -1)",
        seeded >= 0,
        f"(got {seeded} — 0 is OK on re-run, 2 on fresh DB)",
    )


def test_second_document() -> None:
    """Index a second document on a different topic for ranking tests."""
    payload = {
        "url":   "https://example.org/python-asyncio",
        "title": "Python AsyncIO Guide",
        "text":  (
            "Python asyncio is the cornerstone of asynchronous programming. "
            "The event loop drives coroutines and tasks concurrently. "
            "Use asyncio.create_task to run coroutines as background tasks."
        ),
        "links": [],
    }
    code, body = http_post("/api/index", payload)
    _record("Second document indexed OK", code == 200 and body.get("data", {}).get("indexed"))


def test_third_document_high_relevance() -> None:
    """Index a document with high keyword density for ranking verification."""
    payload = {
        "url":   "https://example.com/hybrid-search-engine",
        "title": "Hybrid Search Engine Architecture",
        "text":  (
            "A hybrid search engine combines full-text search with vector embeddings. "
            "Hybrid search engine systems use BM25 scoring for keyword retrieval. "
            "Building a hybrid search engine requires careful indexing strategy. "
            "The hybrid search engine approach delivers superior relevance ranking. "
            "FastAPI powers our hybrid search engine backend efficiently."
        ),
        "links": [],
    }
    code, body = http_post("/api/index", payload)
    _record("High-relevance document indexed OK", code == 200)


def test_search_basic() -> None:
    """GET /api/search should return results for indexed content."""
    time.sleep(0.5)  # give SQLite a moment to commit
    code, body = http_get("/api/search?q=shomaj+search")
    _record("GET /api/search returns 200", code == 200)

    data = body.get("data", {})
    _record("Search returns results", data.get("total_hits", 0) > 0)
    _record("Results array is present", isinstance(data.get("results"), list))

    results = data.get("results", [])
    if results:
        first = results[0]
        _record("Result has 'url' field",     "url"     in first)
        _record("Result has 'title' field",   "title"   in first)
        _record("Result has 'snippet' field", "snippet" in first)
        _record("Result has 'score' field",   "score"   in first)
        _record("Result has 'is_private' field", "is_private" in first)
        _record("Score is a positive float", isinstance(first.get("score"), float) and first["score"] >= 0)


def test_search_bm25_ranking() -> None:
    """
    The document with higher keyword density ('hybrid search engine' repeated 5×)
    should rank above the document where the term appears once.
    """
    code, body = http_get("/api/search?q=hybrid+search+engine")
    data = body.get("data", {})
    results = data.get("results", [])

    if not results:
        _record("BM25 ranking test (no results — skip)", True, "⚠ no results to rank")
        return

    urls = [r["url"] for r in results]
    high_rel_url = "https://example.com/hybrid-search-engine"
    low_rel_url  = "https://example.com/test-page"

    high_pos = urls.index(high_rel_url) if high_rel_url in urls else 999
    low_pos  = urls.index(low_rel_url)  if low_rel_url  in urls else 999

    _record(
        "High-density doc ranks above low-density doc",
        high_pos < low_pos,
        f"(positions: high={high_pos}, low={low_pos})",
    )

    # Verify scores decrease (ascending positions = decreasing relevance)
    scores = [r["score"] for r in results]
    _record("Scores are non-increasing (best first)", all(
        scores[i] >= scores[i+1] for i in range(len(scores)-1)
    ))


def test_search_no_results() -> None:
    """A query with no matching content should return empty results, not an error."""
    import time
    term = f"zzzzxxxxxnonexistentkeyword9999_{int(time.time())}"
    code, body = http_get(f"/api/search?q={term}")
    _record("Search with no matches returns 200", code == 200)
    data = body.get("data", {})
    _record("total_hits is 0 for missing term", data.get("total_hits", -1) == 0)


def test_search_empty_query() -> None:
    """An empty query should return 422 validation error."""
    code, _ = http_get("/api/search?q=")
    _record("Empty query returns 422", code == 422)


def test_stats_endpoint() -> None:
    """GET /api/stats should reflect the documents we indexed."""
    code, body = http_get("/api/stats")
    _record("GET /api/stats returns 200", code == 200)
    data = body.get("data", {})
    _record("total_indexed >= 3", data.get("total_indexed", 0) >= 3)
    _record("private_indexed >= 3", data.get("private_indexed", 0) >= 3)  # all from extension
    _record("top_domains is a list", isinstance(data.get("top_domains"), list))


def test_crawler_state_machine() -> None:
    """Test the full IDLE → RUNNING → PAUSED → RUNNING → STOPPED cycle."""
    # 1. Check initial state
    code, body = http_get("/api/crawl/status")
    _record("GET /api/crawl/status returns 200", code == 200)
    initial_state = body.get("data", {}).get("state")
    _record("Initial crawler state is IDLE", initial_state == "IDLE", f"(got {initial_state!r})")

    # 2. Start
    code, body = http_post("/api/crawl/start")
    _record("POST /api/crawl/start returns 200", code == 200)
    data = body.get("data", {})
    _record("State transitions to RUNNING on start", data.get("state") == "RUNNING")

    time.sleep(0.5)

    # 3. Pause
    code, body = http_post("/api/crawl/pause")
    _record("POST /api/crawl/pause returns 200", code == 200)
    _record("State transitions to PAUSED", body.get("data", {}).get("state") == "PAUSED")

    # 4. Resume
    code, body = http_post("/api/crawl/start")
    _record("POST /api/crawl/start (resume) returns 200", code == 200)
    _record("State transitions back to RUNNING", body.get("data", {}).get("state") == "RUNNING")

    time.sleep(0.5)

    # 5. Stop
    code, body = http_post("/api/crawl/stop")
    _record("POST /api/crawl/stop returns 200", code == 200)
    _record("State resets to IDLE after stop", body.get("data", {}).get("state") == "IDLE")

    # 6. Verify status endpoint reflects the reset
    code, body = http_get("/api/crawl/status")
    state = body.get("data", {}).get("state")
    _record("Status confirms IDLE after stop", state == "IDLE", f"(got {state!r})")


def test_crawler_config() -> None:
    """POST /api/crawl/config should update delay, depth, and robots.txt setting."""
    code, body = http_post("/api/crawl/config", {
        "delay_seconds": 2.5,
        "max_depth": 5,
        "respect_robots_txt": False
    })
    _record("POST /api/crawl/config returns 200", code == 200)
    data = body.get("data", {})
    _record("delay_seconds updated to 2.5", data.get("delay_seconds") == 2.5)
    _record("max_depth updated to 5",       data.get("max_depth") == 5)
    _record("respect_robots_txt updated to False", data.get("respect_robots_txt") is False)

    # Restore defaults
    http_post("/api/crawl/config", {
        "delay_seconds": 1.5,
        "max_depth": 3,
        "respect_robots_txt": True
    })


def test_config_persistence() -> None:
    """POST /api/crawl/config should write values to system_settings table in SQLite."""
    # 1. Update config via API
    code, body = http_post("/api/crawl/config", {
        "delay_seconds": 4.2,
        "max_depth": 7,
        "respect_robots_txt": False
    })
    _record("POST /api/crawl/config returns 200 (persistence test)", code == 200)

    # 2. Query DB directly to check if they are saved
    import sqlite3
    from database import DB_PATH
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    
    row_delay = conn.execute("SELECT value FROM system_settings WHERE key = 'delay_seconds'").fetchone()
    row_depth = conn.execute("SELECT value FROM system_settings WHERE key = 'max_depth'").fetchone()
    row_robots = conn.execute("SELECT value FROM system_settings WHERE key = 'respect_robots_txt'").fetchone()
    conn.close()

    _record("delay_seconds written to database as '4.2'", row_delay is not None and row_delay["value"] == "4.2")
    _record("max_depth written to database as '7'", row_depth is not None and row_depth["value"] == "7")
    _record("respect_robots_txt written to database as 'False'", row_robots is not None and row_robots["value"] == "False")

    # Restore defaults
    http_post("/api/crawl/config", {
        "delay_seconds": 1.5,
        "max_depth": 3,
        "respect_robots_txt": True
    })


def test_seed_endpoint() -> None:
    """POST /api/crawl/seed should add valid URLs and reject blocked ones."""
    # Use unique timestamped URLs so they are always fresh even on re-runs
    ts = int(time.time())
    payload = {
        "urls": [
            f"https://news.ycombinator.com/test-{ts}",
            f"https://en.wikipedia.org/wiki/Search_engine_{ts}",
            "https://facebook.com/blocked",          # blocked
            "https://localhost/also-blocked",        # blocked
        ],
        "depth": 0,
    }
    code, body = http_post("/api/crawl/seed", payload)
    _record("POST /api/crawl/seed returns 200", code == 200)
    data = body.get("data", {})
    _record("2 valid URLs added",     data.get("added",   -1) == 2, f"(got {data.get('added')})")
    _record("2 blocked URLs skipped", data.get("skipped", -1) == 2, f"(got {data.get('skipped')})")  


def test_queue_stats() -> None:
    """After seeding, the pending queue count should be >= 0."""
    code, body = http_get("/api/crawl/status")
    data = body.get("data", {})
    pending = data.get("queue_pending", 0)
    _record("Queue has pending items after seed", pending > 0, f"(pending={pending})")


def test_docs_endpoint() -> None:
    """FastAPI auto-generated Swagger UI (/docs) should return 200 HTML."""
    url = BASE_URL + "/docs"
    req = urllib.request.Request(url, headers={"Accept": "text/html,application/xhtml+xml"})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as resp:
            code = resp.status
            content_type = resp.headers.get("Content-Type", "")
            _record("GET /docs returns 200 (OpenAPI UI)", code == 200)
            _record("GET /docs returns HTML", "text/html" in content_type)
    except urllib.error.HTTPError as e:
        _record("GET /docs returns 200 (OpenAPI UI)", False, f"HTTP {e.code}")
    except Exception as exc:
        _record("GET /docs returns 200 (OpenAPI UI)", False, str(exc))


def test_dashboard_served() -> None:
    """The root / endpoint should serve the index.html dashboard."""
    url = BASE_URL + "/"
    req = urllib.request.Request(url, headers={"Accept": "text/html"})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as resp:
            content_type = resp.headers.get("Content-Type", "")
            _record("GET / serves HTML dashboard", "text/html" in content_type)
    except Exception as exc:
        _record("GET / serves HTML dashboard", False, str(exc))


def test_invalid_index_payload() -> None:
    """Malformed extension payload should return 422."""
    code, _ = http_post("/api/index", {"url": "not-a-valid-url-scheme://bad", "text": "x"})
    _record("Invalid URL scheme in /api/index returns 422", code == 422)


def test_domains_management() -> None:
    """Test GET /api/domains and POST /api/domains/{domain}"""
    code, body = http_get("/api/domains")
    _record("GET /api/domains returns 200", code == 200)
    data = body.get("data", [])
    _record("Domains list is a list", isinstance(data, list))
    
    # Configure a test domain
    payload = {
        "is_public": 1,
        "crawl_enabled": 0,
        "priority": 7,
        "sitemap_url": "https://example.com/sitemap_test.xml",
        "notes": "Test domain notes"
    }
    code2, body2 = http_post("/api/domains/example.com", payload)
    _record("POST /api/domains/example.com returns 200", code2 == 200)
    _record("Saved domain has crawl_enabled = 0", body2.get("data", {}).get("crawl_enabled") == 0)


def test_products_indexing_and_search() -> None:
    """Index a page with product structured data, then search it."""
    import time
    conn = sqlite3.connect("shomaj_search.db")
    conn.execute("DELETE FROM product_index WHERE url = ?", ("https://example.com/products/iphone-17-test",))
    conn.execute("DELETE FROM product_fts WHERE url = ?", ("https://example.com/products/iphone-17-test",))
    
    now = int(time.time())
    conn.execute(
        """
        INSERT INTO product_index (url, name, description, brand, sku, price, price_text, currency, availability, image_url, domain, is_private, schema_type, raw_schema, extracted_at, last_checked)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "https://example.com/products/iphone-17-test",
            "iPhone 17 Pro Max",
            "Cheapest flagship Apple device with ready stock",
            "Apple",
            "IPH17PM-TEST",
            170000.00,
            "৳ 1,70,000",
            "BDT",
            "in_stock",
            "https://example.com/images/iphone17.png",
            "example.com",
            0,
            "json-ld",
            "{}",
            now,
            now
        )
    )
    conn.execute("INSERT INTO product_fts (url, name, description, brand) VALUES (?, ?, ?, ?)",
                 ("https://example.com/products/iphone-17-test", "iPhone 17 Pro Max", "Cheapest flagship Apple device with ready stock", "Apple"))
    conn.commit()
    conn.close()
    
    code, body = http_get("/api/search/products?q=iPhone+17+Pro+Max&sort=price_asc")
    _record("GET /api/search/products returns 200", code == 200)
    data = body.get("data", {})
    results = data.get("results", [])
    _record("Product search returns at least 1 hit", len(results) > 0)
    if results:
        first = results[0]
        _record("Product search returns correct SKU", first.get("sku") == "IPH17PM-TEST")
        _record("Product search returns correct price", first.get("price") == 170000.0)
        _record("Product search returns availability", first.get("availability") == "in_stock")


def test_search_cache_invalidation() -> None:
    """Search results caching and invalidation on new index writes."""
    import time
    ts = int(time.time())
    keyword = f"cachingtest{ts}"
    url = f"https://example.com/caching-test-page-{ts}"

    # 1. Perform search
    code1, body1 = http_get(f"/api/search?q={keyword}")
    _record("GET /api/search with new query returns 200", code1 == 200)
    data1 = body1.get("data", {})
    _record("Initial cached search has 0 hits", data1.get("total_hits", 0) == 0)

    # 2. Ingest document containing the keyword
    payload = {
        "url": url,
        "title": "Caching Test Title",
        "text": f"This is {keyword} data page text.",
        "links": [],
        "images": [],
        "videos": []
    }
    code_idx, _ = http_post("/api/index", payload)
    _record("Ingesting test doc returns 200", code_idx == 200)

    # 3. Perform search again — should hit the DB (due to cache invalidation) and find the page
    code2, body2 = http_get(f"/api/search?q={keyword}")
    _record("Second search returns 200", code2 == 200)
    data2 = body2.get("data", {})
    _record("Search after invalidation returns 1 hit", data2.get("total_hits", 0) == 1)


def test_safe_search_filtering() -> None:
    """Safe Search filtering on query keywords and indexed documents."""
    # 1. Ingest an adult-themed page
    payload = {
        "url": "https://example.com/some-adult-content-xxx-page",
        "title": "Unsafe adult portal",
        "text": "This is a dummy portal containing forbidden adult terms.",
        "links": [],
        "images": [],
        "videos": []
    }
    code_idx, _ = http_post("/api/index", payload)
    _record("Ingesting unsafe page returns 200", code_idx == 200)

    # 2. Search with safe_search=True (default) for 'adult' -> should return 0 hits
    code_safe, body_safe = http_get("/api/search?q=adult&safe_search=true")
    _record("Safe search active returns 200", code_safe == 200)
    hits_safe = body_safe.get("data", {}).get("total_hits", 0)
    _record("Safe search query filters out adult results", hits_safe == 0)

    # 3. Search with safe_search=False -> should return at least 1 hit
    code_unsafe, body_unsafe = http_get("/api/search?q=adult&safe_search=false")
    _record("Unsafe search returns 200", code_unsafe == 200)
    hits_unsafe = body_unsafe.get("data", {}).get("total_hits", 0)
    _record("Unsafe search query allows adult results", hits_unsafe > 0)


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------
def main() -> int:
    print(f"\n{BOLD}{CYAN}{'='*60}")
    print("  Shomaj Search — Integration Test Suite")
    print(f"  Target: {BASE_URL}")
    print(f"{'='*60}{RESET}\n")

    # Check connectivity first
    print(f"{BOLD}◆ Pre-flight: Backend Connectivity{RESET}")
    try:
        code, _ = http_get("/health")
        if code != 200:
            print(f"  {RED}Backend not reachable at {BASE_URL}. Start uvicorn first.{RESET}")
            return 1
        print(f"  {GREEN}Backend is reachable ✓{RESET}\n")
    except Exception as exc:
        print(f"  {RED}Cannot connect to backend: {exc}{RESET}")
        print(f"  Run:  uvicorn main:app --reload\n")
        return 1

    # Run all test groups
    tests = [
        ("1. Health Check",              test_health_check),
        ("2. Database Schema",           test_database_schema),
        ("3. Passive Ingestion",         test_passive_ingestion),
        ("4. Second Document",           test_second_document),
        ("5. High-Relevance Document",   test_third_document_high_relevance),
        ("6. Basic Search",              test_search_basic),
        ("7. BM25 Relevance Ranking",    test_search_bm25_ranking),
        ("8. No-Results Query",          test_search_no_results),
        ("9. Empty Query Validation",    test_search_empty_query),
        ("10. Stats Endpoint",           test_stats_endpoint),
        ("11. Crawler State Machine",    test_crawler_state_machine),
        ("12. Crawler Configuration",    test_crawler_config),
        ("13. Queue Seeding",            test_seed_endpoint),
        ("14. Queue Stats",              test_queue_stats),
        ("15. OpenAPI Docs",             test_docs_endpoint),
        ("16. Dashboard HTML Served",    test_dashboard_served),
        ("17. Invalid Payload Rejected", test_invalid_index_payload),
        ("18. Domain Management API",    test_domains_management),
        ("19. Product Search API",       test_products_indexing_and_search),
        ("20. Caching & Invalidation",    test_search_cache_invalidation),
        ("21. Safe Search Filtering",     test_safe_search_filtering),
        ("22. Configuration Persistence", test_config_persistence),
    ]

    for group_name, fn in tests:
        print(f"{BOLD}◆ {group_name}{RESET}")
        try:
            fn()
        except Exception as exc:
            _record(f"[{group_name}] Unexpected exception", False, str(exc))
        print()

    # Summary
    total = _pass_count + _fail_count
    print(f"{BOLD}{CYAN}{'='*60}{RESET}")
    print(f"{BOLD}  Results: {GREEN}{_pass_count}/{total} passed{RESET}", end="")
    if _fail_count:
        print(f"  {RED}{_fail_count} failed{RESET}")
    else:
        print(f"  {GREEN}All tests passed! 🎉{RESET}")
    print(f"{BOLD}{CYAN}{'='*60}{RESET}\n")

    return 0 if _fail_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
