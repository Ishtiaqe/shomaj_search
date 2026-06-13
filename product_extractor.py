"""
product_extractor.py — Shomaj Search
Extracts structured product metadata from HTML pages.

Priority chain (stops at first success):
  1. JSON-LD  (schema.org/Product)           — most reliable, structured
  2. OpenGraph product tags                  — common in Shopify / WooCommerce
  3. HTML <meta> product tags                — Twitter cards, custom implementations
  4. HTML Microdata (itemtype=schema.org)    — older structured markup
  5. Heuristic CSS / text extraction         — last resort, best-effort

No local media is stored. image_url is a remote reference only.
"""

import json
import re
from dataclasses import dataclass, field
from typing import Optional

from bs4 import BeautifulSoup, Tag

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Mapping of raw availability strings → normalized internal values.
# Covers schema.org URIs, OpenGraph strings, common plain text.
_AVAILABILITY_MAP: dict[str, str] = {
    # schema.org URIs (lower-cased)
    "https://schema.org/instock":              "in_stock",
    "http://schema.org/instock":               "in_stock",
    "https://schema.org/limitedavailability":  "in_stock",
    "http://schema.org/limitedavailability":   "in_stock",
    "https://schema.org/onlineonly":           "in_stock",
    "https://schema.org/outofstock":           "out_of_stock",
    "http://schema.org/outofstock":            "out_of_stock",
    "https://schema.org/soldout":              "out_of_stock",
    "https://schema.org/preorder":             "preorder",
    "http://schema.org/preorder":              "preorder",
    "https://schema.org/presale":              "preorder",
    "https://schema.org/discontinued":         "discontinued",
    "http://schema.org/discontinued":          "discontinued",
    # Short-form
    "instock":       "in_stock",
    "in_stock":      "in_stock",
    "in stock":      "in_stock",
    "in-stock":      "in_stock",
    "available":     "in_stock",
    "yes":           "in_stock",
    "outofstock":    "out_of_stock",
    "out_of_stock":  "out_of_stock",
    "out of stock":  "out_of_stock",
    "out-of-stock":  "out_of_stock",
    "soldout":       "out_of_stock",
    "sold out":      "out_of_stock",
    "sold-out":      "out_of_stock",
    "no":            "out_of_stock",
    "oos":           "out_of_stock",
    "unavailable":   "out_of_stock",
    "preorder":      "preorder",
    "pre-order":     "preorder",
    "pre order":     "preorder",
    "presale":       "preorder",
    "pre-sale":      "preorder",
    "coming soon":   "preorder",
    "comingsoon":    "preorder",
    "pending":       "preorder",
    "backorder":     "preorder",
    "back order":    "preorder",
    "discontinued":  "discontinued",
}

# Ranked availability for sorting (lower index = higher priority in results)
AVAILABILITY_RANK = {
    "in_stock":     0,
    "preorder":     1,
    "out_of_stock": 2,
    "discontinued": 3,
    "unknown":      4,
}

# Common CSS selectors for price elements (tried in order)
_PRICE_SELECTORS = [
    "[itemprop='price']",
    "[class*='product-price']",
    "[class*='sale-price']",
    "[class*='current-price']",
    "[class*='regular-price']",
    "[class*='offer-price']",
    "[class*='woocommerce-Price-amount']",
    "[class*='price']",
    "[id*='price']",
]

# Price pattern: matches ৳, Tk, BDT, $, €, £ followed by digits
_PRICE_PATTERN = re.compile(
    r"(?:৳|Tk\.?\s*|BDT\s*|\$|€|£|USD\s*|EUR\s*)?"
    r"(\d[\d,.\s]*\d|\d+)"
    r"(?:\s*(?:৳|BDT|USD|EUR|GBP|Tk\.?))?",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ProductData:
    """Structured product data extracted from a web page."""
    name:          str            = ""
    description:   str            = ""
    brand:         str            = ""
    sku:           str            = ""
    price:         Optional[float] = None    # numeric (BDT or detected currency)
    price_text:    str            = ""       # original formatted string, e.g. "৳ 1,20,000"
    currency:      str            = ""       # "BDT", "USD", etc.
    availability:  str            = "unknown"
    image_url:     str            = ""
    schema_type:   str            = ""       # source of extraction
    raw_schema:    str            = ""       # JSON dump of raw extracted schema
    is_product_page: bool         = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize_availability(raw: str) -> str:
    """Normalises any availability string to an internal canonical value."""
    if not raw:
        return "unknown"
    key = raw.strip().lower()
    # Exact match
    if key in _AVAILABILITY_MAP:
        return _AVAILABILITY_MAP[key]
    # Partial match (some schemas append path suffixes)
    for known, norm in _AVAILABILITY_MAP.items():
        if key.endswith(known):
            return norm
    return "unknown"


def parse_price(raw: str, hint_currency: str = "BDT") -> tuple[Optional[float], str, str]:
    """
    Parses a price string into (numeric_float, display_text, currency_code).

    Args:
        raw:           Raw price string, e.g. "৳ 1,20,000" or "185000.00"
        hint_currency: Fallback currency when none can be detected from the string.

    Returns:
        (numeric_price or None, original_display_text, currency_code)
    """
    if not raw:
        return None, "", ""

    text = raw.strip()

    # Detect currency symbol in text
    currency = ""
    if any(c in text for c in ("৳", "Tk", "BDT")):
        currency = "BDT"
    elif "$" in text or "USD" in text:
        currency = "USD"
    elif "€" in text or "EUR" in text:
        currency = "EUR"
    elif "£" in text or "GBP" in text:
        currency = "GBP"
    else:
        currency = hint_currency

    # Strip everything except digits and decimal points
    cleaned = re.sub(r"[^\d.]", "", text.replace(",", ""))
    # Remove trailing/leading dots
    cleaned = cleaned.strip(".")

    match = re.search(r"\d+(?:\.\d+)?", cleaned)
    if match:
        try:
            numeric = float(match.group())
            # Sanity: ignore prices > 100 million (likely CMS IDs)
            if numeric > 100_000_000:
                return None, text, currency
            return numeric, text, currency
        except ValueError:
            pass

    return None, text, currency


def _first_str(*values: str) -> str:
    """Returns the first non-empty string."""
    for v in values:
        s = str(v).strip()
        if s:
            return s
    return ""


def _meta_content(soup: BeautifulSoup, *props: str) -> str:
    """Reads the content of a <meta> tag by property or name."""
    for prop in props:
        tag = (
            soup.find("meta", property=prop)
            or soup.find("meta", attrs={"name": prop})
        )
        if tag and isinstance(tag, Tag):
            return str(tag.get("content", "")).strip()
    return ""


# ---------------------------------------------------------------------------
# ProductExtractor
# ---------------------------------------------------------------------------

class ProductExtractor:
    """
    Stateless product data extractor.
    Call .extract(html, url) → ProductData for each page.
    """

    def extract(self, html: str, url: str) -> ProductData:
        """
        Main entry point. Tries extraction strategies in priority order.
        Returns a ProductData; is_product_page=False if no product detected.
        """
        soup = BeautifulSoup(html, "html.parser")
        result = ProductData()

        for strategy, schema_label in [
            (self._from_json_ld,    "json-ld"),
            (self._from_opengraph,  "opengraph"),
            (self._from_meta_tags,  "meta"),
            (self._from_microdata,  "microdata"),
            (self._from_heuristic,  "heuristic"),
        ]:
            if strategy(soup, result, url):
                result.schema_type    = schema_label
                result.is_product_page = True
                # Truncate long fields
                result.name        = result.name[:512]
                result.description = result.description[:2000]
                result.brand       = result.brand[:256]
                result.sku         = result.sku[:128]
                result.image_url   = result.image_url[:1024]
                return result

        return result

    # -----------------------------------------------------------------------
    # Strategy 1: JSON-LD
    # -----------------------------------------------------------------------

    def _from_json_ld(self, soup: BeautifulSoup, result: ProductData, url: str) -> bool:
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                raw = script.string or ""
                data = json.loads(raw)
            except (json.JSONDecodeError, AttributeError):
                continue

            # Normalise to a flat list of schema objects
            items: list[dict] = []
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                items = data.get("@graph", [data])

            for item in items:
                if not isinstance(item, dict):
                    continue
                schema_type = item.get("@type", "")
                if isinstance(schema_type, list):
                    schema_type = " ".join(schema_type)
                if "product" in str(schema_type).lower():
                    if self._parse_product_schema(item, result):
                        return True
        return False

    def _parse_product_schema(self, data: dict, result: ProductData) -> bool:
        result.name        = _first_str(str(data.get("name", "")))
        result.description = _first_str(str(data.get("description", "")))
        result.sku         = _first_str(str(data.get("sku", "")),
                                        str(data.get("mpn", "")),
                                        str(data.get("gtin", "")))

        brand = data.get("brand", {})
        if isinstance(brand, dict):
            result.brand = _first_str(str(brand.get("name", "")))
        elif isinstance(brand, str):
            result.brand = brand

        image = data.get("image", "")
        if isinstance(image, list) and image:
            image = image[0]
        if isinstance(image, dict):
            image = image.get("url", "")
        result.image_url = str(image)

        # Offers — can be Offer, AggregateOffer, or list
        offers = data.get("offers", {})
        if isinstance(offers, list):
            self._pick_best_offer(offers, result)
        elif isinstance(offers, dict):
            self._apply_offer(offers, result)
        elif "lowPrice" in data or "price" in data:
            # Top-level price (some schemas put it directly on the Product)
            price_raw = _first_str(str(data.get("price", "")),
                                   str(data.get("lowPrice", "")))
            result.price, result.price_text, result.currency = parse_price(price_raw)
            result.currency = result.currency or str(data.get("priceCurrency", ""))

        result.raw_schema = json.dumps(data, ensure_ascii=False)[:4000]
        return bool(result.name)

    def _apply_offer(self, offer: dict, result: ProductData) -> None:
        price_raw = _first_str(str(offer.get("price", "")),
                               str(offer.get("lowPrice", "")))
        p, pt, pc = parse_price(price_raw)
        result.price       = p
        result.price_text  = pt
        result.currency    = pc or str(offer.get("priceCurrency", ""))
        result.availability = normalize_availability(
            str(offer.get("availability", ""))
        )

    def _pick_best_offer(self, offers: list[dict], result: ProductData) -> None:
        """
        From a list of offers, picks the cheapest in-stock one.
        Falls back to the first offer with a valid price.
        """
        best: Optional[dict] = None
        best_price: float = float("inf")

        for offer in offers:
            if not isinstance(offer, dict):
                continue
            avail = normalize_availability(str(offer.get("availability", "")))
            price_raw = _first_str(str(offer.get("price", "")),
                                   str(offer.get("lowPrice", "")))
            p, _, _ = parse_price(price_raw)
            if p is None:
                continue
            if avail == "in_stock" and p < best_price:
                best_price = p
                best = offer
            elif best is None:
                best = offer

        if best:
            self._apply_offer(best, result)
        elif offers and isinstance(offers[0], dict):
            self._apply_offer(offers[0], result)

    # -----------------------------------------------------------------------
    # Strategy 2: OpenGraph
    # -----------------------------------------------------------------------

    def _from_opengraph(self, soup: BeautifulSoup, result: ProductData, url: str) -> bool:
        og_type = _meta_content(soup, "og:type")
        if "product" not in og_type.lower():
            return False

        result.name        = _meta_content(soup, "og:title", "og:product:title")
        result.description = _meta_content(soup, "og:description")
        result.image_url   = _meta_content(soup, "og:image")
        result.brand       = _meta_content(soup, "og:brand", "og:product:brand", "product:brand")

        price_raw = _meta_content(soup,
            "product:price:amount", "og:price:amount",
            "product:amount",       "twitter:data1",
        )
        currency  = _meta_content(soup,
            "product:price:currency", "og:price:currency", "product:currency"
        )
        result.price, result.price_text, detected_currency = parse_price(price_raw)
        result.currency = currency or detected_currency

        avail_raw = _meta_content(soup, "product:availability", "og:availability")
        result.availability = normalize_availability(avail_raw)

        return bool(result.name)

    # -----------------------------------------------------------------------
    # Strategy 3: <meta> product tags
    # -----------------------------------------------------------------------

    def _from_meta_tags(self, soup: BeautifulSoup, result: ProductData, url: str) -> bool:
        price_raw = _meta_content(soup,
            "product:price:amount", "price", "twitter:data1"
        )
        if not price_raw:
            return False

        result.price, result.price_text, result.currency = parse_price(price_raw)
        if result.price is None:
            return False

        result.currency    = result.currency or _meta_content(soup, "product:price:currency", "currency")
        result.availability = normalize_availability(
            _meta_content(soup, "product:availability", "availability")
        )
        result.name        = _meta_content(soup, "product:name", "product_name")
        result.description = _meta_content(soup, "description", "product:description")
        result.image_url   = _meta_content(soup, "image", "product:image")
        result.brand       = _meta_content(soup, "product:brand", "brand")

        if not result.name:
            t = soup.find("title")
            result.name = t.get_text(strip=True) if t else ""

        return True

    # -----------------------------------------------------------------------
    # Strategy 4: HTML Microdata
    # -----------------------------------------------------------------------

    def _from_microdata(self, soup: BeautifulSoup, result: ProductData, url: str) -> bool:
        product_el = soup.find(
            attrs={"itemtype": re.compile(r"schema\.org/Product", re.I)}
        )
        if not product_el or not isinstance(product_el, Tag):
            return False

        def prop(name: str) -> str:
            el = product_el.find(attrs={"itemprop": name})  # type: ignore[union-attr]
            if not el or not isinstance(el, Tag):
                return ""
            return (el.get("content") or el.get_text(strip=True)).strip()

        result.name        = prop("name")
        result.description = prop("description")
        result.brand       = prop("brand")
        result.sku         = prop("sku")
        result.image_url   = prop("image")

        price_raw = prop("price")
        result.price, result.price_text, result.currency = parse_price(price_raw)
        result.currency = result.currency or prop("priceCurrency")
        result.availability = normalize_availability(prop("availability"))

        return bool(result.name)

    # -----------------------------------------------------------------------
    # Strategy 5: Heuristic (CSS selectors + text patterns)
    # -----------------------------------------------------------------------

    def _from_heuristic(self, soup: BeautifulSoup, result: ProductData, url: str) -> bool:
        """
        Best-effort extraction for pages without any structured data.
        Declares success only if we can find both a title and a price.
        """
        # Product URL heuristics
        product_url_hints = [
            r"/product[s]?/", r"/item[s]?/", r"/p/[^/]+$",
            r"/shop/[^/]+$", r"[?&]product_?id=", r"\.html$",
            r"/dp/[A-Z0-9]{10}", r"/sku/",
        ]
        likely_product = any(re.search(p, url, re.I) for p in product_url_hints)

        # Page title / h1
        h1 = soup.find("h1")
        result.name = h1.get_text(strip=True) if h1 else ""
        if not result.name:
            t = soup.find("title")
            result.name = t.get_text(strip=True) if t else ""

        # Price via CSS selectors
        for selector in _PRICE_SELECTORS:
            try:
                el = soup.select_one(selector)
                if el:
                    text = el.get_text(strip=True)
                    if re.search(r"\d", text):
                        p, pt, pc = parse_price(text)
                        if p is not None and p > 0:
                            result.price      = p
                            result.price_text = pt
                            result.currency   = pc
                            break
            except Exception:
                continue

        # Availability via text scan
        page_text = soup.get_text(separator=" ").lower()
        avail_patterns = [
            (r"\bin[\s-]?stock\b",      "in_stock"),
            (r"\bavailable\b",          "in_stock"),
            (r"\bout[\s-]?of[\s-]?stock\b", "out_of_stock"),
            (r"\bsold[\s-]?out\b",      "out_of_stock"),
            (r"\bpre[\s-]?order\b",     "preorder"),
            (r"\bcoming[\s-]?soon\b",   "preorder"),
        ]
        for pattern, avail in avail_patterns:
            if re.search(pattern, page_text, re.I):
                result.availability = avail
                break

        # OG image as fallback
        og = soup.find("meta", property="og:image")
        if og and isinstance(og, Tag):
            result.image_url = str(og.get("content", ""))

        # Brand from common tags
        result.brand = _meta_content(soup, "og:site_name", "author")

        # Only claim success if we have a name AND a price (or URL strongly suggests product)
        return bool(result.name) and (result.price is not None or likely_product)
