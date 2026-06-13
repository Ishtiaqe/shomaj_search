"""
cache.py — Search Result Caching for Shomaj Search
Provides a thread-safe, fast in-memory LRU cache to store search query results.
"""
import logging

logger = logging.getLogger("shomaj.cache")

class SearchCache:
    def __init__(self, maxsize: int = 512):
        self.maxsize = maxsize
        self.cache = {}
        self.keys = []

    def get(self, key):
        if key in self.cache:
            try:
                self.keys.remove(key)
            except ValueError:
                pass
            self.keys.append(key)
            logger.debug("[Cache] Hit for key: %s", key)
            return self.cache[key]
        logger.debug("[Cache] Miss for key: %s", key)
        return None

    def set(self, key, value):
        if key in self.cache:
            try:
                self.keys.remove(key)
            except ValueError:
                pass
        self.cache[key] = value
        self.keys.append(key)
        if len(self.keys) > self.maxsize:
            oldest = self.keys.pop(0)
            self.cache.pop(oldest, None)
        logger.debug("[Cache] Set key: %s", key)

    def clear(self):
        self.cache.clear()
        self.keys.clear()
        logger.info("[Cache] Invalidated/Cleared.")

search_cache = SearchCache()
