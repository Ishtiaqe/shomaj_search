/**
 * content.js — Shomaj Search Extension
 *
 * Injected into every http/https page at document_idle.
 *
 * Strategy:
 *  1. Wait 2500 ms after injection so that SPA frameworks, lazy loaders,
 *     and Infinite Scroll layers have time to populate the DOM.
 *  2. Extract the page URL, title, visible body text, and meaningful links.
 *  3. Send the payload to the background service worker via chrome.runtime.sendMessage.
 *     The background worker relays it to localhost:8000/api/index.
 *
 * Debounce guard: a module-level flag prevents re-execution if the script
 * is somehow injected multiple times into the same document.
 */

(function () {
  "use strict";

  // -------------------------------------------------------------------------
  // Guard: only run once per page lifetime
  // -------------------------------------------------------------------------
  if (window.__shomajIndexed) return;
  window.__shomajIndexed = true;

  // -------------------------------------------------------------------------
  // Configuration
  // -------------------------------------------------------------------------
  const DEBOUNCE_MS = 2500; // wait for async DOM mutations

  // -------------------------------------------------------------------------
  // Extraction logic (runs after debounce)
  // -------------------------------------------------------------------------
  function extractPageData() {
    const url   = window.location.href;
    const title = document.title || url;

    // -------------------------------------------------------------------
    // Text extraction
    // Clone body, remove noisy structural elements, then read innerText.
    // Using innerText (not textContent) gives us only *visible* text
    // because it respects CSS display:none and visibility:hidden.
    // -------------------------------------------------------------------
    let text = "";
    try {
      const bodyClone = document.body.cloneNode(true);

      // Strip noisy tags from the clone
      const noiseSelectors = [
        "script", "style", "noscript", "iframe",
        "nav", "header", "footer", "aside",
        "[aria-hidden='true']",
      ];
      noiseSelectors.forEach((sel) => {
        bodyClone.querySelectorAll(sel).forEach((el) => el.remove());
      });

      text = (bodyClone.innerText || bodyClone.textContent || "")
        .replace(/\s{2,}/g, " ")
        .trim()
        .slice(0, 200000); // 200 KB ceiling

    } catch (e) {
      // Fallback — use raw innerText without cloning
      text = (document.body.innerText || "").slice(0, 200000);
    }

    // -------------------------------------------------------------------
    // Link extraction
    // Collect hrefs from <a> elements that have non-empty anchor text.
    // We resolve relative URLs to absolute using the current page URL.
    // Deduplicated via a Set to avoid sending hundreds of identical links.
    // -------------------------------------------------------------------
    const linkSet = new Set();
    try {
      document.querySelectorAll("a[href]").forEach((anchor) => {
        const anchorText = (anchor.innerText || anchor.textContent || "").trim();
        if (!anchorText) return; // skip icon-only / empty links

        let href = anchor.href; // browser already resolves to absolute URL
        if (!href) return;

        // Only include http/https links
        if (!href.startsWith("http://") && !href.startsWith("https://")) return;

        // Strip fragment identifiers — we want canonical page URLs
        href = href.split("#")[0];
        if (href) linkSet.add(href);
      });
    } catch (e) {
      // Link extraction failure is non-critical — proceed without links
    }

    return {
      url,
      title,
      text,
      links: Array.from(linkSet).slice(0, 500), // cap at 500 links per page
    };
  }

  // -------------------------------------------------------------------------
  // Send to background service worker (which relays to localhost:8000)
  // -------------------------------------------------------------------------
  function sendToBackground(data) {
    try {
      chrome.runtime.sendMessage(
        { type: "SHOMAJ_INDEX", payload: data },
        (response) => {
          // Suppress "Extension context invalidated" errors on navigation
          if (chrome.runtime.lastError) return;
          // Optional: log success in dev mode
          // console.debug("[Shomaj] Indexed →", data.url, response);
        }
      );
    } catch (e) {
      // Extension context may be invalidated during hot reloads — fail silently
    }
  }

  // -------------------------------------------------------------------------
  // Debounced execution
  // -------------------------------------------------------------------------
  setTimeout(function () {
    try {
      const data = extractPageData();
      sendToBackground(data);
    } catch (e) {
      // Any extraction error must not affect user's browsing experience
    }
  }, DEBOUNCE_MS);

})();
