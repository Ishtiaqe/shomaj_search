/**
 * background.js — Shomaj Search Extension (Service Worker)
 *
 * Receives SHOMAJ_INDEX messages from content.js and relays them to
 * the local Shomaj Search backend at http://localhost:8000/api/index.
 *
 * Design principles:
 *  - Fire-and-forget: fetch is async; no await blocks the service worker.
 *  - Fail silently: if the local backend is unreachable, the user sees nothing.
 *  - No data is stored in the extension — it is a pure relay.
 *  - Exponential backoff retry: up to 2 retries on network failure.
 */

"use strict";

const BACKEND_URL   = "http://localhost:8000/api/index";
const MAX_RETRIES   = 2;
const RETRY_BASE_MS = 500;

/**
 * Sends payload to the Shomaj backend with optional retry logic.
 * @param {Object} payload  - { url, title, text, links }
 * @param {number} attempt  - Current attempt number (0-indexed)
 */
async function relayToBackend(payload, attempt = 0) {
  try {
    const response = await fetch(BACKEND_URL, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify(payload),
    });

    if (!response.ok) {
      // Server returned an error status — do not retry (backend is alive but rejected it)
      console.warn(
        `[Shomaj BG] Backend returned HTTP ${response.status} for ${payload.url}`
      );
    }
    // Success — no action needed (fire and forget)

  } catch (networkError) {
    // Network error: backend may be down or not yet started
    if (attempt < MAX_RETRIES) {
      const delay = RETRY_BASE_MS * Math.pow(2, attempt);
      // console.debug(`[Shomaj BG] Retry ${attempt + 1} in ${delay}ms for ${payload.url}`);
      await new Promise((resolve) => setTimeout(resolve, delay));
      return relayToBackend(payload, attempt + 1);
    }
    // Final failure — swallow silently, never surface to user
  }
}

/**
 * Validate and sanitise the payload before sending.
 * Guards against malformed messages from content.js.
 */
function sanitisePayload(payload) {
  if (!payload || typeof payload.url !== "string") return null;
  return {
    url:   String(payload.url).slice(0, 2048),
    title: String(payload.title  || "").slice(0, 512),
    text:  String(payload.text   || "").slice(0, 200000),
    links: Array.isArray(payload.links)
      ? payload.links.filter((l) => typeof l === "string").slice(0, 500)
      : [],
  };
}

// ---------------------------------------------------------------------------
// Message listener — receives from content.js
// ---------------------------------------------------------------------------
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message?.type !== "SHOMAJ_INDEX") return false;

  const payload = sanitisePayload(message.payload);
  if (!payload) {
    sendResponse({ ok: false, error: "Invalid payload" });
    return false;
  }

  // Relay asynchronously — do NOT block with await in the message handler
  relayToBackend(payload).catch(() => {});

  sendResponse({ ok: true });
  return false; // synchronous response — no need to keep channel open
});

// ---------------------------------------------------------------------------
// Extension install / update event — log for debugging
// ---------------------------------------------------------------------------
chrome.runtime.onInstalled.addListener((details) => {
  if (details.reason === "install") {
    console.log("[Shomaj] Extension installed. Backend URL:", BACKEND_URL);
  } else if (details.reason === "update") {
    console.log("[Shomaj] Extension updated to version", chrome.runtime.getManifest().version);
  }
});
