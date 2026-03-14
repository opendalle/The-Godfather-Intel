"""
crawlers/firecrawl_client.py — Anti-bot bypass layer
Wraps Firecrawl API as a drop-in replacement for requests.Session.get()
for Cloudflare-protected and JS-rendered sites.

Blocked sources this solves:
  - 99acres, MagicBricks, Anarock, JLL, CBRE, Colliers, Knight Frank
  - ibapi.in, bankauctions.co.in, sarfaesi.com
  - drt.gov.in, nclt.gov.in, ibbi.gov.in, mca.gov.in
  - narcl.co.in, edelweissarc.com, arcil.com, phoenixarc.co.in
  - reuters.com, vccircle.com
"""
from __future__ import annotations
import os, time, logging, requests

logger = logging.getLogger("nexus.firecrawl")

FIRECRAWL_API_KEY = os.environ.get("FIRECRAWL_API_KEY", "")
FIRECRAWL_BASE    = "https://api.firecrawl.dev/v1"


class FirecrawlResponse:
    """Mimics requests.Response so callers use .text / .json() as usual."""
    def __init__(self, text: str, status_code: int = 200, url: str = ""):
        self.text        = text
        self.status_code = status_code
        self.url         = url
        self._json       = None

    def json(self):
        import json
        if self._json is None:
            self._json = json.loads(self.text)
        return self._json


class FirecrawlSession:
    """Session wrapper that routes requests through Firecrawl."""

    SCRAPE_ENDPOINT = f"{FIRECRAWL_BASE}/scrape"
    RATE_LIMIT_DELAY = 2.0   # seconds between Firecrawl calls

    def __init__(self):
        if not FIRECRAWL_API_KEY:
            logger.warning("FIRECRAWL_API_KEY not set — Firecrawl calls will fail")
        self._last_call = 0.0

    def get(self, url: str, **kwargs) -> FirecrawlResponse | None:
        """Fetch URL via Firecrawl. Returns FirecrawlResponse or None."""
        if not FIRECRAWL_API_KEY:
            return None
        # Rate limiting
        elapsed = time.time() - self._last_call
        if elapsed < self.RATE_LIMIT_DELAY:
            time.sleep(self.RATE_LIMIT_DELAY - elapsed)
        self._last_call = time.time()
        try:
            resp = requests.post(
                self.SCRAPE_ENDPOINT,
                headers={
                    "Authorization": f"Bearer {FIRECRAWL_API_KEY}",
                    "Content-Type":  "application/json",
                },
                json={
                    "url":     url,
                    "formats": ["markdown"],
                    "actions": [],
                },
                timeout=30,
            )
            if resp.status_code == 200:
                data = resp.json()
                content = data.get("data", {}).get("markdown", "") or data.get("data", {}).get("content", "")
                return FirecrawlResponse(text=content, status_code=200, url=url)
            logger.warning(f"Firecrawl {resp.status_code} for {url}")
            return None
        except Exception as e:
            logger.error(f"Firecrawl error for {url}: {e}")
            return None
