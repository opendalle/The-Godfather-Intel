"""
crawlers/base.py — Nexus Asia Intel unified base crawler
All crawlers inherit from BaseCrawler. Includes:
  - Firecrawl integration for bot-protected sites
  - Auto retry + backoff
  - Shared HEADERS, REQUEST_TIMEOUT, safe_get()
  - DistressEvent dataclass (supply-side)
  - DemandArticle dataclass (demand-side)
"""
from __future__ import annotations
import os, re, time, logging, requests
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

FIRECRAWL_API_KEY = os.environ.get("FIRECRAWL_API_KEY", "")


@dataclass
class DistressEvent:
    company_name:    str
    signal_keyword:  str
    signal_category: str
    source:          str
    url:             str
    headline:        str = ""
    snippet:         str = ""
    detected_at:     str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    published_at:    Optional[str] = None
    severity:        str = "medium"
    asset_class:     Optional[str] = None
    price_crore:     Optional[float] = None
    location:        Optional[str] = None
    is_mmr:          bool = False
    metadata:        dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "company_name":    self.company_name,
            "signal_keyword":  self.signal_keyword,
            "signal_category": self.signal_category,
            "source":          self.source,
            "url":             self.url,
            "headline":        self.headline[:500] if self.headline else "",
            "snippet":         self.snippet[:1000] if self.snippet else "",
            "detected_at":     self.detected_at,
            "published_at":    self.published_at,
            "severity":        self.severity,
            "asset_class":     self.asset_class,
            "price_crore":     self.price_crore,
            "location":        self.location,
            "is_mmr":          self.is_mmr,
            "metadata":        self.metadata,
        }


@dataclass
class DemandArticle:
    title:           str
    text:            str
    url:             str
    source:          str
    published:       Optional[str] = None
    company_hint:    Optional[str] = None
    location_hint:   Optional[str] = None
    signal_type_hint: Optional[str] = None
    metadata:        dict = field(default_factory=dict)


class BaseCrawler(ABC):
    SOURCE_NAME:     str = "unknown"
    SOURCE_URL:      str = ""
    CATEGORY:        str = "other"
    REQUEST_TIMEOUT: int = 20
    MAX_ARTICLES:    int = 50
    CRAWL_DELAY:     float = 1.5   # seconds between requests

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }

    def __init__(self):
        self.logger = logging.getLogger(f"crawler.{self.SOURCE_NAME}")
        self._session = self._build_session()
        self._firecrawl_session = None

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(self.HEADERS)
        return session

    def _get_firecrawl(self):
        if not self._firecrawl_session and FIRECRAWL_API_KEY:
            from crawlers.firecrawl_client import FirecrawlSession
            self._firecrawl_session = FirecrawlSession()
        return self._firecrawl_session

    @abstractmethod
    def crawl(self) -> list:
        """
        Returns list of DistressEvent.to_dict() or DemandArticle for demand crawlers.
        """
        ...

    def safe_get(self, url: str, use_firecrawl: bool = False, **kwargs) -> Optional[requests.Response]:
        """HTTP GET with timeout, error handling, optional Firecrawl bypass."""
        if use_firecrawl and FIRECRAWL_API_KEY:
            fc = self._get_firecrawl()
            if fc:
                return fc.get(url)
        timeout = kwargs.pop("timeout", self.REQUEST_TIMEOUT)
        try:
            time.sleep(self.CRAWL_DELAY)
            resp = self._session.get(url, timeout=timeout, **kwargs)
            if resp.status_code == 200:
                return resp
            self.logger.warning(f"HTTP {resp.status_code} for {url}")
            return None
        except Exception as e:
            self.logger.error(f"Request failed for {url[:80]}: {e}")
            return None

    def make_event(
        self,
        company_name: str,
        keyword: str,
        category: str,
        url: str,
        headline: str = "",
        snippet: str = "",
        published_at: str = None,
        metadata: dict = None,
        location: str = None,
        price_crore: float = None,
        asset_class: str = None,
    ) -> DistressEvent:
        from nlp.engine import get_severity, extract_price_crore, extract_location
        MMR_CITIES = {
            'mumbai', 'thane', 'navi mumbai', 'bkc', 'andheri', 'powai', 'malad',
            'goregaon', 'kurla', 'vikhroli', 'worli', 'lower parel', 'belapur',
            'airoli', 'kharghar', 'vashi', 'wadala', 'bhandup', 'mulund',
        }
        loc = location or extract_location(headline + " " + snippet)
        price = price_crore or extract_price_crore(headline + " " + snippet)
        is_mmr = bool(loc and any(m in loc.lower() for m in MMR_CITIES))
        return DistressEvent(
            company_name=company_name,
            signal_keyword=keyword,
            signal_category=category,
            source=self.SOURCE_NAME,
            url=url,
            headline=headline,
            snippet=snippet,
            published_at=published_at,
            severity=get_severity(category),
            location=loc,
            price_crore=price,
            is_mmr=is_mmr,
            asset_class=asset_class,
            metadata=metadata or {},
        )
