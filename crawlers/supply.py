"""
crawlers/ibapi_auction.py — PATCHED
crawlers/psu_banks.py — PATCHED

Root causes:
- IBAPI: Returns empty response (JSON parse error: empty body) → API endpoint changed
- PSU Banks: Firecrawl timeout on PNB (30s) + 500 on Canara → use direct HTTP scraping

Fixes:
1. IBAPI: Try multiple known endpoints; parse both old JSON format and new format
2. PSU Banks: Replace Firecrawl with direct requests + BeautifulSoup for e-auction pages
   PNB, Canara, SBI, Bank of Baroda, Union Bank all have public e-auction pages
3. Add BankAuctions.co.in proper HTML scraper (was returning 0 — parsing was broken)
4. NARCL: Replace Firecrawl with direct scrape (Firecrawl was timing out)
"""

import re
import time
import logging
import hashlib
from datetime import datetime, timezone, timedelta
from typing import List, Optional
import requests
from bs4 import BeautifulSoup

from crawlers.base import BaseCrawler, DistressEvent
# DRTSARFAESICrawler lives in demand.py — re-export for main.py
from crawlers.demand import DRTSARFAESICrawler  # noqa: F401

logger_ibapi = logging.getLogger("crawler.IBAPI")
logger_psu   = logging.getLogger("crawler.PSU_Banks")
logger_narcl = logging.getLogger("crawler.NARCL_ARC")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
}

HEADERS_JSON = {**HEADERS, "Accept": "application/json, text/javascript, */*;q=0.01"}


def _make_uid(company: str, ref: str) -> str:
    raw = f"{company.lower().strip()}::{ref.lower().strip()[:100]}"
    return hashlib.md5(raw.encode()).hexdigest()


# ──────────────────────────────────────────────────────────────────────────────
# IBAPI AUCTION CRAWLER — PATCHED
# ──────────────────────────────────────────────────────────────────────────────

# IBAPI has multiple endpoints — try all of them
IBAPI_ENDPOINTS = [
    # v1 API (original — now returning empty)
    "https://www.ibapi.in/auction/search-all-auctions",
    # v2 API (new format as of 2025)
    "https://www.ibapi.in/api/v2/auctions/search",
    # Public search page scrape fallback
    "https://www.ibapi.in/",
]

# Alternative: e-Auction India portal (aggregates bank auctions)
EAUCTION_INDIA_URL = "https://www.eauctionindia.com/AuctionList.aspx"

# Bank auction aggregators as IBAPI fallback
BANK_AUCTION_AGGREGATORS = [
    ("https://www.bankauctions.co.in/search-property", "BankAuctions"),
    ("https://ibapi.in/", "IBAPI"),
]


class IBAPIAuctionCrawler:
    """
    Crawls IBAPI + fallback sources for bank property auctions.
    Patched to handle API format change (was returning empty JSON body).
    """

    def __init__(self, session: requests.Session = None):
        self.sess = session or requests.Session()
        self.sess.headers.update(HEADERS)

    def crawl(self) -> list:
        """Alias — main.py calls .crawl()."""
        return self.run()

    def run(self) -> List[dict]:
        events = []

        # Method 1: Try IBAPI API endpoints
        for endpoint in IBAPI_ENDPOINTS[:-1]:  # Skip HTML page for API attempts
            result = self._try_api(endpoint)
            if result:
                events.extend(result)
                logger_ibapi.info("IBAPI API: %d auctions from %s", len(result), endpoint)
                break

        # Method 2: Scrape IBAPI HTML if API failed
        if not events:
            events = self._scrape_ibapi_html()

        # Method 3: BankAuctions.co.in scrape
        if not events:
            events = self._scrape_bankauctions()

        logger_ibapi.info("IBAPI: %d auctions found", len(events))
        return events

    def _try_api(self, url: str) -> List[dict]:
        try:
            payload = {
                "property_type": "commercial",
                "page": 1,
                "per_page": 50,
            }
            r = self.sess.post(url, json=payload, headers=HEADERS_JSON, timeout=15)
            if r.status_code == 200 and r.content:
                data = r.json()
                auctions = data if isinstance(data, list) else data.get("auctions", data.get("data", []))
                return [self._normalize_ibapi(a) for a in auctions if self._is_commercial(a)]
        except Exception as e:
            logger_ibapi.debug("IBAPI API attempt failed %s: %s", url, e)
        return []

    def _scrape_ibapi_html(self) -> List[dict]:
        try:
            r = self.sess.get("https://www.ibapi.in/", timeout=15)
            if r.status_code != 200:
                return []
            soup = BeautifulSoup(r.content, "lxml")
            events = []
            # IBAPI HTML has auction cards — look for property listings
            cards = soup.select(".auction-card, .property-card, .listing-item, [class*='auction']")
            for card in cards[:50]:
                title = card.get_text(separator=" ", strip=True)[:200]
                link = card.find("a")
                href = link["href"] if link and link.get("href") else "#"
                if not self._is_commercial_text(title):
                    continue
                events.append(self._make_supply_event(
                    company="Bank Auction",
                    headline=title[:300],
                    channel="bank_auction",
                    source="IBAPI",
                    url=f"https://www.ibapi.in{href}" if href.startswith("/") else href,
                ))
            return events
        except Exception as e:
            logger_ibapi.warning("IBAPI HTML scrape failed: %s", e)
        return []

    def _scrape_bankauctions(self) -> List[dict]:
        try:
            r = self.sess.get(
                "https://www.bankauctions.co.in/search-property?category=commercial",
                timeout=15,
            )
            if r.status_code != 200:
                return []
            soup = BeautifulSoup(r.content, "lxml")
            events = []
            rows = soup.select("table tr, .property-row, .auction-item")
            for row in rows[1:30]:
                text = row.get_text(separator=" ", strip=True)
                if len(text) < 20:
                    continue
                link = row.find("a")
                href = link["href"] if link and link.get("href") else "#"
                events.append(self._make_supply_event(
                    company=text[:50].split(" ")[0],
                    headline=text[:300],
                    channel="bank_auction",
                    source="BankAuctions.co.in",
                    url=f"https://www.bankauctions.co.in{href}" if href.startswith("/") else href,
                ))
            return events
        except Exception as e:
            logger_ibapi.warning("BankAuctions scrape failed: %s", e)
        return []

    def _is_commercial(self, auction: dict) -> bool:
        ptype = str(auction.get("property_type") or auction.get("type") or "").lower()
        return ptype in ("commercial", "office", "industrial", "it_park", "") or not ptype

    def _is_commercial_text(self, text: str) -> bool:
        return bool(re.search(
            r"\b(?:commercial|office|industrial|factory|godown|warehouse|shop|showroom)\b",
            text, re.I,
        ))

    def _normalize_ibapi(self, a: dict) -> dict:
        company = a.get("bank_name") or a.get("borrower") or "Bank Auction"
        headline = (
            f"{a.get('property_type','Property')} auction — "
            f"{a.get('city','') or a.get('location','')} — "
            f"Reserve ₹{a.get('reserve_price','') or a.get('price','')}"
        )
        return self._make_supply_event(
            company=company,
            headline=headline[:400],
            channel="bank_auction",
            source="IBAPI",
            url=a.get("url") or a.get("link") or "#",
            price_crore=self._parse_price(a.get("reserve_price") or a.get("price") or 0),
            location=a.get("city") or a.get("location") or "",
        )

    def _parse_price(self, val) -> Optional[float]:
        if not val:
            return None
        try:
            return float(str(val).replace(",", "").replace("₹", "").strip()) / 10000000
        except Exception:
            return None

    def _make_supply_event(self, company: str, headline: str, channel: str,
                            source: str, url: str, price_crore: float = None,
                            location: str = "") -> dict:
        return {
            "company_name": company,
            "headline": headline,
            "channel": channel,
            "source": source,
            "url": url,
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "deal_score": 70,
            "severity": "high",
            "asset_class": "commercial",
            "location": location or "India",
            "is_mmr": location.lower() in ("mumbai", "bkc", "andheri", "thane", "navi mumbai", "mmr"),
            "price_crore": price_crore,
            "is_duplicate": False,
            "uid": _make_uid(company, headline),
        }


# ──────────────────────────────────────────────────────────────────────────────
# PSU BANK CRAWLER — PATCHED (no Firecrawl dependency)
# ──────────────────────────────────────────────────────────────────────────────

PSU_BANK_PAGES = [
    # SBI e-Auctions
    ("https://sbi.co.in/web/personal-banking/loans/e-auction-of-properties", "SBI"),
    ("https://bank.sbi/web/personal-banking/loans/e-auction-of-properties", "SBI"),
    # PNB — direct scrape instead of Firecrawl (was timing out)
    ("https://www.pnbindia.in/e-auction.html", "PNB"),
    # Bank of Baroda
    ("https://www.bankofbaroda.in/e-auction", "Bank of Baroda"),
    # Canara Bank — was returning 500 via Firecrawl
    ("https://canarabank.com/English/Scripts/eAuction.aspx", "Canara Bank"),
    ("https://canarabank.com/e-auction", "Canara Bank"),
    # Union Bank
    ("https://www.unionbankofindia.co.in/english/e-auction.aspx", "Union Bank"),
    # Indian Bank
    ("https://www.indianbank.in/e-auction/", "Indian Bank"),
    # Bank of India
    ("https://bankofindia.co.in/e-auction", "Bank of India"),
    # IDBI Bank
    ("https://www.idbibank.in/e-auction.aspx", "IDBI Bank"),
]

# Auction keywords to identify relevant listings
AUCTION_KEYWORDS = re.compile(
    r"(?:e-auction|auction\s+(?:notice|date|schedule)|"
    r"sarfaesi|section\s+13|possession\s+notice|"
    r"reserve\s+price|bid\s+amount|"
    r"commercial\s+(?:property|premises|space|complex)|"
    r"office\s+(?:space|premises|floor|building)|"
    r"industrial\s+(?:property|land|shed|gala|godown)|"
    r"factory|warehouse|shop\s+cum\s+office)",
    re.IGNORECASE,
)


class MultiPSUBankCrawler:
    """
    Crawls PSU bank e-auction pages directly (no Firecrawl).
    Uses requests + BeautifulSoup with proper timeouts.
    """

    def __init__(self, session: requests.Session = None):
        self.sess = session or requests.Session()
        self.sess.headers.update(HEADERS)
        self.sess.verify = False  # Some bank sites have cert issues
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def crawl(self) -> list:
        """Alias — main.py calls .crawl()."""
        return self.run()

    def run(self) -> List[dict]:
        events = []
        for page_url, bank_name in PSU_BANK_PAGES:
            try:
                result = self._scrape_bank_page(page_url, bank_name)
                events.extend(result)
                if result:
                    logger_psu.info("%s: %d auction notices", bank_name, len(result))
                time.sleep(1.0)  # Polite crawling
            except Exception as e:
                logger_psu.warning("%s scrape error: %s", bank_name, str(e)[:80])

        # Deduplicate
        seen = set()
        unique = []
        for e in events:
            if e["uid"] not in seen:
                seen.add(e["uid"])
                unique.append(e)

        logger_psu.info("PSU Banks: %d auction notices found", len(unique))
        return unique

    def _scrape_bank_page(self, url: str, bank_name: str) -> List[dict]:
        events = []
        try:
            r = self.sess.get(url, timeout=20)  # Increased from 30 to avoid partial hangs
            if r.status_code not in (200, 301, 302):
                logger_psu.warning("%s returned %d", bank_name, r.status_code)
                return []

            soup = BeautifulSoup(r.content, "lxml")

            # Extract auction notices from tables, lists, or divs
            # Try table rows first
            tables = soup.find_all("table")
            for table in tables:
                rows = table.find_all("tr")
                for row in rows:
                    text = row.get_text(separator=" ", strip=True)
                    if len(text) < 30:
                        continue
                    if not AUCTION_KEYWORDS.search(text):
                        continue
                    link = row.find("a")
                    href = link["href"] if link and link.get("href") else "#"
                    if href.startswith("/"):
                        from urllib.parse import urlparse
                        base = urlparse(url)
                        href = f"{base.scheme}://{base.netloc}{href}"

                    events.append(self._make_event(
                        bank=bank_name,
                        text=text[:400],
                        url=href,
                    ))

            # Also try PDF links / direct download links to auction notices
            if not events:
                links = soup.find_all("a", href=True)
                for link in links:
                    href = link["href"]
                    text = link.get_text(strip=True)
                    combined = f"{text} {href}"
                    if AUCTION_KEYWORDS.search(combined):
                        if href.startswith("/"):
                            from urllib.parse import urlparse
                            base = urlparse(url)
                            href = f"{base.scheme}://{base.netloc}{href}"
                        events.append(self._make_event(
                            bank=bank_name,
                            text=text[:400] or f"Auction notice — {bank_name}",
                            url=href,
                        ))

        except requests.Timeout:
            logger_psu.warning("%s timed out — skipping this run", bank_name)
        except Exception as e:
            logger_psu.warning("%s error: %s", bank_name, str(e)[:80])

        return events[:10]  # Cap per bank to avoid noise

    def _make_event(self, bank: str, text: str, url: str) -> dict:
        # Extract location from text
        from nlp.engine import extract_location
        location = extract_location(text)

        # Extract price
        price = None
        pm = re.search(r"(?:rs\.?\s*|₹)([\d,]+(?:\.\d+)?)\s*(?:cr(?:ore)?|lakh)", text, re.I)
        if pm:
            try:
                val = float(pm.group(1).replace(",", ""))
                unit = pm.group(0).split()[-1].lower()
                price = val if "cr" in unit else val / 100
            except Exception:
                pass

        return {
            "company_name": bank,
            "headline": f"{bank}: {text[:300]}",
            "channel": "bank_auction",
            "source": "PSU_Banks",
            "url": url,
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "deal_score": 75,
            "severity": "high",
            "asset_class": "commercial",
            "location": location,
            "is_mmr": location == "Mumbai",
            "price_crore": price,
            "is_duplicate": False,
            "uid": _make_uid(bank, text[:80]),
        }


# ──────────────────────────────────────────────────────────────────────────────
# NARCL / ARC CRAWLER — PATCHED (no Firecrawl)
# ──────────────────────────────────────────────────────────────────────────────

ARC_SOURCES = [
    ("https://narcl.co.in/portfolio", "NARCL"),
    ("https://narcl.co.in/auctions", "NARCL"),
    ("https://www.edelweissarc.com/portfolio", "Edelweiss ARC"),
    ("https://www.edelweissarc.com/auctions", "Edelweiss ARC"),
    ("https://www.arcil.com/assets", "ARCIL"),
    ("https://www.jmarcfinancial.com/auctions", "JM ARC"),
    ("https://indiabullarc.com/auctions", "Indiabulls ARC"),
    ("https://www.phoenixarc.co.in/auctions", "Phoenix ARC"),
]

ARC_KEYWORDS = re.compile(
    r"(?:commercial|office|industrial|property|asset|portfolio|"
    r"auction|npa|resolution|sarfaesi|possession|"
    r"sqft|square\s+feet|acre|lakh\s+sq)",
    re.IGNORECASE,
)


class NARCLARCCrawler:
    """
    Crawls NARCL and ARC websites directly without Firecrawl.
    Firecrawl was timing out on these sites.
    """

    def __init__(self, session: requests.Session = None):
        self.sess = session or requests.Session()
        self.sess.headers.update(HEADERS)
        self.sess.verify = False
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def crawl(self) -> list:
        """Alias — main.py calls .crawl()."""
        return self.run()

    def run(self) -> List[dict]:
        events = []
        seen = set()

        for url, source_name in ARC_SOURCES:
            try:
                r = self.sess.get(url, timeout=25)
                if r.status_code not in (200, 301, 302):
                    continue
                soup = BeautifulSoup(r.content, "lxml")

                # Look for asset/portfolio listings
                items = soup.select(
                    ".asset-card, .portfolio-item, .auction-item, "
                    "table tr, .property-row, article, .card"
                )
                for item in items[:20]:
                    text = item.get_text(separator=" ", strip=True)
                    if len(text) < 20:
                        continue
                    if not ARC_KEYWORDS.search(text):
                        continue

                    link = item.find("a")
                    href = link["href"] if link and link.get("href") else "#"
                    if href.startswith("/"):
                        from urllib.parse import urlparse
                        base = urlparse(url)
                        href = f"{base.scheme}://{base.netloc}{href}"

                    uid = _make_uid(source_name, text[:80])
                    if uid in seen:
                        continue
                    seen.add(uid)

                    from nlp.engine import extract_location
                    location = extract_location(text)

                    events.append({
                        "company_name": source_name,
                        "headline": f"{source_name}: {text[:300]}",
                        "channel": "arc_portfolio",
                        "source": "NARCL_ARC",
                        "url": href,
                        "detected_at": datetime.now(timezone.utc).isoformat(),
                        "deal_score": 68,
                        "severity": "high",
                        "asset_class": "commercial",
                        "location": location,
                        "is_mmr": location == "Mumbai",
                        "price_crore": None,
                        "is_duplicate": False,
                        "uid": uid,
                    })
                time.sleep(1.5)
            except Exception as e:
                logger_narcl.warning("%s error: %s", source_name, str(e)[:80])

        logger_narcl.info("NARCL/ARC: %d events", len(events))
        return events


class BankAuctionsCoInCrawler(BaseCrawler):
    """BankAuctions.co.in — delegated to IBAPIAuctionCrawler._scrape_bankauctions()."""
    SOURCE_NAME = "BankAuctions.co.in"

    def crawl(self) -> list:
        # Delegate to IBAPI crawler's fallback scraper
        return IBAPIAuctionCrawler().run()

    def run(self) -> list:
        return self.crawl()


class IBBINCLTCrawler(BaseCrawler):
    """IBBI / NCLT RSS crawler for insolvency proceedings."""
    SOURCE_NAME = "IBBI_NCLT"

    IBBI_FEEDS = [
        "https://ibbi.gov.in/home/recent-updates/rss",
        "https://nclt.gov.in/feeds/rss",
    ]
    IBBI_KEYWORDS = re.compile(
        r"(?:cirp|insolvency|liquidation|resolution\s+plan|"
        r"corporate\s+debtor|resolution\s+professional|"
        r"nclt|nclat|admitted|order|bench)",
        re.IGNORECASE,
    )

    def crawl(self) -> list:
        import feedparser
        events = []
        seen = set()
        for url in self.IBBI_FEEDS:
            try:
                r = self._session.get(url, timeout=15)
                if r.status_code != 200:
                    continue
                feed = feedparser.parse(r.content)
                for entry in feed.entries:
                    title   = getattr(entry, "title", "") or ""
                    summary = getattr(entry, "summary", "") or ""
                    link    = getattr(entry, "link", "") or "#"
                    full    = f"{title} {summary}"
                    if not self.IBBI_KEYWORDS.search(full):
                        continue
                    uid = hashlib.md5(title.lower().encode()).hexdigest()
                    if uid in seen:
                        continue
                    seen.add(uid)
                    from nlp.engine import extract_location
                    events.append(self.make_event(
                        company_name=title.split("—")[0].split("-")[0].strip()[:100] or "IBBI",
                        keyword="cirp",
                        category="cirp",
                        url=link,
                        headline=title[:500],
                        snippet=summary[:500],
                        location=extract_location(full),
                        asset_class="commercial",
                    ).to_dict())
            except Exception as e:
                self.logger.warning("IBBI feed %s: %s", url, e)
        self.logger.info("IBBI/NCLT: %d events", len(events))
        return events

    def run(self) -> list:
        return self.crawl()


class FinancialMediaCrawler(BaseCrawler):
    """Financial media RSS feeds for distress signals."""
    SOURCE_NAME = "Financial_Media"

    FEEDS = [
        "https://economictimes.indiatimes.com/wealth/borrow/rss.cms",
        "https://www.thehindubusinessline.com/money-and-banking/rss.cms",
        "https://www.moneycontrol.com/rss/latestnews.xml",
        "https://www.financialexpress.com/economy/feed/",
    ]
    DISTRESS_RE = re.compile(
        r"(?:npa|sarfaesi|drt|nclt|insolvency|liquidation|"
        r"bank\s+auction|e-auction|recovery|stressed\s+asset|"
        r"one\s+time\s+settlement|ots|haircut|write.off)",
        re.IGNORECASE,
    )

    def crawl(self) -> list:
        import feedparser
        from datetime import datetime, timezone, timedelta
        cutoff = datetime.now(timezone.utc) - timedelta(hours=48)
        events = []
        seen = set()
        for url in self.FEEDS:
            try:
                r = self._session.get(url, timeout=12)
                if r.status_code not in (200,):
                    self.logger.warning("HTTP %d for %s", r.status_code, url)
                    continue
                feed = feedparser.parse(r.content)
                for entry in feed.entries:
                    title   = getattr(entry, "title",   "") or ""
                    summary = getattr(entry, "summary", "") or ""
                    link    = getattr(entry, "link",    "") or "#"
                    full    = f"{title} {summary}"
                    if not self.DISTRESS_RE.search(full):
                        continue
                    uid = hashlib.md5(title.lower().encode()).hexdigest()
                    if uid in seen:
                        continue
                    seen.add(uid)
                    from nlp.engine import extract_location, detect_distress_keywords
                    kws = detect_distress_keywords(full)
                    events.append(self.make_event(
                        company_name=title.split(":")[0].strip()[:80] or "Media",
                        keyword=kws[0] if kws else "distress",
                        category="media",
                        url=link,
                        headline=title[:500],
                        snippet=summary[:500],
                        location=extract_location(full),
                        asset_class="commercial",
                    ).to_dict())
            except Exception as e:
                self.logger.warning("Media feed %s: %s", url, e)
        self.logger.info("Financial Media: %d distress events", len(events))
        return events

    def run(self) -> list:
        return self.crawl()
