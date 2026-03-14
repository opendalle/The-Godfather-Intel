"""
crawlers/supply.py — All supply-side distress crawlers
Covers: bank auctions, DRT, SARFAESI, IBBI, NARCL/ARC, financial media
"""
from __future__ import annotations
import re, time, logging, json
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
import requests
from bs4 import BeautifulSoup

from crawlers.base import BaseCrawler, DistressEvent
from nlp.engine import detect_distress_keywords, extract_company_names, get_severity

logger = logging.getLogger(__name__)

# ─── Shared constants ────────────────────────────────────────────────────────

ALL_LOCATIONS = {
    'mumbai': 'Mumbai', 'thane': 'Thane', 'navi mumbai': 'Navi Mumbai',
    'bkc': 'BKC', 'andheri': 'Andheri', 'powai': 'Powai', 'malad': 'Malad',
    'goregaon': 'Goregaon', 'kurla': 'Kurla', 'vikhroli': 'Vikhroli',
    'worli': 'Worli', 'lower parel': 'Lower Parel', 'wadala': 'Wadala',
    'bandra': 'Bandra', 'airoli': 'Airoli', 'belapur': 'Belapur',
    'kharghar': 'Kharghar', 'vashi': 'Vashi', 'thane': 'Thane',
    'pune': 'Pune', 'hinjewadi': 'Hinjewadi', 'kharadi': 'Kharadi',
    'delhi': 'Delhi', 'noida': 'Noida', 'gurgaon': 'Gurgaon', 'gurugram': 'Gurugram',
    'bengaluru': 'Bengaluru', 'bangalore': 'Bengaluru', 'whitefield': 'Whitefield',
    'hyderabad': 'Hyderabad', 'hitec city': 'HiTec City', 'gachibowli': 'Gachibowli',
    'chennai': 'Chennai', 'ahmedabad': 'Ahmedabad', 'kolkata': 'Kolkata',
}

MMR_CITIES = {
    'mumbai', 'thane', 'navi mumbai', 'bkc', 'andheri', 'powai', 'malad',
    'goregaon', 'kurla', 'vikhroli', 'worli', 'lower parel', 'wadala',
    'bandra', 'airoli', 'belapur', 'kharghar', 'vashi', 'bhiwandi',
    'kalyan', 'dombivli', 'panvel', 'ulwe', 'kanjurmarg', 'bhandup',
}

COMMERCIAL_KW = [
    'office', 'commercial', 'shop', 'showroom', 'godown', 'warehouse',
    'factory', 'industrial', 'it park', 'bpo', 'business park',
    'mall', 'retail', 'plaza', 'complex', 'premises', 'unit', 'floor',
    'wing', 'building', 'tower', 'shed', 'it/ites',
]

PRICE_RE = re.compile(
    r'(?:rs\.?|₹|inr|reserve\s+price[:\s]*)?\s*([\d,]+(?:\.\d+)?)\s*(crore|cr\.?|lakh|lac)',
    re.IGNORECASE
)

def _extract_price(text: str):
    m = PRICE_RE.search(text)
    if m:
        try:
            val = float(m.group(1).replace(",", ""))
            unit = m.group(2).lower()
            return round(val / 100 if "lakh" in unit or "lac" in unit else val, 2)
        except:
            pass
    return None

def _extract_location(text: str):
    text_lower = text.lower()
    for loc_key, loc_val in ALL_LOCATIONS.items():
        if loc_key in text_lower:
            return loc_val
    return None

def _is_mmr(location: str) -> bool:
    return bool(location and any(m in location.lower() for m in MMR_CITIES))

def _asset_class(text: str) -> str:
    text_lower = text.lower()
    if any(kw in text_lower for kw in COMMERCIAL_KW):
        return "commercial"
    if any(kw in text_lower for kw in ['flat', 'apartment', 'residential', '1bhk', '2bhk', '3bhk', 'villa']):
        return "residential"
    if any(kw in text_lower for kw in ['land', 'plot', 'agricultural']):
        return "land"
    return "other"


# ═══════════════════════════════════════════════════════════════════════════
# BANK AUCTION CRAWLERS
# ═══════════════════════════════════════════════════════════════════════════

class IBAPIAuctionCrawler(BaseCrawler):
    """IBAPI — RBI-mandated bank auction portal, JSON API. Tier 1."""
    SOURCE_NAME = "IBAPI"
    SOURCE_URL  = "https://ibapi.in"
    CATEGORY    = "auction"

    API_URL = "https://ibapi.in/api/auctions"

    def crawl(self) -> list:
        resp = self.safe_get(self.API_URL, use_firecrawl=True)
        if not resp:
            return []
        events = []
        try:
            data = resp.json() if hasattr(resp, 'json') else json.loads(resp.text)
            items = data.get("auctions", data) if isinstance(data, dict) else data
            for item in (items or [])[:self.MAX_ARTICLES]:
                text = str(item)
                companies = extract_company_names(text) or ["Unknown"]
                loc = _extract_location(text)
                evt = self.make_event(
                    company_name=companies[0],
                    keyword="auction",
                    category="auction",
                    url=item.get("url", self.SOURCE_URL),
                    headline=str(item.get("title", item.get("property_description", "")))[:400],
                    snippet=text[:800],
                    location=loc,
                    price_crore=_extract_price(text),
                    asset_class=_asset_class(text),
                    metadata={"source_data": item},
                )
                events.append(evt.to_dict())
        except Exception as e:
            self.logger.error(f"IBAPI parse error: {e}")
        self.logger.info(f"IBAPI: {len(events)} auctions found")
        return events


class BankAuctionsCoInCrawler(BaseCrawler):
    """BankAuctions.co.in — third-party aggregator, HTML. Tier 1."""
    SOURCE_NAME = "BankAuctions.co.in"
    SOURCE_URL  = "https://bankauctions.co.in"
    CATEGORY    = "auction"
    PAGES       = ["https://bankauctions.co.in/auctions/commercial", "https://bankauctions.co.in/auctions"]

    def crawl(self) -> list:
        events = []
        for url in self.PAGES:
            resp = self.safe_get(url, use_firecrawl=True)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, "lxml")
            for card in soup.select(".auction-card, .property-card, .listing-item, article")[:30]:
                text = card.get_text(" ", strip=True)
                if len(text) < 50:
                    continue
                companies = extract_company_names(text) or ["Bank Auction Property"]
                link = card.find("a")
                href = link.get("href", url) if link else url
                if href.startswith("/"):
                    href = self.SOURCE_URL + href
                evt = self.make_event(
                    company_name=companies[0],
                    keyword="bank auction",
                    category="auction",
                    url=href,
                    headline=text[:300],
                    snippet=text[:600],
                    price_crore=_extract_price(text),
                    location=_extract_location(text),
                    asset_class=_asset_class(text),
                )
                events.append(evt.to_dict())
        self.logger.info(f"BankAuctions.co.in: {len(events)} found")
        return events


class MultiPSUBankCrawler(BaseCrawler):
    """Crawls auction pages of multiple PSU banks."""
    SOURCE_NAME = "PSU_Banks"
    SOURCE_URL  = "https://bankauctions.co.in"
    CATEGORY    = "auction"

    BANK_AUCTION_URLS = [
        ("SBI",                  "https://sbi.co.in/web/sbi-in-the-news/auction-notices"),
        ("Bank of Baroda",       "https://www.bankofbaroda.in/banking-mantra/e-auction"),
        ("Punjab National Bank", "https://www.pnbindia.in/e-auction.html"),
        ("Canara Bank",          "https://canarabank.com/e-auction"),
        ("Union Bank of India",  "https://unionbankofindia.co.in/english/e-auction.aspx"),
        ("Bank of Maharashtra",  "https://www.bankofmaharashtra.in/auction-notice"),
        ("Indian Overseas Bank", "https://www.iob.in/e-auction"),
        ("Central Bank",         "https://www.centralbankofindia.co.in/en/e-auction"),
    ]

    def crawl(self) -> list:
        events = []
        for bank_name, url in self.BANK_AUCTION_URLS:
            resp = self.safe_get(url, use_firecrawl=True)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, "lxml")
            for item in soup.select("table tr, .notice-item, .auction-item, li")[:20]:
                text = item.get_text(" ", strip=True)
                if len(text) < 40 or not any(kw in text.lower() for kw in ["auction", "sarfaesi", "property", "office", "reserve"]):
                    continue
                link = item.find("a")
                href = (link.get("href", url) if link else url)
                if href.startswith("/"):
                    href = url.split("/")[0] + "//" + url.split("/")[2] + href
                companies = extract_company_names(text) or [f"{bank_name} Auction"]
                evt = self.make_event(
                    company_name=companies[0],
                    keyword="sarfaesi",
                    category="sarfaesi",
                    url=href,
                    headline=f"{bank_name}: {text[:200]}",
                    snippet=text[:500],
                    price_crore=_extract_price(text),
                    location=_extract_location(text),
                    asset_class=_asset_class(text),
                    metadata={"bank": bank_name},
                )
                events.append(evt.to_dict())
        self.logger.info(f"PSU Banks: {len(events)} auction notices found")
        return events


# ═══════════════════════════════════════════════════════════════════════════
# DRT / SARFAESI CRAWLERS
# ═══════════════════════════════════════════════════════════════════════════

DRT_BENCHES = {
    'DRT Mumbai':    'https://drt.gov.in/DRT_Mumbai',
    'DRT Pune':      'https://drt.gov.in/DRT_Pune',
    'DRT Delhi':     'https://drt.gov.in/DRT_Delhi',
    'DRT Ahmedabad': 'https://drt.gov.in/DRT_Ahmedabad',
    'DRT Bengaluru': 'https://drt.gov.in/DRT_Bangalore',
    'DRT Chennai':   'https://drt.gov.in/DRT_Chennai',
}

LEGAL_NPA_FEEDS = [
    ("NPA Legal Blog",       "https://npablog.in/feed"),
    ("Insolvency Tracker",   "https://insolvencytracker.in/feed"),
    ("CAM Legal",            "https://www.camlegal.in/blog/feed"),
    ("AZB NPA",              "https://azbpartners.com/blog/feed/"),
    ("Khaitan NPA",          "https://www.khaitanco.com/thought-leadership/rss"),
    ("Trilegal Finance",     "https://trilegal.com/insights/rss/"),
]

DRT_KEYWORDS = [
    'original application', 'oa filed', 'debt recovery', 'recovery certificate',
    'drt order', 'section 13(2)', 'section 13(4)', '60 day notice',
    'symbolic possession', 'physical possession taken', 'secured asset auctioned',
    'attachment of property', 'npa loan recovery', 'sarfaesi possession',
    'writ against bank auction', 'high court stay auction',
]

class DRTSARFAESICrawler(BaseCrawler):
    """Crawls DRT portals and NPA law firm feeds for pre-auction signals."""
    SOURCE_NAME = "DRT_SARFAESI"
    CATEGORY    = "creditor_action"

    def crawl(self) -> list:
        events = []
        # 1. DRT bench portals
        for bench_name, url in DRT_BENCHES.items():
            resp = self.safe_get(url, use_firecrawl=True)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, "lxml")
            for item in soup.select("table tr, .order-item, li")[:20]:
                text = item.get_text(" ", strip=True)
                if not any(kw in text.lower() for kw in DRT_KEYWORDS):
                    continue
                companies = extract_company_names(text) or ["DRT Case"]
                evt = self.make_event(
                    company_name=companies[0],
                    keyword="drt",
                    category="creditor_action",
                    url=url,
                    headline=f"{bench_name}: {text[:250]}",
                    snippet=text[:600],
                    location=_extract_location(bench_name + " " + text),
                    metadata={"bench": bench_name},
                )
                events.append(evt.to_dict())

        # 2. NPA law firm RSS feeds
        for feed_name, feed_url in LEGAL_NPA_FEEDS:
            try:
                resp = self.safe_get(feed_url)
                if not resp:
                    continue
                root = ET.fromstring(resp.text)
                for item in root.findall(".//item")[:10]:
                    title = (item.findtext("title") or "").strip()
                    desc  = (item.findtext("description") or "").strip()
                    link  = (item.findtext("link") or feed_url).strip()
                    text  = title + " " + desc
                    hits  = detect_distress_keywords(text)
                    if not hits:
                        continue
                    kw, cat = hits[0]
                    companies = extract_company_names(text) or ["Legal NPA"]
                    evt = self.make_event(
                        company_name=companies[0],
                        keyword=kw,
                        category=cat,
                        url=link,
                        headline=title,
                        snippet=desc[:500],
                        location=_extract_location(text),
                        metadata={"feed": feed_name},
                    )
                    events.append(evt.to_dict())
            except Exception as e:
                self.logger.warning(f"{feed_name} RSS: {e}")

        self.logger.info(f"DRT/SARFAESI: {len(events)} events")
        return events


# ═══════════════════════════════════════════════════════════════════════════
# IBBI / REGULATORY CRAWLERS
# ═══════════════════════════════════════════════════════════════════════════

IBBI_GNEWS_QUERIES = [
    "IBBI insolvency India commercial real estate 2025",
    "NCLT CIRP admitted commercial property Mumbai 2025",
    "NCLT liquidation order office building India 2025",
    "IBBI resolution applicant commercial real estate 2025",
    "insolvency real estate developer Mumbai NCLT 2025",
]

class IBBINCLTCrawler(BaseCrawler):
    """IBBI + NCLT insolvency signals via Google News RSS."""
    SOURCE_NAME = "IBBI_NCLT"
    CATEGORY    = "regulatory"

    GNEWS_BASE = "https://news.google.com/rss/search?q={q}&hl=en-IN&gl=IN&ceid=IN:en"

    def crawl(self) -> list:
        events = []
        for query in IBBI_GNEWS_QUERIES:
            url = self.GNEWS_BASE.format(q=quote(query))
            resp = self.safe_get(url)
            if not resp:
                continue
            try:
                root = ET.fromstring(resp.text)
                for item in root.findall(".//item")[:8]:
                    title = (item.findtext("title") or "").strip()
                    desc  = (item.findtext("description") or "").strip()
                    link  = (item.findtext("link") or "").strip()
                    pub   = item.findtext("pubDate", "")
                    text  = title + " " + desc
                    hits  = detect_distress_keywords(text)
                    if not hits:
                        continue
                    kw, cat = hits[0]
                    companies = extract_company_names(text) or ["IBBI Case"]
                    evt = self.make_event(
                        company_name=companies[0],
                        keyword=kw,
                        category="cirp" if "cirp" in text.lower() or "nclt" in text.lower() else cat,
                        url=link,
                        headline=title,
                        snippet=desc[:500],
                        published_at=pub,
                        location=_extract_location(text),
                        metadata={"query": query},
                    )
                    events.append(evt.to_dict())
            except Exception as e:
                self.logger.warning(f"IBBI GNEWS '{query}': {e}")
        self.logger.info(f"IBBI/NCLT: {len(events)} events")
        return events


# ═══════════════════════════════════════════════════════════════════════════
# NARCL / ARC CRAWLERS
# ═══════════════════════════════════════════════════════════════════════════

ARC_GNEWS_QUERIES = [
    "NARCL bad bank India commercial real estate acquisition 2025",
    "NARCL portfolio stressed asset sale 2025",
    "ARCIL Edelweiss ARC commercial property India 2025",
    "Phoenix ARC HDFC distressed commercial sale 2025",
    "ARC portfolio India commercial office sale 2025",
]

class NARCLARCCrawler(BaseCrawler):
    """NARCL + major ARC portfolio tracking via Google News + direct scrape."""
    SOURCE_NAME = "NARCL_ARC"
    CATEGORY    = "arc_portfolio"

    ARC_URLS = [
        ("NARCL",        "https://narcl.co.in"),
        ("ARCIL",        "https://www.arcil.com/news-and-media"),
        ("Edelweiss ARC", "https://www.edelweissarc.com/media"),
        ("Phoenix ARC",  "https://www.phoenixarc.co.in"),
    ]
    GNEWS_BASE = "https://news.google.com/rss/search?q={q}&hl=en-IN&gl=IN&ceid=IN:en"

    def crawl(self) -> list:
        events = []
        # Direct ARC websites (Firecrawl for JS-rendered sites)
        for arc_name, url in self.ARC_URLS:
            resp = self.safe_get(url, use_firecrawl=True)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, "lxml")
            for item in soup.select("article, .news-item, .portfolio-item, li")[:15]:
                text = item.get_text(" ", strip=True)
                if len(text) < 50:
                    continue
                if not any(kw in text.lower() for kw in ['crore', 'property', 'commercial', 'real estate', 'npa', 'asset']):
                    continue
                link = item.find("a")
                href = link.get("href", url) if link else url
                if href.startswith("/"):
                    href = url.rstrip("/") + href
                companies = extract_company_names(text) or [arc_name]
                evt = self.make_event(
                    company_name=companies[0],
                    keyword="arc portfolio",
                    category="arc_portfolio",
                    url=href,
                    headline=f"{arc_name}: {text[:250]}",
                    snippet=text[:600],
                    price_crore=_extract_price(text),
                    location=_extract_location(text),
                    asset_class=_asset_class(text),
                    metadata={"arc": arc_name},
                )
                events.append(evt.to_dict())

        # Google News for ARC deal flow
        for query in ARC_GNEWS_QUERIES:
            url = self.GNEWS_BASE.format(q=quote(query))
            resp = self.safe_get(url)
            if not resp:
                continue
            try:
                root = ET.fromstring(resp.text)
                for item in root.findall(".//item")[:6]:
                    title = (item.findtext("title") or "").strip()
                    desc  = (item.findtext("description") or "").strip()
                    link  = (item.findtext("link") or "").strip()
                    text  = title + " " + desc
                    if not detect_distress_keywords(text):
                        continue
                    companies = extract_company_names(text) or ["ARC Deal"]
                    evt = self.make_event(
                        company_name=companies[0],
                        keyword="arc portfolio",
                        category="arc_portfolio",
                        url=link,
                        headline=title,
                        snippet=desc[:500],
                        location=_extract_location(text),
                        price_crore=_extract_price(text),
                    )
                    events.append(evt.to_dict())
            except Exception as e:
                self.logger.warning(f"ARC GNEWS '{query}': {e}")

        self.logger.info(f"NARCL/ARC: {len(events)} events")
        return events


# ═══════════════════════════════════════════════════════════════════════════
# FINANCIAL MEDIA CRAWLER (distress-filtered)
# ═══════════════════════════════════════════════════════════════════════════

MEDIA_GNEWS_QUERIES = [
    "NPA commercial real estate Mumbai 2025",
    "SARFAESI commercial property auction India 2025",
    "distressed commercial asset sale India 2025",
    "default commercial office India bank 2025",
    "stressed real estate India developer NPA 2025",
    "NCLT insolvency commercial Mumbai Pune Bengaluru",
    "PE fund exit commercial office India 2025",
    "REIT India office vacancy distress 2025",
]

class FinancialMediaCrawler(BaseCrawler):
    """ET, BS, Mint, Moneycontrol distress signals via Google News RSS."""
    SOURCE_NAME = "Financial_Media"
    CATEGORY    = "financial_media"

    GNEWS_BASE  = "https://news.google.com/rss/search?q={q}&hl=en-IN&gl=IN&ceid=IN:en"
    RSS_FEEDS   = [
        ("Economic Times Realty",  "https://realty.economictimes.indiatimes.com/rss/topstories"),
        ("Business Standard CRE",  "https://www.business-standard.com/rss/companies-101.rss"),
        ("Livemint Real Estate",   "https://www.livemint.com/rss/real-estate"),
        ("Moneycontrol Business",  "https://www.moneycontrol.com/rss/business.xml"),
    ]

    def crawl(self) -> list:
        events = []
        # RSS feeds first
        for feed_name, feed_url in self.RSS_FEEDS:
            resp = self.safe_get(feed_url)
            if not resp:
                continue
            try:
                root = ET.fromstring(resp.text)
                for item in root.findall(".//item")[:15]:
                    title = (item.findtext("title") or "").strip()
                    desc  = (item.findtext("description") or "").strip()
                    link  = (item.findtext("link") or "").strip()
                    pub   = item.findtext("pubDate", "")
                    text  = title + " " + desc
                    hits  = detect_distress_keywords(text)
                    if not hits:
                        continue
                    kw, cat = hits[0]
                    companies = extract_company_names(text) or ["Unknown"]
                    evt = self.make_event(
                        company_name=companies[0],
                        keyword=kw,
                        category=cat,
                        url=link,
                        headline=title,
                        snippet=desc[:600],
                        published_at=pub,
                        location=_extract_location(text),
                        price_crore=_extract_price(text),
                        asset_class=_asset_class(text),
                        metadata={"feed": feed_name},
                    )
                    events.append(evt.to_dict())
            except Exception as e:
                self.logger.warning(f"{feed_name}: {e}")

        # Targeted Google News searches
        for query in MEDIA_GNEWS_QUERIES:
            url = self.GNEWS_BASE.format(q=quote(query))
            resp = self.safe_get(url)
            if not resp:
                continue
            try:
                root = ET.fromstring(resp.text)
                for item in root.findall(".//item")[:5]:
                    title = (item.findtext("title") or "").strip()
                    desc  = (item.findtext("description") or "").strip()
                    link  = (item.findtext("link") or "").strip()
                    text  = title + " " + desc
                    hits  = detect_distress_keywords(text)
                    if not hits:
                        continue
                    kw, cat = hits[0]
                    companies = extract_company_names(text) or ["Unknown"]
                    evt = self.make_event(
                        company_name=companies[0],
                        keyword=kw,
                        category=cat,
                        url=link,
                        headline=title,
                        snippet=desc[:500],
                        location=_extract_location(text),
                        price_crore=_extract_price(text),
                        asset_class=_asset_class(text),
                        metadata={"query": query},
                    )
                    events.append(evt.to_dict())
            except Exception as e:
                self.logger.warning(f"Media GNEWS '{query}': {e}")

        self.logger.info(f"Financial Media: {len(events)} distress events")
        return events
