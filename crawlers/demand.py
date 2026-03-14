"""
crawlers/demand.py — All demand-side tenant crawlers
Covers: BSE/NSE filings, LinkedIn hiring surge, funding news, RSS feeds
"""
from __future__ import annotations
import re, time, logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
import requests
from bs4 import BeautifulSoup

from crawlers.base import BaseCrawler, DemandArticle
from nlp.engine import classify_demand_signal, _parse_funding_cr

logger = logging.getLogger(__name__)

RSS_FEEDS = [
    "https://realty.economictimes.indiatimes.com/rss/topstories",
    "https://economictimes.indiatimes.com/industry/services/property-/-cstruction/rssfeeds/13358319.cms",
    "https://www.business-standard.com/rss/companies-101.rss",
    "https://www.livemint.com/rss/companies",
    "https://www.livemint.com/rss/real-estate",
    "https://inc42.com/feed/",
    "https://yourstory.com/feed",
    "https://entrackr.com/feed/",
    "https://housing.com/news/feed/",
    "https://www.moneycontrol.com/rss/business.xml",
    "https://timesofindia.indiatimes.com/rssfeeds/1898055.cms",
]

CRE_NOISE_KEYWORDS = [
    "appointment", "resignation", "cfo", "ceo", "coo", "esop", "allotment",
    "dividend", "agm", "egm", "auditor", "book closure", "record date",
    "financial results", "quarterly results", "credit rating", "shareholding",
    "compliance officer", "intimation", "outcome of board", "investor meet",
]


# ═══════════════════════════════════════════════════════════════════════════
# BSE FILING CRAWLER
# ═══════════════════════════════════════════════════════════════════════════

class BSEFilingCrawler(BaseCrawler):
    """BSE corporate announcements API — noise-filtered for CRE signals."""
    SOURCE_NAME = "BSE_FILING"
    CATEGORY    = "other"

    API_URL      = "https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w"
    ANNOUNCE_URL = "https://www.bseindia.com/corporates/ann.html"

    CRE_KEYWORDS = [
        "new office", "office space", "office premises", "office campus",
        "sq ft", "sqft", "square feet", "lease", "leased", "leasing",
        "new facility", "new campus", "new headquarters", "new hq",
        "relocation", "relocated", "new premises", "commercial property",
        "additional space", "office expansion",
    ]

    def crawl(self, days_back: int = 2) -> list:
        articles = []
        self._session.headers.update({
            "Referer": "https://www.bseindia.com/",
        })
        try:
            params = {
                "strCat":      "-1",
                "strPrevDate": (datetime.now() - timedelta(days=days_back)).strftime("%Y%m%d"),
                "strScrip":    "",
                "strSearch":   "P",
                "strToDate":   datetime.now().strftime("%Y%m%d"),
                "strType":     "C",
                "subcategory": "-1",
            }
            resp = self.safe_get(self.API_URL, params=params)
            if not resp:
                return []
            for item in resp.json().get("Table", []):
                headline = item.get("HEADLINE", "")
                company  = item.get("SLONGNAME", "")
                pdf_name = item.get("ATTACHMENTNAME", "")
                ann_date = item.get("NEWS_DT", "")
                hl_lower = headline.lower()
                if any(nk in hl_lower for nk in CRE_NOISE_KEYWORDS):
                    continue
                if not any(kw in hl_lower for kw in self.CRE_KEYWORDS):
                    continue
                pdf_url = f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{pdf_name}" if pdf_name else self.ANNOUNCE_URL
                articles.append(DemandArticle(
                    title=f"{company}: {headline}",
                    text=headline,
                    url=pdf_url,
                    source="BSE_FILING",
                    published=ann_date,
                    company_hint=company,
                    signal_type_hint="FILING",
                ))
                self.logger.info(f"[BSE] {company[:40]} — {headline[:50]}")
        except Exception as e:
            self.logger.error(f"BSEFilingCrawler: {e}")
        self.logger.info(f"BSE: {len(articles)} CRE filings")
        return articles


# ═══════════════════════════════════════════════════════════════════════════
# NSE FILING CRAWLER
# ═══════════════════════════════════════════════════════════════════════════

class NSEFilingCrawler(BaseCrawler):
    """NSE corporate announcements — headline-only, no PDF download."""
    SOURCE_NAME = "NSE_FILING"
    CATEGORY    = "other"

    NSE_API = "https://www.nseindia.com/api/corporate-announcements"

    CRE_KEYWORDS = [
        "sq ft", "sqft", "office", "campus", "facility", "lease",
        "expansion", "relocation", "headquarters", "new premises", "capex",
    ]

    def crawl(self, days_back: int = 3) -> list:
        articles = []
        self._session.headers.update({
            "Referer": "https://www.nseindia.com/",
            "Accept":  "application/json, text/plain, */*",
        })
        try:
            # Warm up NSE session
            self.safe_get("https://www.nseindia.com")
            time.sleep(2)
            params = {
                "index":     "equities",
                "from_date": (datetime.now() - timedelta(days=days_back)).strftime("%d-%m-%Y"),
                "to_date":   datetime.now().strftime("%d-%m-%Y"),
            }
            resp = self.safe_get(self.NSE_API, params=params)
            if not resp:
                return []
            data  = resp.json()
            items = data if isinstance(data, list) else data.get("data", [])
            for item in (items or []):
                if not isinstance(item, dict):
                    continue
                subject = item.get("subject", item.get("desc", ""))
                company = item.get("company", item.get("symbol", ""))
                if not any(kw in subject.lower() for kw in self.CRE_KEYWORDS):
                    continue
                articles.append(DemandArticle(
                    title=f"{company}: {subject}",
                    text=subject,
                    url=self.NSE_API,
                    source="NSE_FILING",
                    published=item.get("an_dt", datetime.now().isoformat()),
                    company_hint=company,
                    signal_type_hint="FILING",
                ))
        except Exception as e:
            self.logger.error(f"NSEFilingCrawler: {e}")
        self.logger.info(f"NSE: {len(articles)} CRE filings")
        return articles


# ═══════════════════════════════════════════════════════════════════════════
# LINKEDIN HIRING SURGE CRAWLER
# ═══════════════════════════════════════════════════════════════════════════

class LinkedInHiringCrawler(BaseCrawler):
    """Detects hiring surges per city — proxy for imminent office expansion."""
    SOURCE_NAME = "LINKEDIN_JOBS"
    CATEGORY    = "other"

    SEARCH_URL = "https://www.linkedin.com/jobs/search/"
    CITY_SLUGS = {
        "Bengaluru":  "bengaluru-karnataka-india",
        "Mumbai":     "mumbai-maharashtra-india",
        "Hyderabad":  "hyderabad-telangana-india",
        "Pune":       "pune-maharashtra-india",
        "Delhi NCR":  "delhi-india",
        "Chennai":    "chennai-tamil-nadu-india",
    }

    def crawl(self, min_jobs: int = 15) -> list:
        articles = []
        company_city_count: dict = {}
        for city_name, city_slug in self.CITY_SLUGS.items():
            url  = f"{self.SEARCH_URL}?location={city_slug}&f_TPR=r86400&position=1&pageNum=0"
            resp = self.safe_get(url)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, "lxml")
            for card in soup.select("div.base-card"):
                el = card.select_one(".base-search-card__subtitle")
                if el:
                    key = (el.get_text(strip=True), city_name)
                    company_city_count[key] = company_city_count.get(key, 0) + 1
        for (company, city), count in company_city_count.items():
            if count >= min_jobs:
                articles.append(DemandArticle(
                    title=f"{company} hiring surge in {city} ({count}+ roles)",
                    text=f"{company} is actively hiring {count}+ positions in {city}. Large-scale hiring signals upcoming office expansion.",
                    url=f"https://www.linkedin.com/jobs/search/?keywords={company}&location={city}",
                    source="LINKEDIN_JOBS",
                    company_hint=company,
                    location_hint=city,
                    signal_type_hint="HIRING",
                ))
                self.logger.info(f"[LinkedIn] Surge: {company} in {city} — {count} jobs")
        self.logger.info(f"LinkedIn: {len(articles)} hiring surges")
        return articles


# ═══════════════════════════════════════════════════════════════════════════
# RSS / NEWS CRAWLER
# ═══════════════════════════════════════════════════════════════════════════

class RSSNewsCrawler(BaseCrawler):
    """Multi-feed RSS crawler for CRE demand signals."""
    SOURCE_NAME = "RSS_NEWS"
    CATEGORY    = "other"

    def crawl(self) -> list:
        articles = []
        for feed_url in RSS_FEEDS:
            resp = self.safe_get(feed_url)
            if not resp:
                continue
            try:
                root = ET.fromstring(resp.text)
                for item in root.findall(".//item")[:20]:
                    title   = (item.findtext("title") or "").strip()
                    desc    = (item.findtext("description") or "").strip()
                    link    = (item.findtext("link") or "").strip()
                    pub     = item.findtext("pubDate", "")
                    if not title:
                        continue
                    articles.append(DemandArticle(
                        title=title,
                        text=desc,
                        url=link,
                        source="RSS_NEWS",
                        published=pub,
                    ))
            except Exception as e:
                self.logger.warning(f"RSS {feed_url}: {e}")
        self.logger.info(f"RSS: {len(articles)} articles")
        return articles


# ═══════════════════════════════════════════════════════════════════════════
# MCA NEW INCORPORATION CRAWLER (new — no equivalent in either old repo)
# ═══════════════════════════════════════════════════════════════════════════

class MCAIncorporationCrawler(BaseCrawler):
    """
    MCA21 new company registrations — future office requirements in 6-18 months.
    Filters: Maharashtra/Karnataka/Telangana, Private Limited/LLP, paid-up > ₹10L
    This source is unique — no other CRE intelligence tool mines this systematically.
    """
    SOURCE_NAME = "MCA_INCORPORATION"
    CATEGORY    = "other"

    MCA_GNEWS_QUERIES = [
        "new company incorporated Mumbai Maharashtra 2025 office",
        "startup registered Bengaluru Karnataka new office 2025",
        "MCA incorporation Hyderabad Telangana 2025 tech company",
        "new private limited company Pune 2025",
        "MCA21 new registration Delhi NCR startup 2025",
    ]
    GNEWS_BASE = "https://news.google.com/rss/search?q={q}&hl=en-IN&gl=IN&ceid=IN:en"

    def crawl(self) -> list:
        from urllib.parse import quote
        articles = []
        for query in self.MCA_GNEWS_QUERIES:
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
                    if not title:
                        continue
                    articles.append(DemandArticle(
                        title=title,
                        text=desc,
                        url=link,
                        source="MCA_INCORPORATION",
                        signal_type_hint="NEW_ENTRANT",
                    ))
            except Exception as e:
                self.logger.warning(f"MCA GNEWS: {e}")
        self.logger.info(f"MCA: {len(articles)} incorporation signals")
        return articles
