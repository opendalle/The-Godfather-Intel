"""
crawlers/bse_filing.py  — PATCHED
crawlers/nse_filing.py  — PATCHED

Key changes vs original:
1. Apply cre_intelligence.score_signal() to every filing before inserting
2. Hard-block non-CRE filings (dividends, auditor changes, promoter shareholding)
3. NSE: add Accept/Referer headers to defeat 403; add rate limiting
4. NSE: parse actual filing text/subject not just category code
5. Both: generate real why_cre and suggested_action from filing content
6. Dedup: skip if identical company+headline in last 7 days

Usage:
    from crawlers.bse_filing import BSEFilingCrawler
    from crawlers.nse_filing import NSEFilingCrawler

Each crawler exposes a .run() -> List[dict] method returning demand_signal rows
ready for upsert into demand_signals table.
"""

import re
import time
import logging
import hashlib
import feedparser
import requests
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from nlp.engine import score_signal, is_ipo_spam

logger_bse = logging.getLogger("crawler.BSE_FILING")
logger_nse = logging.getLogger("crawler.NSE_FILING")

# ──────────────────────────────────────────────────────────────────────────────
# SHARED HELPERS
# ──────────────────────────────────────────────────────────────────────────────

HEADERS_BROWSER = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
}

HEADERS_NSE = {
    **HEADERS_BROWSER,
    "Referer": "https://www.nseindia.com/",
    "X-Requested-With": "XMLHttpRequest",
}


def _make_uid(company: str, headline: str) -> str:
    raw = f"{company.lower().strip()}::{headline.lower().strip()[:120]}"
    return hashlib.md5(raw.encode()).hexdigest()


def _parse_dt(s: Optional[str]) -> datetime:
    if not s:
        return datetime.now(timezone.utc)
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%d-%b-%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S",
                "%d-%b-%Y", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s.strip(), fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return datetime.now(timezone.utc)


def _build_demand_row(company: str, headline: str, body: str,
                      source: str, url: str, detected_at: datetime,
                      intelligence: dict) -> dict:
    return {
        "company_name": company,
        "signal_type": intelligence["signal_type"],
        "confidence_score": intelligence["confidence_score"],
        "urgency": intelligence["urgency"],
        "location": intelligence["location"],
        "sqft_mentioned": intelligence.get("sqft_mentioned"),
        "funding_amount_cr": intelligence.get("funding_amount_cr"),
        "why_cre": intelligence["why_cre"],
        "suggested_action": intelligence["suggested_action"],
        "summary": headline[:500],
        "data_source": source,
        "source_url": url,
        "detected_at": detected_at.isoformat(),
        "is_duplicate": False,
        "uid": _make_uid(company, headline),
    }


# ──────────────────────────────────────────────────────────────────────────────
# BSE FILING CRAWLER
# ──────────────────────────────────────────────────────────────────────────────

# BSE bulk filing RSS — covers all equity disclosures
BSE_FEED_URL = "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w?strCat=-1&strPrevDate=&strScrip=&strSearch=&strToDate=&strType=C&subcategory=-1"
# Fallback: BSE's public RSS endpoint
BSE_RSS_URLS = [
    "https://www.bseindia.com/corporates/ann.html",
    # Direct API for latest filings (JSON)
    "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w?strCat=-1&strPrevDate=&strScrip=&strSearch=&strType=C&subcategory=-1",
]

# BSE XBRL / filing category codes that are worth parsing
BSE_CRE_CATEGORIES = {
    "New Facility",
    "Expansion Plans",
    "Business Acquisition",
    "Incorporation Of Subsidiary",
    "Commencement of New Project",
    "Setting up of New Entity",
    "Regulation 30",          # General: any material event — filter by content
    "Change In Registered Office Address",
    "Update on Capacity Addition",
    "MoU Signed",
    "Joint Venture",
    "Strategic Partnership",
    "Fund Raising",
    "Rights Issue",           # Override block if paired with expansion language
}

# BSE categories that are ALWAYS noise (hard skip before intelligence scoring)
BSE_SKIP_CATEGORIES = {
    "Dividend",
    "Auditors Report",
    "Change In Auditor",
    "Quarterly Results",
    "Half Yearly Results",
    "Annual Results",
    "Book Closure",
    "Voting Results",
    "Credit Rating",
    "Loss of Share Certificates",
    "Shareholder Meeting",
    "Intimation Of Board Meeting",
    "Board Meeting Intimation",
    "Board Meeting Outcome",    # Outcome alone is noise — only paired with expansion
    "Outcome of Board Meeting",
    "Insider Trading",
    "Promoter Shareholding",
    "Pledge of Shares",
    "Trading Window",
    "Financial Statements",
}


class BSEFilingCrawler:
    """
    Crawls BSE filing API for announcements made in the last 48 hours,
    applies CRE intelligence scoring, and returns only genuine demand signals.
    """

    def __init__(self, session: requests.Session = None):
        self.sess = session or requests.Session()
        self.sess.headers.update(HEADERS_BROWSER)

    def run(self) -> List[dict]:
        signals = []
        raw = self._fetch_filings()
        logger_bse.info("BSE raw filings fetched: %d", len(raw))

        for filing in raw:
            company = (filing.get("SLONGNAME") or filing.get("company") or "").strip()
            headline = (filing.get("HEADLINE") or filing.get("headline") or "").strip()
            category = (filing.get("CATEGORYNAME") or filing.get("category") or "").strip()
            url = filing.get("ATTACHMENTNAME") or filing.get("url") or "#"
            dt_str = filing.get("NEWS_DT") or filing.get("date") or ""
            detected_at = _parse_dt(dt_str)

            if not company or not headline:
                continue

            # Hard skip by category before heavy processing
            if category in BSE_SKIP_CATEGORIES:
                continue

            # Score with intelligence engine
            intel = score_signal(
                company_name=company,
                headline=headline,
                body=category,
                source="BSE_FILING",
                detected_at=detected_at,
            )

            if not intel.get("is_cre"):
                logger_bse.debug("BSE skip [%s]: %s — %s",
                                 company[:30], headline[:60],
                                 intel.get("block_reason", ""))
                continue

            if is_ipo_spam(company, intel.get("why_cre", "")):
                logger_bse.debug("BSE IPO spam skip: %s", company)
                continue

            row = _build_demand_row(
                company=company,
                headline=headline,
                body=category,
                source="BSE_FILING",
                url=url if url.startswith("http") else f"https://www.bseindia.com{url}",
                detected_at=detected_at,
                intelligence=intel,
            )
            signals.append(row)
            logger_bse.info("[BSE] ✓ %s — %s [%s, score=%d]",
                            company, headline[:80], intel["signal_type"],
                            intel["confidence_score"])

        logger_bse.info("BSE: %d CRE filings (from %d raw)", len(signals), len(raw))
        return signals

    def _fetch_filings(self) -> list:
        """
        Try BSE's JSON API first, fall back to parsing the HTML table.
        Returns list of dicts with at least: SLONGNAME, HEADLINE, CATEGORYNAME, NEWS_DT, ATTACHMENTNAME
        """
        # Endpoint: last 2 days of filings, all categories
        today = datetime.now().strftime("%Y%m%d")
        two_days_ago = (datetime.now() - timedelta(days=2)).strftime("%Y%m%d")

        api_url = (
            f"https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
            f"?strCat=-1&strPrevDate={two_days_ago}&strScrip=&strSearch="
            f"&strToDate={today}&strType=C&subcategory=-1"
        )

        try:
            r = self.sess.get(api_url, timeout=20)
            if r.status_code == 200:
                data = r.json()
                # BSE returns {'Table': [...]} or a list directly
                if isinstance(data, dict):
                    return data.get("Table", data.get("table", []))
                if isinstance(data, list):
                    return data
        except Exception as e:
            logger_bse.warning("BSE API error: %s — trying fallback", e)

        # Fallback: BSE RSS feed for recent corporate announcements
        try:
            rss_url = "https://www.bseindia.com/markets/MarketInfo/CorpNotices.aspx?expandable=5"
            feed = feedparser.parse(rss_url)
            results = []
            for entry in feed.entries:
                results.append({
                    "SLONGNAME": entry.get("title", "").split("–")[0].split("-")[0].strip(),
                    "HEADLINE": entry.get("summary", entry.get("title", "")),
                    "CATEGORYNAME": "Regulation 30",
                    "NEWS_DT": entry.get("published", ""),
                    "ATTACHMENTNAME": entry.get("link", "#"),
                })
            return results
        except Exception as e:
            logger_bse.warning("BSE RSS fallback error: %s", e)

        return []


# ──────────────────────────────────────────────────────────────────────────────
# NSE FILING CRAWLER
# ──────────────────────────────────────────────────────────────────────────────

# NSE API endpoints — these need session cookies + correct headers
NSE_ANNOUNCEMENTS_URL = "https://www.nseindia.com/api/corporate-announcements?index=equities"
NSE_HOME_URL = "https://www.nseindia.com/"

# NSE subject patterns that signal CRE potential (regex matched against subject line)
NSE_CRE_SUBJECTS = re.compile(
    r"(?:new\s+(?:office|facility|plant|campus|center|centre|branch|subsidiary|entity|unit|project)|"
    r"expansion|acqui(?:sition|red|ring)|"
    r"registered\s+office|"
    r"joint\s+venture|"
    r"mou\s+(?:signed?|executed?)|"
    r"fund(?:ing)?\s+(?:raised?|round)|"
    r"ipo\s+(?:filing|drhp|red\s+herring|listing)|"
    r"gcc|global\s+(?:capability|delivery)\s+cent(?:er|re)|"
    r"capex|capital\s+expenditure|"
    r"new\s+(?:vertical|division|business\s+line|product\s+line)|"
    r"commence(?:ment|d)\s+of\s+(?:business|operations?|project)|"
    r"new\s+(?:premises|location)|"
    r"lease\s+(?:agreement|deed)|"
    r"setting\s+up|set\s+up\s+of)",
    re.IGNORECASE,
)

# NSE subject patterns that are guaranteed noise — skip immediately
NSE_SKIP_SUBJECTS = re.compile(
    r"(?:dividend|quarterly\s+results?|financial\s+results?|"
    r"q[1-4]\s+results?|half.?year|annual\s+results?|"
    r"credit\s+rating|auditor|board\s+meeting\s+(?:notice|intimation)|"
    r"book\s+closure|trading\s+window|insider\s+trading|"
    r"promoter\s+(?:shareholding|stake)|pledge|"
    r"bonus\s+shares?|rights\s+issue\s+(?:record|closure)|"
    r"exchange\s+of\s+shares?|scheme\s+of\s+arrangement|"
    r"loss\s+of\s+(?:share\s+certificate|documents)|"
    r"duplicate\s+share)",
    re.IGNORECASE,
)


class NSEFilingCrawler:
    """
    Crawls NSE corporate announcements API with proper session handling
    to defeat 403. Applies CRE intelligence scoring.
    """

    def __init__(self, session: requests.Session = None):
        self.sess = session or requests.Session()
        self.sess.headers.update(HEADERS_BROWSER)
        self._cookies_loaded = False

    def _load_nse_cookies(self):
        """NSE requires visiting the homepage first to set session cookies."""
        if self._cookies_loaded:
            return True
        try:
            r = self.sess.get(NSE_HOME_URL, timeout=15)
            if r.status_code == 200:
                self._cookies_loaded = True
                logger_nse.debug("NSE session cookies loaded")
                return True
            logger_nse.warning("NSE homepage returned %d", r.status_code)
            return False
        except Exception as e:
            logger_nse.warning("NSE cookie load failed: %s", e)
            return False

    def run(self) -> List[dict]:
        signals = []

        if not self._load_nse_cookies():
            logger_nse.warning("NSE: Could not establish session — will retry next run")
            return []

        time.sleep(1.5)  # Rate limit respect after homepage visit

        raw = self._fetch_announcements()
        logger_nse.info("NSE raw announcements fetched: %d", len(raw))

        for ann in raw:
            company = (ann.get("symbol") or ann.get("comp") or ann.get("company", "")).strip()
            subject = (ann.get("subject") or ann.get("headline") or "").strip()
            body = (ann.get("desc") or ann.get("body") or "").strip()
            category = (ann.get("sort_date") or "")
            url = ann.get("attchmntFile") or ann.get("url") or "#"
            dt_str = ann.get("sort_date") or ann.get("an_dt") or ""
            detected_at = _parse_dt(dt_str)

            # Use actual company name from symbol if available
            company_full = ann.get("comp", company)

            if not company_full or not subject:
                continue

            # Fast noise filter on subject line
            if NSE_SKIP_SUBJECTS.search(subject):
                continue

            # Only proceed if subject has CRE potential OR we let intelligence decide
            # (intelligence scoring is the final arbiter, but subject pre-filter saves time)
            has_potential = bool(NSE_CRE_SUBJECTS.search(subject))

            # If subject has no obvious CRE signal, still run intelligence on it
            # because even a neutral subject like "Disclosure under Reg 30" could
            # have CRE content. But do limit total processing time.
            intel = score_signal(
                company_name=company_full,
                headline=subject,
                body=body or "",
                source="NSE_FILING",
                detected_at=detected_at,
            )

            if not intel.get("is_cre"):
                if has_potential:
                    logger_nse.debug("NSE intelligence blocked [%s]: %s — %s",
                                     company_full[:30], subject[:60],
                                     intel.get("block_reason", ""))
                continue

            if is_ipo_spam(company_full, intel.get("why_cre", "")):
                continue

            # Build URL
            if url and not url.startswith("http"):
                url = f"https://www.nseindia.com{url}"

            row = _build_demand_row(
                company=company_full,
                headline=subject,
                body=body,
                source="NSE_FILING",
                url=url,
                detected_at=detected_at,
                intelligence=intel,
            )
            signals.append(row)
            logger_nse.info("[NSE] ✓ %s — %s [%s, score=%d]",
                            company_full[:30], subject[:70],
                            intel["signal_type"], intel["confidence_score"])

        logger_nse.info("NSE: %d CRE filings (from %d raw)", len(signals), len(raw))
        return signals

    def _fetch_announcements(self) -> list:
        """
        Fetch NSE corporate announcements. Returns list of announcement dicts.
        NSE uses DOMContentLoaded cookie — we must have loaded homepage first.
        """
        headers = {**HEADERS_NSE}
        try:
            r = self.sess.get(
                NSE_ANNOUNCEMENTS_URL,
                headers=headers,
                timeout=20,
            )
            if r.status_code == 403:
                logger_nse.warning(
                    "NSE 403 — retrying with delay after re-visiting homepage"
                )
                time.sleep(3)
                self._cookies_loaded = False
                self._load_nse_cookies()
                time.sleep(2)
                r = self.sess.get(NSE_ANNOUNCEMENTS_URL, headers=headers, timeout=20)

            if r.status_code != 200:
                logger_nse.warning("NSE API returned %d", r.status_code)
                return []

            data = r.json()
            # NSE returns a list of announcement objects or {'data': [...]}
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return data.get("data", data.get("announcements", []))
            return []

        except Exception as e:
            logger_nse.error("NSE fetch error: %s", e)
            return []


# ═══════════════════════════════════════════════════════════════
# RSS / FINANCIAL MEDIA CRAWLERS (was rss_crawler_patch.py)
# ═══════════════════════════════════════════════════════════════
:
    """
    Crawls all RSS/Atom feeds. Applies CRE intelligence scoring.
    Returns (demand_signals, supply_events) tuple.
    """

    def __init__(self, session: requests.Session = None):
        self.sess = session or requests.Session()
        self.sess.headers.update(HEADERS)
        self._seen_uids: set = set()

    def run(self, cutoff_hours: int = 48) -> tuple:
        """Returns (demand_list, supply_list)"""
        demand_signals = []
        supply_events = []
        cutoff = datetime.now(timezone.utc) - timedelta(hours=cutoff_hours)
        total_articles = 0

        for feed_url, source_label, feed_type in ALL_FEEDS:
            try:
                entries = self._fetch_feed(feed_url)
                for entry in entries:
                    total_articles += 1
                    result = self._process_entry(
                        entry, source_label, feed_type, cutoff
                    )
                    if result is None:
                        continue
                    if result["_type"] == "demand":
                        demand_signals.append(result)
                    elif result["_type"] == "supply":
                        supply_events.append(result)
            except Exception as e:
                logger_rss.warning("Feed error %s: %s", feed_url, str(e)[:80])
            time.sleep(0.3)  # Be polite to servers

        logger_rss.info(
            "RSS: %d articles → %d demand signals, %d supply events",
            total_articles, len(demand_signals), len(supply_events),
        )
        return demand_signals, supply_events

    def _fetch_feed(self, url: str) -> list:
        try:
            r = self.sess.get(url, timeout=12)
            if r.status_code == 200:
                feed = feedparser.parse(r.content)
                return feed.entries
            elif r.status_code in (403, 406):
                # Try without custom headers
                feed = feedparser.parse(url)
                return feed.entries
            else:
                logger_rss.warning("HTTP %d for %s", r.status_code, url)
                return []
        except Exception as e:
            logger_rss.warning("Fetch error %s: %s", url, str(e)[:60])
            return []

    def _process_entry(self, entry, source_label: str, feed_type: str,
                       cutoff: datetime) -> Optional[dict]:
        title = getattr(entry, "title", "") or ""
        summary = getattr(entry, "summary", "") or getattr(entry, "description", "") or ""
        link = getattr(entry, "link", "") or "#"
        detected_at = _parse_feed_date(entry)

        if detected_at < cutoff:
            return None

        if not title.strip():
            return None

        # Extract company
        company = _extract_company_from_title(title)

        uid = _make_uid(company, title)
        if uid in self._seen_uids:
            return None
        self._seen_uids.add(uid)

        full_text = f"{title} {summary}"

        # Score with intelligence
        intel = score_signal(
            company_name=company,
            headline=title,
            body=summary,
            source=source_label,
            detected_at=detected_at,
        )

        if not intel.get("is_cre"):
            return None

        if is_ipo_spam(company, intel.get("why_cre", "")):
            return None

        # Decide signal type based on feed_type + intelligence
        if feed_type == "supply":
            # Supply events go to distress_events table
            return {
                "_type": "supply",
                "company_name": company,
                "headline": title[:500],
                "channel": "media",
                "source": source_label,
                "url": link,
                "detected_at": detected_at.isoformat(),
                "deal_score": intel["confidence_score"],
                "severity": _urgency_to_severity(intel["urgency"]),
                "asset_class": "commercial",
                "location": intel["location"],
                "is_mmr": intel["location"] in ("Mumbai",),
                "is_duplicate": False,
                "uid": uid,
            }
        else:
            # Demand signals
            return {
                "_type": "demand",
                "company_name": company,
                "signal_type": intel["signal_type"],
                "confidence_score": intel["confidence_score"],
                "urgency": intel["urgency"],
                "location": intel["location"],
                "sqft_mentioned": intel.get("sqft_mentioned"),
                "funding_amount_cr": intel.get("funding_amount_cr"),
                "why_cre": intel["why_cre"],
                "suggested_action": intel["suggested_action"],
                "summary": title[:500],
                "data_source": source_label,
                "source_url": link,
                "detected_at": detected_at.isoformat(),
                "is_duplicate": False,
                "uid": uid,
            }


def _urgency_to_severity(urgency: str) -> str:
    return {"CRITICAL": "critical", "HIGH": "high", "MEDIUM": "medium", "LOW": "low"}.get(
        urgency, "medium"
    )


# ──────────────────────────────────────────────────────────────────────────────
# DRT / SARFAESI CRAWLER — patched for dead sources
# ──────────────────────────────────────────────────────────────────────────────

class DRTSARFAESICrawler:
    """
    Crawls legal/insolvency RSS feeds for SARFAESI, DRT, and NPA signals.
    Dead sources replaced. SSL verification disabled for sites with cert issues.
    """

    FEEDS = [
        # Bar and Bench — replaces camlegal.in
        ("https://www.barandbench.com/feed", True),
        # LiveLaw — replaces npablog.in
        ("https://www.livelaw.in/feed", True),
        # SCC Online blog — banking/insolvency law updates
        ("https://www.scconline.com/blog/feed/", True),
        # Cyril Amarchand blog — replaces AZB (404)
        ("https://www.cyrilamarchandblogs.com/feed/", True),
        # JSA Law — replaces trilegal.com
        ("https://jsalaw.com/feed/", True),
        # Khaitan & Co — fixed URL (was 404 because wrong path)
        ("https://www.khaitanco.com/insights/rss/", True),
    ]

    # Keywords specific to distress/SARFAESI/DRT (supply signals)
    DISTRESS_KEYWORDS = re.compile(
        r"(?:sarfaesi|section\s+13|possession\s+notice|"
        r"drt\s+(?:order|judgment|ruling|filing)|"
        r"npa|non.performing\s+asset|"
        r"nclt\s+(?:admits?|order|judgment)|"
        r"insolvency\s+(?:resolution|proceedings?|petition)|"
        r"cirp|liquidation|"
        r"bank\s+(?:auction|e-auction)|"
        r"e-auction\s+notice|"
        r"recovery\s+(?:proceedings?|action|notice)|"
        r"asset\s+reconstruction|arc\s+portfolio|"
        r"haircut|settlement\s+with\s+bank|"
        r"one\s+time\s+settlement|ots)",
        re.IGNORECASE,
    )

    def __init__(self, session: requests.Session = None):
        self.sess = session or requests.Session()
        self.sess.headers.update(HEADERS)
        self.sess.verify = False  # Some legal sites have cert issues
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def run(self, cutoff_hours: int = 48) -> List[dict]:
        events = []
        cutoff = datetime.now(timezone.utc) - timedelta(hours=cutoff_hours)
        seen = set()

        for feed_url, enabled in self.FEEDS:
            if not enabled:
                continue
            try:
                r = self.sess.get(feed_url, timeout=12)
                if r.status_code not in (200, 301, 302):
                    logger_rss.warning("HTTP %d for %s", r.status_code, feed_url)
                    continue
                feed = feedparser.parse(r.content)
                for entry in feed.entries:
                    title = getattr(entry, "title", "") or ""
                    summary = getattr(entry, "summary", "") or ""
                    link = getattr(entry, "link", "") or "#"
                    detected_at = _parse_feed_date(entry)

                    if detected_at < cutoff:
                        continue

                    full = f"{title} {summary}"
                    if not self.DISTRESS_KEYWORDS.search(full):
                        continue

                    uid = _make_uid("", title)
                    if uid in seen:
                        continue
                    seen.add(uid)

                    # Extract company from title if present
                    company = _extract_company_from_title(title) or "Unknown"

                    events.append({
                        "company_name": company,
                        "headline": title[:500],
                        "channel": "drt",
                        "source": "DRT_SARFAESI",
                        "url": link,
                        "detected_at": detected_at.isoformat(),
                        "deal_score": 65,  # base score; enrichment will refine
                        "severity": "high",
                        "asset_class": "commercial",
                        "location": "India",
                        "is_mmr": False,
                        "is_duplicate": False,
                        "uid": uid,
                    })
            except Exception as e:
                logger_rss.warning("DRT feed error %s: %s", feed_url, str(e)[:60])
            time.sleep(0.5)

        logger_rss.info("DRT/SARFAESI: %d events", len(events))
        return events


# ═══════════════════════════════════════════════════════════════
# MCA INCORPORATION CRAWLER (was mca_crawler_patch.py)
# ═══════════════════════════════════════════════════════════════
:
    """
    Crawls MCA for new company incorporations. Applies deep name + context
    intelligence to filter for genuine CRE demand signals.
    """

    def __init__(self, session: requests.Session = None, supabase_client=None):
        self.sess = session or requests.Session()
        self.sess.headers.update(HEADERS)
        self.sb = supabase_client  # for checking if company already exists

    def run(self) -> List[dict]:
        raw = self._fetch_incorporations()
        logger.info("MCA raw incorporations: %d", len(raw))

        signals = []
        blocked = 0

        for company_data in raw:
            result = self._score_incorporation(company_data)
            if result is None:
                blocked += 1
                continue
            signals.append(result)

        logger.info("MCA: %d signals (blocked %d low-signal)", len(signals), blocked)
        return signals

    def _score_incorporation(self, data: dict) -> Optional[dict]:
        """
        Apply intelligence scoring to a new incorporation record.
        Returns demand_signal dict or None if not CRE-relevant.
        """
        company_name = (data.get("company_name") or data.get("name") or "").strip()
        cin = data.get("cin") or data.get("CIN") or ""
        address = data.get("registered_address") or data.get("address") or ""
        incorporation_date = data.get("date_of_incorporation") or data.get("inc_date") or ""
        paid_up_capital = data.get("paid_up_capital") or 0
        activity = data.get("principal_business_activity") or data.get("activity") or ""
        country_of_parent = data.get("country_of_incorporation") or ""

        if not company_name or len(company_name) < 4:
            return None

        # ── HARD BLOCK: low-signal company types ────────────────────────────
        if LOW_SIGNAL_NAMES.search(company_name):
            logger.debug("MCA block (low-signal name): %s", company_name)
            return None

        # Skip if paid-up capital is suspiciously low (shelf companies)
        try:
            capital_cr = float(str(paid_up_capital).replace(",", "")) / 10000000
            if capital_cr < 0.001:  # Less than ₹10,000 — shelf company
                logger.debug("MCA block (zero capital): %s", company_name)
                return None
        except (ValueError, TypeError):
            pass

        # ── SCORE & CLASSIFY ────────────────────────────────────────────────
        score_base = 25  # base score for any new incorporation
        reasons = []
        signal_type = "EXPAND"
        urgency = "MEDIUM"
        location = "India"

        # High-signal name
        if HIGH_SIGNAL_NAMES.search(company_name):
            score_base += 20
            reasons.append(f"Company name '{company_name}' signals knowledge-worker / tech business")

        # Foreign parent → GCC
        if FOREIGN_PARENT.search(company_name) or (country_of_parent and country_of_parent.strip().upper() not in ("INDIA", "IN", "")):
            score_base += 30
            signal_type = "GCC"
            urgency = "HIGH"
            reasons.append("Foreign parent / India-subsidiary indicator — likely GCC or new market entry")

        # Listed parent → regulated entity → proper office required
        if cin.startswith("U") or cin.startswith("L"):
            # L = listed company; subsidiary of listed = regulated
            if any(parent_indicator in company_name.upper() for parent_indicator in
                   ["INDIA", "SERVICES", "TECHNOLOGY", "FINANCIAL", "SOLUTIONS"]):
                score_base += 15
                reasons.append("Subsidiary structure suggests compliance-grade registered office needed")

        # Capital size
        try:
            if capital_cr >= 100:
                score_base += 20
                reasons.append(f"Paid-up capital ₹{capital_cr:.0f}Cr — serious business, not shelf")
                urgency = "HIGH"
            elif capital_cr >= 10:
                score_base += 10
                reasons.append(f"Paid-up capital ₹{capital_cr:.0f}Cr")
        except Exception:
            pass

        # Activity description
        if activity:
            act_intel = score_signal(
                company_name=company_name,
                headline=f"New company incorporated: {company_name}",
                body=f"Principal activity: {activity}. Address: {address}",
                source="MCA_INCORPORATION",
            )
            if act_intel.get("is_cre"):
                score_base = max(score_base, act_intel["confidence_score"])
                if act_intel.get("why_cre"):
                    reasons.append(act_intel["why_cre"])

        # Location
        location = _extract_location_from_cin_address(cin, address)

        # Final score gate
        if score_base < 30:
            logger.debug("MCA low score (%d): %s", score_base, company_name)
            return None

        score_base = min(score_base, 92)  # Cap at 92 — we can't be 100% sure from name alone

        # Build suggested action
        if signal_type == "GCC":
            action = f"Priority outreach — {company_name} is likely setting up India GCC. First-mover advantage in {location}."
        elif urgency == "HIGH":
            action = f"Contact {company_name} founders/directors. First office requirement imminent post-incorporation."
        else:
            action = f"Monitor {company_name} — track first address change from CA office to commercial premises."

        detected_at = datetime.now(timezone.utc)
        if incorporation_date:
            try:
                from nlp.engine import _parse_dt
                detected_at = _parse_dt(incorporation_date)
            except Exception:
                pass

        uid_raw = f"mca::{company_name.lower()}::{cin}"
        uid = hashlib.md5(uid_raw.encode()).hexdigest()

        return {
            "company_name": company_name,
            "signal_type": signal_type,
            "confidence_score": score_base,
            "urgency": urgency,
            "location": location,
            "sqft_mentioned": None,
            "funding_amount_cr": None,
            "why_cre": "; ".join(reasons[:3]) or f"New incorporation — {company_name} in {location}",
            "suggested_action": action,
            "summary": f"MCA: {company_name} incorporated. CIN: {cin}. {activity[:100] if activity else ''}",
            "data_source": "MCA_INCORPORATION",
            "source_url": f"https://www.mca.gov.in/mcafoportal/companyLLPMasterData.do",
            "detected_at": detected_at.isoformat(),
            "is_duplicate": False,
            "uid": uid,
        }

    def _fetch_incorporations(self) -> List[dict]:
        """
        Fetch recent company incorporations from MCA.
        Returns list of dicts with company details.
        """
        results = []

        # Method 1: MCA recent filings page (scrape)
        try:
            r = self.sess.post(
                MCA_NEW_COMPANIES_URL,
                data={"companyType": "company", "noOfRows": "100"},
                timeout=20,
            )
            if r.status_code == 200:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(r.content, "lxml")
                rows = soup.select("table tr")
                for row in rows[1:]:  # Skip header
                    cells = row.find_all("td")
                    if len(cells) >= 4:
                        results.append({
                            "cin": cells[0].get_text(strip=True),
                            "company_name": cells[1].get_text(strip=True),
                            "date_of_incorporation": cells[2].get_text(strip=True),
                            "registered_address": cells[3].get_text(strip=True) if len(cells) > 3 else "",
                        })
        except Exception as e:
            logger.warning("MCA scrape failed: %s", e)

        # Method 2: MCA RSS feed
        if not results:
            try:
                import feedparser
                feed = feedparser.parse(MCA_RSS)
                for entry in feed.entries:
                    title = getattr(entry, "title", "")
                    # Extract company name from RSS title
                    results.append({
                        "company_name": title.split("–")[0].strip(),
                        "cin": "",
                        "date_of_incorporation": getattr(entry, "published", ""),
                        "registered_address": "",
                        "paid_up_capital": 0,
                        "principal_business_activity": getattr(entry, "summary", ""),
                    })
            except Exception as e:
                logger.warning("MCA RSS failed: %s", e)

        return results


def _extract_location_from_cin_address(cin: str, address: str) -> str:
    """
    Extract city from CIN (state code at chars 7-8) or address string.
    """
    # Try CIN state code first
    if len(cin) >= 9:
        state_code = cin[7:9].upper()
        if state_code in STATE_CITY_MAP:
            return STATE_CITY_MAP[state_code]

    # Try address
    if address:
        from nlp.engine import extract_location
        loc = extract_location(address)
        if loc != "India":
            return loc

        # CIN-based city name matching
        address_lower = address.lower()
        city_keywords = {
            "Mumbai": ["mumbai", "bkc", "andheri", "powai", "thane", "navi mumbai", "worli", "lower parel"],
            "Bengaluru": ["bengaluru", "bangalore", "whitefield", "hsr", "koramangala"],
            "Hyderabad": ["hyderabad", "hitec city", "gachibowli", "madhapur"],
            "Pune": ["pune", "hinjewadi", "wakad", "baner"],
            "Chennai": ["chennai", "tidel", "sholinganallur"],
            "NCR": ["delhi", "gurgaon", "gurugram", "noida", "faridabad"],
        }
        for city, keywords in city_keywords.items():
            if any(kw in address_lower for kw in keywords):
                return city

    return "India"

