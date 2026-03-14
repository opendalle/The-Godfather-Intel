"""
main.py — Nexus Asia Intel Master Orchestrator

USAGE:
  python main.py                        # run all crawlers (default)
  python main.py --group supply         # distress/supply crawlers only
  python main.py --group demand         # tenant demand crawlers only
  python main.py --group bank_auction   # bank portals only
  python main.py --group legal          # DRT + SARFAESI
  python main.py --group arc            # NARCL + ARC
  python main.py --group media          # financial media
  python main.py --dry-run              # no DB writes
  python main.py --enrich-only          # enrichment + deal matching only
  python main.py --digest               # send daily digest only

PIPELINE:
  1. Supply crawlers → distress_events table
  2. Demand crawlers → demand_signals table
  3. Enrichment:
     - Score distress events (deal_score, channel, is_mmr)
     - Score demand signals (upsert lead_scores)
     - Cross-signal deal matching → deal_matches table
  4. Alerts: Slack/Telegram for high-priority signals + new matches
"""
from __future__ import annotations

import os, sys, uuid, logging, argparse, time
from datetime import datetime, timezone

from db.client import (
    insert_distress_event, insert_demand_signal, upsert_company,
    is_duplicate_distress, db_get,
)
from nlp.engine import (
    detect_distress_keywords, classify_demand_signal,
    extract_company_names, extract_location, extract_sqft,
)
from nlp.text_cleaner import is_junk_company, clean_text, deduplicate
from enrichment.engine import run_full_enrichment
from notifier.alerts import alert_supply_event, alert_demand_signal, alert_deal_match, send_daily_digest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("nexus.main")

DRY_RUN = False

JUNK_NAMES = [
    "href=", "&#", "cin:", "dalal street", "5th floor", "limited cin",
    "stock exchange", "bse limited", "nse limited", "listing department",
    "p.j. tower", "g-block", "g block", "khasra", "plot ", "kisl/",
    "assemblies limited", "compliance officer", "registered office",
]


# ─── Signal router ────────────────────────────────────────────────────────────

def _should_skip_company(name: str) -> bool:
    if not name or len(name.strip()) < 3:
        return True
    return is_junk_company(name)

def save_supply_event(event: dict, stats: dict):
    """Write a distress event to DB + trigger alert if high-priority."""
    company = event.get("company_name", "Unknown")
    keyword = event.get("signal_keyword", "")
    source  = event.get("source", "")

    if _should_skip_company(company):
        stats["skipped"] += 1
        return

    if is_duplicate_distress(company, keyword, source):
        stats["dupes"] += 1
        return

    if not DRY_RUN:
        ok = insert_distress_event(event)
        if ok:
            stats["inserted"] += 1
            if event.get("deal_score", 0) >= 70:
                alert_supply_event(event)
        else:
            stats["errors"] += 1
    else:
        stats["inserted"] += 1
        logger.debug(f"[DRY-RUN] Supply: {company} | {keyword} | score={event.get('deal_score',0)}")

def save_demand_signal(company_name: str, signal: dict, stats: dict):
    """Write a demand signal to DB + trigger alert if high-urgency."""
    if _should_skip_company(company_name):
        stats["skipped"] += 1
        return
    if not DRY_RUN:
        ok = insert_demand_signal(company_name, signal)
        if ok:
            stats["inserted"] += 1
            if signal.get("urgency") in ("HIGH", "CRITICAL") and (signal.get("confidence") or 0) >= 70:
                alert_demand_signal({**signal, "company_name": company_name})
        else:
            stats["errors"] += 1
    else:
        stats["inserted"] += 1
        logger.debug(f"[DRY-RUN] Demand: {company_name} | {signal.get('signal_type')} | conf={signal.get('confidence',0)}")


# ─── Supply pipeline ──────────────────────────────────────────────────────────

def run_supply_pipeline(group: str = "all") -> dict:
    from crawlers.supply import (
        IBAPIAuctionCrawler, BankAuctionsCoInCrawler, MultiPSUBankCrawler,
        DRTSARFAESICrawler, IBBINCLTCrawler, NARCLARCCrawler, FinancialMediaCrawler,
    )
    from enrichment.engine import compute_deal_score

    CRAWLER_GROUPS = {
        "bank_auction": [IBAPIAuctionCrawler, BankAuctionsCoInCrawler, MultiPSUBankCrawler],
        "legal":        [DRTSARFAESICrawler],
        "regulatory":   [IBBINCLTCrawler],
        "arc":          [NARCLARCCrawler],
        "media":        [FinancialMediaCrawler],
    }
    CRAWLER_GROUPS["all"] = [c for grp in CRAWLER_GROUPS.values() for c in grp]
    CRAWLER_GROUPS["supply"] = CRAWLER_GROUPS["all"]

    crawlers = CRAWLER_GROUPS.get(group, CRAWLER_GROUPS["all"])
    stats = {"inserted": 0, "dupes": 0, "skipped": 0, "errors": 0}

    for CrawlerClass in crawlers:
        crawler_name = CrawlerClass.__name__
        logger.info(f"Running {crawler_name}...")
        t0 = time.time()
        try:
            crawler = CrawlerClass()
            events  = crawler.crawl()
            for event in events:
                # Apply deal scoring inline before save
                scores = compute_deal_score(event)
                event.update(scores)
                save_supply_event(event, stats)
            elapsed = round(time.time() - t0, 1)
            logger.info(f"{crawler_name}: {len(events)} events in {elapsed}s")
        except Exception as e:
            logger.error(f"{crawler_name} failed: {e}", exc_info=True)
            stats["errors"] += 1

    logger.info(f"Supply pipeline: inserted={stats['inserted']} dupes={stats['dupes']} skipped={stats['skipped']} errors={stats['errors']}")
    return stats


# ─── Demand pipeline ─────────────────────────────────────────────────────────

def run_demand_pipeline() -> dict:
    from crawlers.demand import (
        BSEFilingCrawler, NSEFilingCrawler, LinkedInHiringCrawler,
        RSSNewsCrawler, MCAIncorporationCrawler,
    )

    DEMAND_CRAWLERS = [
        BSEFilingCrawler, NSEFilingCrawler, LinkedInHiringCrawler,
        RSSNewsCrawler, MCAIncorporationCrawler,
    ]
    stats = {"inserted": 0, "dupes": 0, "skipped": 0, "errors": 0}

    for CrawlerClass in DEMAND_CRAWLERS:
        crawler_name = CrawlerClass.__name__
        logger.info(f"Running {crawler_name}...")
        try:
            crawler  = CrawlerClass()
            articles = crawler.crawl()
            # Deduplicate by URL
            articles = deduplicate([a.__dict__ for a in articles], key="url")
            for art in articles:
                text = clean_text(f"{art.get('title','')} {art.get('text','')}")
                signal = classify_demand_signal({"title": art.get("title",""), "text": text})
                if not signal:
                    continue
                # Determine company name
                company = art.get("company_hint") or ""
                if not company:
                    companies = extract_company_names(text)
                    company = companies[0] if companies else ""
                if not company:
                    continue
                # Enrich signal with location + sqft
                signal["location"]      = art.get("location_hint") or extract_location(text) or "India"
                signal["sqft_mentioned"] = extract_sqft(text)
                signal["source_url"]    = art.get("url", "")
                signal["data_source"]   = art.get("source", "RSS")
                signal["summary"]       = text[:300]
                save_demand_signal(company, signal, stats)
        except Exception as e:
            logger.error(f"{crawler_name} failed: {e}", exc_info=True)
            stats["errors"] += 1

    logger.info(f"Demand pipeline: inserted={stats['inserted']} dupes={stats['dupes']} skipped={stats['skipped']} errors={stats['errors']}")
    return stats


# ─── Entry point ─────────────────────────────────────────────────────────────

def main():
    global DRY_RUN

    parser = argparse.ArgumentParser(description="Nexus Asia Intel — Master Orchestrator")
    parser.add_argument("--group",        default="all",   help="Crawler group to run")
    parser.add_argument("--dry-run",      action="store_true", help="No DB writes")
    parser.add_argument("--enrich-only",  action="store_true", help="Run enrichment + matching only")
    parser.add_argument("--digest",       action="store_true", help="Send daily digest only")
    parser.add_argument("--supply-only",  action="store_true", help="Run supply crawlers only")
    parser.add_argument("--demand-only",  action="store_true", help="Run demand crawlers only")
    args = parser.parse_args()

    DRY_RUN = args.dry_run
    if DRY_RUN:
        logger.info("DRY RUN MODE — no DB writes")

    run_id = str(uuid.uuid4())[:8]
    logger.info(f"{'='*60}")
    logger.info(f"Nexus Asia Intel — Run {run_id} | Group: {args.group}")
    logger.info(f"{'='*60}")

    if args.digest:
        send_daily_digest()
        return

    if args.enrich_only:
        run_full_enrichment()
        return

    t_start = time.time()
    supply_stats = {}
    demand_stats = {}

    if not args.demand_only:
        group = args.group if args.group != "all" else "all"
        supply_stats = run_supply_pipeline(group)

    if not args.supply_only:
        demand_stats = run_demand_pipeline()

    # Post-crawl enrichment
    logger.info("Running enrichment + deal matching...")
    run_full_enrichment()

    elapsed = round(time.time() - t_start, 1)
    total_inserted = supply_stats.get("inserted", 0) + demand_stats.get("inserted", 0)
    logger.info(f"{'='*60}")
    logger.info(f"Run complete in {elapsed}s | Total inserted: {total_inserted}")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    main()
