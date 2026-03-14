"""
enrichment/engine.py — Nexus Asia Intel Deal Enrichment Engine

Runs after every crawl batch to:
  1. Enrich distress_events with deal_score, asset_class, location, is_mmr
  2. Promote high-score commercial+MMR events to pre_leased_assets check
  3. Score demand signals and upsert lead_scores per company
  4. Trigger cross-signal deal matching
  5. Cap rate calculation for pre-leased assets
"""
from __future__ import annotations
import os, re, logging, requests
from datetime import datetime, timezone, timedelta

from db.client import db_get, db_patch, db_upsert, run_deal_matching

logger = logging.getLogger("nexus.enrichment")

SUPABASE_URL      = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "")

# ─── Deal scoring weights ────────────────────────────────────────────────────
# Calibrated for MMR commercial CRE, 10–500Cr sweet spot

SCORE_WEIGHTS = {
    # Asset class
    "commercial":         +40,
    "grade_a_office":     +45,
    "industrial":         +20,
    "residential":        -10,
    "land":               +10,
    # Location
    "is_mmr":             +30,
    "other_metro":        +10,
    # Price band (₹ crore)
    "price_10_500cr":     +20,
    "price_5_10cr":       +10,
    "price_over_500cr":   +5,
    # Channel / source motivation
    "bank_auction":       +15,
    "sarfaesi":           +15,
    "drt":                +15,
    "arc_portfolio":      +15,
    "pe_activity":        +8,
    "media":              +5,
    # Severity
    "critical":           +10,
    "high":               +5,
    "medium":             0,
    "low":                -5,
}

CHANNEL_MAP = {
    "ibapi":              "bank_auction",
    "bankauctions":       "bank_auction",
    "sarfaesi":           "sarfaesi",
    "psu_banks":          "sarfaesi",
    "drt_sarfaesi":       "drt",
    "narcl_arc":          "arc_portfolio",
    "financial_media":    "media",
    "ibbi_nclt":          "regulatory",
}

MMR_CITIES = {
    'mumbai', 'thane', 'navi mumbai', 'bkc', 'andheri', 'powai', 'malad',
    'goregaon', 'kurla', 'vikhroli', 'worli', 'lower parel', 'belapur',
    'airoli', 'kharghar', 'vashi', 'wadala', 'bhandup', 'mulund',
    'kalyan', 'dombivli', 'bhiwandi', 'panvel',
}

OTHER_METROS = {'pune', 'bengaluru', 'bangalore', 'hyderabad', 'delhi', 'gurgaon', 'noida', 'chennai', 'ahmedabad'}


# ─── Cap rate engine ─────────────────────────────────────────────────────────

MUMBAI_RENT_BENCHMARKS = {
    'bkc':          {'grade_a': 310, 'grade_b': 200},
    'lower parel':  {'grade_a': 255, 'grade_b': 160},
    'worli':        {'grade_a': 240, 'grade_b': 150},
    'andheri':      {'grade_a': 145, 'grade_b': 100},
    'powai':        {'grade_a': 125, 'grade_b': 85},
    'malad':        {'grade_a': 112, 'grade_b': 75},
    'goregaon':     {'grade_a': 115, 'grade_b': 78},
    'kurla':        {'grade_a': 108, 'grade_b': 70},
    'vikhroli':     {'grade_a': 98,  'grade_b': 65},
    'thane':        {'grade_a': 82,  'grade_b': 55},
    'navi mumbai':  {'grade_a': 76,  'grade_b': 50},
    'airoli':       {'grade_a': 70,  'grade_b': 48},
    'belapur':      {'grade_a': 65,  'grade_b': 45},
    'default':      {'grade_a': 120, 'grade_b': 75},
}

def calc_cap_rate(location: str, area_sqft: float, price_crore: float,
                  grade: str = "grade_a", escalation_pct: float = 15.0) -> dict:
    """Calculate cap rate, NOI, and 10-year yield for a given asset."""
    if not location or not area_sqft or not price_crore or price_crore <= 0:
        return {}
    loc_key = location.lower().strip()
    benchmarks = MUMBAI_RENT_BENCHMARKS.get(loc_key, MUMBAI_RENT_BENCHMARKS['default'])
    rent_psf  = benchmarks.get(grade, benchmarks['grade_a'])
    # Annual rent: 11 months (1 month vacancy buffer)
    annual_rent_cr = (rent_psf * area_sqft * 11) / 1e7
    noi = annual_rent_cr * 0.90  # 10% opex
    cap_rate = round((noi / price_crore) * 100, 2)
    # 10-year yield with 15% escalation every 3 years
    total_income = 0.0
    current_rent = annual_rent_cr
    for year in range(1, 11):
        total_income += current_rent * 0.90
        if year % 3 == 0:
            current_rent *= (1 + escalation_pct / 100)
    yield_10yr = round((total_income / (price_crore * 10)) * 100, 2)
    return {
        "rent_psf":           rent_psf,
        "annual_rent_cr":     round(annual_rent_cr, 3),
        "noi_annual_cr":      round(noi, 3),
        "cap_rate_pct":       cap_rate,
        "yield_10yr_pct":     yield_10yr,
        "meets_threshold":    cap_rate >= 8.5,
    }


# ─── Enrichment pipeline ─────────────────────────────────────────────────────

def compute_deal_score(event: dict) -> dict:
    """Score a distress_event 0–100 and determine channel."""
    score = 0
    channel = CHANNEL_MAP.get((event.get("source") or "").lower(), "other")
    asset_class = (event.get("asset_class") or "").lower()
    location    = (event.get("location") or "").lower()
    severity    = (event.get("severity") or "medium").lower()
    price       = event.get("price_crore")

    # Asset class
    if "commercial" in asset_class or "grade_a" in asset_class:
        score += SCORE_WEIGHTS["commercial"]
    elif "industrial" in asset_class:
        score += SCORE_WEIGHTS["industrial"]
    elif "residential" in asset_class:
        score += SCORE_WEIGHTS["residential"]
    elif "land" in asset_class:
        score += SCORE_WEIGHTS["land"]

    # Location
    if any(m in location for m in MMR_CITIES):
        score += SCORE_WEIGHTS["is_mmr"]
    elif any(m in location for m in OTHER_METROS):
        score += SCORE_WEIGHTS["other_metro"]

    # Price band
    if price:
        if 10 <= price <= 500:
            score += SCORE_WEIGHTS["price_10_500cr"]
        elif 5 <= price < 10:
            score += SCORE_WEIGHTS["price_5_10cr"]
        elif price > 500:
            score += SCORE_WEIGHTS["price_over_500cr"]

    # Channel
    score += SCORE_WEIGHTS.get(channel, 0)

    # Severity
    score += SCORE_WEIGHTS.get(severity, 0)

    return {
        "deal_score": max(0, min(100, score)),
        "channel":    channel,
        "is_mmr":     any(m in location for m in MMR_CITIES),
    }


def enrich_distress_events(lookback_hours: int = 24):
    """Fetch recent unenriched distress events and score them."""
    if not SUPABASE_URL:
        return
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).isoformat()
    events = db_get("distress_events", {
        "detected_at": f"gte.{cutoff}",
        "deal_score":  "eq.0",
        "select": "id,asset_class,location,severity,price_crore,source,is_mmr",
        "limit": "200",
    })
    enriched = 0
    for evt in events:
        scores = compute_deal_score(evt)
        db_patch("distress_events", scores, {"id": evt["id"]})
        enriched += 1
    logger.info(f"Enriched {enriched} distress events")


def run_full_enrichment():
    """Full enrichment pipeline: score events → match deals → log."""
    logger.info("Starting full enrichment pipeline...")
    enrich_distress_events(lookback_hours=48)
    matches = run_deal_matching()
    logger.info(f"Enrichment complete. {matches} deal matches created.")
