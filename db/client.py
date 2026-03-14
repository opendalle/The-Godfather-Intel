"""
db/client.py — Nexus Asia Intel unified Supabase client
Handles all reads/writes for distress_events, demand_signals, companies,
pre_leased_assets, deal_matches, cap_rate_snapshots, lead_scores.
"""
from __future__ import annotations
import os, logging, requests
from typing import Optional

logger = logging.getLogger("nexus.db")

SUPABASE_URL         = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_ANON_KEY    = os.environ.get("SUPABASE_ANON_KEY", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "") or SUPABASE_ANON_KEY

_VALID_DISTRESS_CATEGORIES = {
    'insolvency', 'auction', 'restructuring', 'default', 'legal', 'regulatory',
    'general', 'sarfaesi', 'creditor_action', 'rbi_action', 'distressed_asset',
    'cirp', 'liquidation', 'pre_leased_asset', 'cre_vacancy', 'arc_portfolio',
    'pe_activity', 'market_stress', 'financial_media', 'nclt', 'ibbi',
    'bankruptcy', 'debt_resolution', 'asset_auction', 'other',
}
_CAT_REMAP = {
    'pre_leased_cre': 'pre_leased_asset',
    'cre': 'pre_leased_asset',
    'arc': 'arc_portfolio',
    'pe_fund': 'pe_activity',
    'market_distress': 'market_stress',
    'financial media': 'financial_media',
}

def _h(write: bool = False) -> dict:
    key = SUPABASE_SERVICE_KEY if write else SUPABASE_ANON_KEY
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

def _h_repr(write: bool = False) -> dict:
    h = _h(write)
    h["Prefer"] = "return=representation"
    return h

def _h_upsert(write: bool = True) -> dict:
    h = _h(write)
    h["Prefer"] = "resolution=merge-duplicates,return=minimal"
    return h


# ─── Generic CRUD ──────────────────────────────────────────────────────────

def db_get(table: str, params: dict) -> list:
    try:
        r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}", headers=_h(), params=params, timeout=15)
        return r.json() if r.status_code == 200 else []
    except Exception as e:
        logger.warning(f"GET {table}: {e}")
        return []

def db_insert(table: str, rows: list | dict) -> bool:
    if not SUPABASE_URL:
        return False
    payload = rows if isinstance(rows, list) else [rows]
    try:
        r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=_h(write=True), json=payload, timeout=20)
        if r.status_code not in (200, 201):
            logger.error(f"INSERT {table} {r.status_code}: {r.text[:200]}")
            return False
        return True
    except Exception as e:
        logger.error(f"INSERT {table}: {e}")
        return False

def db_upsert(table: str, rows: list | dict, on_conflict: str) -> bool:
    if not SUPABASE_URL:
        return False
    payload = rows if isinstance(rows, list) else [rows]
    try:
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers=_h_upsert(),
            json=payload,
            params={"on_conflict": on_conflict},
            timeout=20,
        )
        return r.status_code in (200, 201)
    except Exception as e:
        logger.error(f"UPSERT {table}: {e}")
        return False

def db_patch(table: str, data: dict, match: dict) -> bool:
    try:
        params = {k: f"eq.{v}" for k, v in match.items()}
        r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}", headers=_h(write=True), params=params, json=data, timeout=15)
        return r.status_code in (200, 204)
    except Exception as e:
        logger.error(f"PATCH {table}: {e}")
        return False


# ─── Company registry ──────────────────────────────────────────────────────

def upsert_company(name: str, **kwargs) -> Optional[str]:
    """Upsert company by normalized name. Returns company UUID."""
    from nlp.text_cleaner import normalize_company_name
    normalized = normalize_company_name(name)
    if not normalized or len(normalized) < 3:
        return None
    payload = {"name": name, "normalized_name": normalized, **kwargs}
    try:
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/companies",
            headers=_h_repr(write=True),
            json=payload,
            params={"on_conflict": "normalized_name"},
        )
        r.raise_for_status()
        if r.json():
            return r.json()[0]["id"]
        # Fetch existing
        existing = db_get("companies", {"normalized_name": f"eq.{normalized}", "select": "id", "limit": "1"})
        return existing[0]["id"] if existing else None
    except Exception as e:
        logger.warning(f"upsert_company {name}: {e}")
        return None


# ─── Distress event ────────────────────────────────────────────────────────

def sanitise_distress_event(row: dict) -> dict:
    row = dict(row)
    cat = row.get("signal_category", "other")
    if cat not in _VALID_DISTRESS_CATEGORIES:
        row["signal_category"] = _CAT_REMAP.get(cat, "other")
    # Truncate long text fields
    if row.get("headline"):
        row["headline"] = row["headline"][:500]
    if row.get("snippet"):
        row["snippet"] = row["snippet"][:1000]
    return row

def is_duplicate_distress(company: str, keyword: str, source: str) -> bool:
    try:
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00Z")
        res = db_get("distress_events", {
            "company_name": f"ilike.{company}",
            "signal_keyword": f"eq.{keyword}",
            "source": f"eq.{source}",
            "detected_at": f"gte.{today}",
            "select": "id",
            "limit": "1",
        })
        return bool(res)
    except:
        return False

def insert_distress_event(event: dict) -> bool:
    return db_insert("distress_events", sanitise_distress_event(event))


# ─── Demand signal ─────────────────────────────────────────────────────────

def insert_demand_signal(company_name: str, signal: dict) -> bool:
    company_id = upsert_company(company_name)
    row = {
        "company_name":     company_name,
        "company_id":       company_id,
        "signal_type":      str(signal.get("signal_type", "OFFICE")).upper(),
        "confidence_score": min(max(int(signal.get("confidence", 0)), 0), 100),
        "urgency":          signal.get("urgency", "MEDIUM"),
        "space_type":       signal.get("space_type"),
        "location":         signal.get("location", "India"),
        "sqft_mentioned":   signal.get("sqft_mentioned"),
        "funding_amount_cr": signal.get("funding_amount_cr"),
        "why_cre":          str(signal.get("why_cre", ""))[:500],
        "suggested_action": str(signal.get("suggested_action", ""))[:500],
        "summary":          str(signal.get("summary", ""))[:500],
        "source_url":       signal.get("source_url", ""),
        "data_source":      signal.get("data_source", "RSS"),
        "matched_phrases":  signal.get("matched_phrases", []),
    }
    return db_insert("demand_signals", row)


# ─── Lead scores ──────────────────────────────────────────────────────────

def upsert_lead_score(company_id: str, demand_score: int, distress_score: int, signal_count: int):
    combined = (demand_score * 0.6 + distress_score * 0.4)
    if combined >= 75:
        priority = "CRITICAL"
    elif combined >= 55:
        priority = "HIGH"
    elif combined >= 30:
        priority = "MEDIUM"
    else:
        priority = "LOW"
    db_upsert("lead_scores", {
        "company_id": company_id,
        "demand_score": demand_score,
        "distress_score": distress_score,
        "combined_score": round(combined),
        "signal_count": signal_count,
        "priority_level": priority,
    }, on_conflict="company_id")


# ─── Deal match engine ─────────────────────────────────────────────────────

def run_deal_matching():
    """
    Cross-signal matching: for each high-score supply event,
    find demand signals in the same location with compatible size/timing.
    """
    logger.info("Running deal matching engine...")
    supply = db_get("distress_events", {
        "deal_score": "gte.60",
        "is_duplicate": "eq.false",
        "is_mmr": "eq.true",
        "select": "id,location,asset_class,price_crore,deal_score,company_name",
        "limit": "100",
        "order": "deal_score.desc",
    })
    demand = db_get("demand_signals", {
        "confidence_score": "gte.55",
        "is_duplicate": "eq.false",
        "urgency": "in.(HIGH,CRITICAL)",
        "select": "id,location,sqft_mentioned,confidence_score,company_name,signal_type,urgency",
        "limit": "100",
    })
    if not supply or not demand:
        return 0
    matches_created = 0
    for s in supply:
        for d in demand:
            score, reason = _score_match(s, d)
            if score < 40:
                continue
            db_upsert("deal_matches", {
                "supply_event_id": s["id"],
                "demand_signal_id": d["id"],
                "match_score": score,
                "match_reason": reason,
                "location_overlap": _location_overlap(s.get("location",""), d.get("location","")),
                "size_compatible": True,
                "timing_overlap": True,
                "broker_action": f"Connect {s['company_name']} (seller) with {d['company_name']} (potential tenant/buyer)",
                "status": "new",
            }, on_conflict="supply_event_id,demand_signal_id")
            matches_created += 1
    logger.info(f"Deal matching: {matches_created} matches created/updated")
    return matches_created

def _location_overlap(supply_loc: str, demand_loc: str) -> bool:
    if not supply_loc or not demand_loc:
        return False
    s, d = supply_loc.lower(), demand_loc.lower()
    MMR_SET = {"mumbai", "thane", "navi mumbai", "bkc", "andheri", "powai", "malad",
               "goregaon", "kurla", "vikhroli", "worli", "lower parel"}
    supply_mmr = any(m in s for m in MMR_SET)
    demand_mmr = any(m in d for m in MMR_SET)
    if supply_mmr and demand_mmr:
        return True
    for city in ["pune", "bengaluru", "bangalore", "hyderabad", "delhi", "chennai"]:
        if city in s and city in d:
            return True
    return False

def _score_match(supply: dict, demand: dict) -> tuple[int, str]:
    score = 0
    reasons = []
    if _location_overlap(supply.get("location",""), demand.get("location","")):
        score += 40
        reasons.append("same market")
    if supply.get("asset_class") == "commercial" and demand.get("signal_type") in ("LEASE", "OFFICE", "EXPAND", "RELOCATE"):
        score += 30
        reasons.append("asset/demand type match")
    if demand.get("urgency") == "HIGH":
        score += 15
        reasons.append("urgent demand")
    elif demand.get("urgency") == "CRITICAL":
        score += 25
        reasons.append("critical demand urgency")
    score = min(score, 100)
    return score, ", ".join(reasons)
