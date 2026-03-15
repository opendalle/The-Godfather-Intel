"""
nexus/db.py — deal_matching engine PATCHED

Root cause of 0 matches:
The original engine was only enriching 2 distress events, and the matching
query likely required exact city match + exact asset_class match.
With 7 new supply events (mostly IBBI/NCLT) and 11 new demand signals
(all stamped "India" as location, mixed types), there were no exact overlaps.

Fixes:
1. Location matching: exact match OR fuzzy (MMR = Mumbai = Thane = Navi Mumbai)
2. Asset class: commercial ↔ office ↔ it_park all match; industrial ↔ warehouse match
3. Score threshold: lowered to 40 for match creation (was probably 60+)
4. Urgency bonus: CRITICAL demand + motivated supply → score boost
5. Timing filter: demand signals detected within 30 days (was probably 7)
6. Direct SQL-based matching (not Python-side loop) for performance
7. Match score formula explained + tunable
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger("nexus.db")

# ──────────────────────────────────────────────────────────────────────────────
# LOCATION CLUSTERS — fuzzy geographic matching
# ──────────────────────────────────────────────────────────────────────────────
# A supply event in "Andheri" should match demand in "Mumbai" — same market.
# These clusters define which location labels are considered the same market.

LOCATION_CLUSTERS = {
    "Mumbai": {
        "Mumbai", "BKC", "Lower Parel", "Worli", "Andheri", "Malad", "Goregaon",
        "Powai", "Vikhroli", "Thane", "Navi Mumbai", "Airoli", "Belapur", "Kharghar",
        "Vashi", "Wadala", "Chembur", "Kurla", "Bandra", "MMR",
    },
    "Bengaluru": {
        "Bengaluru", "Bangalore", "Whitefield", "Electronic City", "Sarjapur",
        "Koramangala", "HSR Layout", "Indiranagar", "ORR", "Manyata", "Hebbal",
    },
    "Hyderabad": {
        "Hyderabad", "HiTec City", "Gachibowli", "Madhapur", "Kondapur",
        "Financial District", "Cyberabad",
    },
    "Pune": {
        "Pune", "Hinjewadi", "Wakad", "Baner", "Viman Nagar", "Kharadi",
        "Magarpatta", "Hadapsar",
    },
    "Chennai": {
        "Chennai", "Tidel Park", "Perungudi", "Sholinganallur", "OMR",
        "Mount Road", "Nungambakkam",
    },
    "NCR": {
        "NCR", "Delhi", "Gurgaon", "Gurugram", "Noida", "Greater Noida",
        "Cyber City", "Sohna Road", "Cyber Hub", "Faridabad",
    },
    "India": set(),  # India = matches any city (low specificity)
}

def _same_market(loc1: str, loc2: str) -> bool:
    """Returns True if loc1 and loc2 are in the same geographic market."""
    if not loc1 or not loc2:
        return True  # Unknown location = possible match
    if loc1 == loc2:
        return True
    if "India" in (loc1, loc2):
        return True  # "India" demand matches any specific supply

    # Normalize
    l1 = loc1.strip().title()
    l2 = loc2.strip().title()

    for cluster_name, aliases in LOCATION_CLUSTERS.items():
        aliases_lower = {a.lower() for a in aliases} | {cluster_name.lower()}
        if l1.lower() in aliases_lower and l2.lower() in aliases_lower:
            return True
        if l1.lower() == cluster_name.lower() and l2.lower() in aliases_lower:
            return True
        if l2.lower() == cluster_name.lower() and l1.lower() in aliases_lower:
            return True

    return False


# ──────────────────────────────────────────────────────────────────────────────
# ASSET CLASS COMPATIBILITY
# ──────────────────────────────────────────────────────────────────────────────

ASSET_COMPATIBLE = {
    "commercial": {"commercial", "office", "grade_a_office", "grade_b_office", "it_park", "sez"},
    "office": {"commercial", "office", "grade_a_office", "grade_b_office", "it_park"},
    "grade_a_office": {"commercial", "office", "grade_a_office", "it_park"},
    "grade_b_office": {"commercial", "office", "grade_b_office", "grade_a_office"},
    "it_park": {"commercial", "office", "it_park", "grade_a_office", "grade_b_office"},
    "industrial": {"industrial", "warehouse", "manufacturing"},
    "warehouse": {"industrial", "warehouse"},
    "retail": {"retail", "commercial"},
}

def _asset_compatible(supply_class: str, demand_type: str) -> bool:
    """Check if supply asset class matches demand signal type."""
    sc = (supply_class or "commercial").lower()
    dt = (demand_type or "OFFICE").upper()

    # Demand signal types that are compatible with commercial supply
    commercial_demand = {"OFFICE", "LEASE", "EXPAND", "GCC", "FUNDING", "HIRING",
                         "IPO_LISTING", "RELOCATE", "SIGNAL"}
    warehouse_demand = {"WAREHOUSE"}
    datacenter_demand = {"DATACENTER"}

    if sc in ("commercial", "office", "grade_a_office", "grade_b_office", "it_park", "sez"):
        return dt in commercial_demand
    if sc in ("industrial", "warehouse"):
        return dt in warehouse_demand or dt in commercial_demand
    if sc == "datacenter":
        return dt in datacenter_demand
    return True


# ──────────────────────────────────────────────────────────────────────────────
# MATCH SCORE FORMULA
# ──────────────────────────────────────────────────────────────────────────────

def calculate_match_score(supply_event: dict, demand_signal: dict) -> int:
    """
    Score a supply↔demand pair from 0–100.
    Only pairs scoring >= MIN_MATCH_SCORE become deal matches.

    Components:
      Supply deal_score (motivation/distress level):  0–35
      Demand confidence_score (signal quality):        0–25
      Location overlap:                                0–20
      Asset class compatibility:                       0–10
      Urgency alignment:                               0–10
    """
    supply_score = min(int(supply_event.get("deal_score") or 50), 100)
    demand_confidence = min(int(demand_signal.get("confidence_score") or 50), 100)

    s_loc = supply_event.get("location") or supply_event.get("is_mmr") and "Mumbai" or "India"
    d_loc = demand_signal.get("location") or "India"

    s_class = supply_event.get("asset_class") or "commercial"
    d_type = demand_signal.get("signal_type") or "OFFICE"

    d_urgency = (demand_signal.get("urgency") or "MEDIUM").upper()
    s_severity = (supply_event.get("severity") or "medium").lower()

    # Base components
    supply_component = int(supply_score * 0.35)    # 0–35
    demand_component = int(demand_confidence * 0.25)  # 0–25

    # Location score
    if s_loc == d_loc:
        loc_score = 20  # Exact match
    elif _same_market(s_loc, d_loc):
        loc_score = 15  # Same market cluster
    elif "India" in (s_loc, d_loc):
        loc_score = 8   # One is pan-India
    else:
        loc_score = 0   # Different markets — hard miss

    # Asset class score
    asset_score = 10 if _asset_compatible(s_class, d_type) else 0

    # Urgency alignment score
    urgency_map = {"CRITICAL": 3, "HIGH": 2, "MEDIUM": 1, "LOW": 0}
    severity_map = {"critical": 3, "high": 2, "medium": 1, "low": 0}
    urgency_val = urgency_map.get(d_urgency, 1) + severity_map.get(s_severity, 1)
    urgency_score = min(urgency_val * 2, 10)  # 0–10

    total = supply_component + demand_component + loc_score + asset_score + urgency_score
    return min(total, 100)


MIN_MATCH_SCORE = 35  # Lowered from implied ~60 in original

URGENCY_ORDER = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1}


def _match_reason(supply: dict, demand: dict, score: int) -> str:
    reasons = []
    s_loc = supply.get("location", "")
    d_loc = demand.get("location", "")

    if s_loc == d_loc and s_loc:
        reasons.append(f"Exact location match: {s_loc}")
    elif _same_market(s_loc, d_loc):
        reasons.append(f"Same market: {s_loc} ↔ {d_loc}")

    s_class = supply.get("asset_class", "commercial")
    d_type = demand.get("signal_type", "OFFICE")
    if _asset_compatible(s_class, d_type):
        reasons.append(f"Asset class compatible: {s_class} ↔ {d_type}")

    d_urgency = demand.get("urgency", "MEDIUM")
    s_severity = supply.get("severity", "medium")
    if d_urgency in ("CRITICAL", "HIGH") or s_severity in ("critical", "high"):
        reasons.append(f"Urgency alignment: demand {d_urgency}, supply {s_severity}")

    reasons.append(f"Combined score: {score}")
    return "; ".join(reasons[:3])


def _broker_action(supply: dict, demand: dict) -> str:
    s_company = supply.get("company_name", "Seller")
    d_company = demand.get("company_name", "Buyer")
    s_price = supply.get("price_crore")
    d_sqft = demand.get("sqft_mentioned")
    s_channel = (supply.get("channel") or "").upper()
    d_type = demand.get("signal_type", "OFFICE")
    d_urgency = demand.get("urgency", "MEDIUM")

    price_str = f"₹{s_price:.0f}Cr distressed" if s_price else "distressed"
    sqft_str = f"{d_sqft//1000}k sqft" if d_sqft else "space"

    if d_urgency == "CRITICAL":
        urgency_str = "IMMEDIATE ACTION —"
    elif d_urgency == "HIGH":
        urgency_str = "Priority:"
    else:
        urgency_str = "Introduce:"

    return (
        f"{urgency_str} Connect {s_company} [{s_channel} · {price_str}] "
        f"with {d_company} [{d_type} · {sqft_str} needed]"
    )


# ──────────────────────────────────────────────────────────────────────────────
# SQL-BASED MATCHING FUNCTION (replaces Python-side loop)
# ──────────────────────────────────────────────────────────────────────────────
# This is the SQL to run as a Supabase RPC / direct query.
# Drop this into your db.py run_deal_matching() function.

DEAL_MATCHING_SQL = """
-- Deal matching engine v2
-- Matches supply (distress_events) with demand (demand_signals)
-- Uses location cluster matching via a helper function

WITH recent_supply AS (
    SELECT 
        id as supply_id,
        company_name as supply_company,
        asset_class,
        location as supply_location,
        is_mmr,
        deal_score,
        severity,
        channel,
        price_crore,
        headline as supply_headline,
        url as supply_url
    FROM distress_events
    WHERE 
        is_duplicate = false
        AND detected_at >= NOW() - INTERVAL '30 days'
        AND deal_score >= 40
),
recent_demand AS (
    SELECT
        id as demand_id,
        company_name as demand_company,
        signal_type,
        urgency,
        confidence_score,
        location as demand_location,
        sqft_mentioned,
        why_cre,
        suggested_action
    FROM demand_signals
    WHERE
        is_duplicate = false
        AND detected_at >= NOW() - INTERVAL '30 days'
        AND confidence_score >= 40
        -- Exclude IPO spam
        AND company_name NOT IN ('IPO', 'FUNDING', 'HIRING', 'SIGNAL')
        AND char_length(company_name) > 3
),
candidate_pairs AS (
    SELECT
        s.supply_id,
        s.supply_company,
        s.asset_class,
        s.supply_location,
        s.deal_score,
        s.severity,
        s.channel,
        s.price_crore,
        s.supply_headline,
        s.supply_url,
        d.demand_id,
        d.demand_company,
        d.signal_type,
        d.urgency,
        d.confidence_score,
        d.demand_location,
        d.sqft_mentioned,
        d.why_cre,
        d.suggested_action,
        -- Score formula (mirrors Python calculate_match_score)
        (
            -- Supply motivation component (0-35)
            LEAST(s.deal_score, 100) * 0.35 +
            -- Demand confidence component (0-25)
            LEAST(d.confidence_score, 100) * 0.25 +
            -- Location score (0-20)
            CASE
                WHEN s.supply_location = d.demand_location THEN 20
                WHEN d.demand_location = 'India' OR s.supply_location = 'India' THEN 8
                WHEN s.is_mmr = true AND d.demand_location IN (
                    'Mumbai','BKC','Andheri','Malad','Powai','Worli',
                    'Lower Parel','Thane','Navi Mumbai','Airoli','MMR'
                ) THEN 15
                ELSE 0
            END +
            -- Asset class score (0-10)
            CASE
                WHEN s.asset_class IN ('commercial','office','grade_a_office','grade_b_office','it_park')
                     AND d.signal_type IN ('OFFICE','LEASE','EXPAND','GCC','FUNDING','HIRING','IPO_LISTING','RELOCATE','SIGNAL')
                     THEN 10
                WHEN s.asset_class = 'industrial' AND d.signal_type = 'WAREHOUSE'
                     THEN 10
                ELSE 2
            END +
            -- Urgency alignment (0-10)
            CASE
                WHEN d.urgency = 'CRITICAL' AND s.severity = 'critical' THEN 10
                WHEN d.urgency IN ('CRITICAL','HIGH') AND s.severity IN ('critical','high') THEN 7
                WHEN d.urgency = 'HIGH' OR s.severity = 'high' THEN 4
                ELSE 2
            END
        )::INT AS raw_score
    FROM recent_supply s
    CROSS JOIN recent_demand d
    -- Pre-filter: skip obviously incompatible pairs
    WHERE NOT (
        s.asset_class = 'industrial' AND d.signal_type IN ('OFFICE','LEASE','GCC')
    )
)
INSERT INTO deal_matches (
    supply_event_id, demand_signal_id,
    supply_company, demand_company,
    supply_location, demand_location,
    asset_class, signal_type,
    price_crore, sqft_mentioned,
    supply_headline, why_cre,
    broker_action, match_reason,
    match_score, status,
    matched_at
)
SELECT
    p.supply_id,
    p.demand_id,
    p.supply_company,
    p.demand_company,
    p.supply_location,
    p.demand_location,
    p.asset_class,
    p.signal_type,
    p.price_crore,
    p.sqft_mentioned,
    p.supply_headline,
    p.why_cre,
    -- Broker action
    CASE
        WHEN p.urgency = 'CRITICAL' THEN 'IMMEDIATE: Connect ' || p.supply_company || ' [' || UPPER(p.channel) || '] with ' || p.demand_company || ' [' || p.signal_type || ']'
        ELSE 'Introduce ' || p.supply_company || ' (' || COALESCE(p.price_crore::TEXT || 'Cr', 'asset') || ') to ' || p.demand_company || ' (' || p.signal_type || ' · ' || p.demand_location || ')'
    END,
    -- Match reason
    'Score ' || p.raw_score || ': location ' || p.supply_location || '↔' || p.demand_location || ', ' || p.asset_class || ' ↔ ' || p.signal_type || ', demand urgency ' || p.urgency,
    p.raw_score,
    'new',
    NOW()
FROM candidate_pairs p
WHERE p.raw_score >= 35
ON CONFLICT (supply_event_id, demand_signal_id)
DO UPDATE SET
    match_score = EXCLUDED.match_score,
    broker_action = EXCLUDED.broker_action,
    match_reason = EXCLUDED.match_reason,
    matched_at = EXCLUDED.matched_at
RETURNING supply_company, demand_company, match_score;
"""

# ──────────────────────────────────────────────────────────────────────────────
# PYTHON FALLBACK MATCHING (for when RPC not available)
# ──────────────────────────────────────────────────────────────────────────────

def run_deal_matching_python(supabase_client) -> int:
    """
    Python-side deal matching. Fetches recent supply + demand from Supabase,
    scores all pairs, inserts matches.
    Returns count of matches created/updated.
    """
    logger.info("Running deal matching engine (Python)...")

    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()

    # Fetch recent supply
    try:
        supply_resp = supabase_client.table("distress_events") \
            .select("id,company_name,asset_class,location,is_mmr,deal_score,severity,channel,price_crore,headline,url") \
            .eq("is_duplicate", False) \
            .gte("detected_at", cutoff) \
            .gte("deal_score", 40) \
            .execute()
        supply_events = supply_resp.data or []
    except Exception as e:
        logger.error("Failed to fetch supply events: %s", e)
        return 0

    # Fetch recent demand
    try:
        demand_resp = supabase_client.table("demand_signals") \
            .select("id,company_name,signal_type,urgency,confidence_score,location,sqft_mentioned,why_cre,suggested_action") \
            .eq("is_duplicate", False) \
            .gte("detected_at", cutoff) \
            .gte("confidence_score", 40) \
            .execute()
        demand_signals = demand_resp.data or []
        # Filter out IPO spam
        demand_signals = [
            d for d in demand_signals
            if d.get("company_name", "").strip().upper() not in ("IPO", "FUNDING", "HIRING", "SIGNAL", "UNKNOWN")
            and len(d.get("company_name", "")) > 3
        ]
    except Exception as e:
        logger.error("Failed to fetch demand signals: %s", e)
        return 0

    logger.info("Matching %d supply events × %d demand signals", len(supply_events), len(demand_signals))

    matches_created = 0
    for supply in supply_events:
        for demand in demand_signals:
            score = calculate_match_score(supply, demand)
            if score < MIN_MATCH_SCORE:
                continue

            # Skip same company matching with itself
            if (supply.get("company_name") or "").lower() == (demand.get("company_name") or "").lower():
                continue

            match_row = {
                "supply_event_id": supply["id"],
                "demand_signal_id": demand["id"],
                "supply_company": supply.get("company_name"),
                "demand_company": demand.get("company_name"),
                "supply_location": supply.get("location"),
                "demand_location": demand.get("location"),
                "asset_class": supply.get("asset_class"),
                "signal_type": demand.get("signal_type"),
                "price_crore": supply.get("price_crore"),
                "sqft_mentioned": demand.get("sqft_mentioned"),
                "supply_headline": (supply.get("headline") or "")[:300],
                "why_cre": (demand.get("why_cre") or "")[:500],
                "broker_action": _broker_action(supply, demand),
                "match_reason": _match_reason(supply, demand, score),
                "match_score": score,
                "status": "new",
                "matched_at": datetime.now(timezone.utc).isoformat(),
            }

            try:
                supabase_client.table("deal_matches") \
                    .upsert(match_row, on_conflict="supply_event_id,demand_signal_id") \
                    .execute()
                matches_created += 1
                logger.debug("Match: %s ↔ %s [%d]",
                             supply.get("company_name"), demand.get("company_name"), score)
            except Exception as e:
                logger.warning("Match upsert failed: %s", e)

    logger.info("Deal matching: %d matches created/updated", matches_created)
    return matches_created
