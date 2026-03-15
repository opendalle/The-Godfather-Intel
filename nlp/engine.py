"""
nlp/engine.py
======================
CRE intelligence engine. Reads between the lines of regulatory filings, RSS articles, and incorporation
records to extract genuine CRE demand signals.

Philosophy:
- A filing that explicitly says "office" is easy — any keyword match gets it.
- The real edge: catch signals that IMPLY space requirements without saying so.
  * Series B+ funding → headcount will double → need space
  * 250+ LinkedIn jobs in one city → facility expansion within 12 months
  * New subsidiary incorporated → first office requirement
  * DRHP filed for ₹1000Cr+ IPO → HQ upgrade + compliance office mandatory
  * Relocation of registered office → active real estate decision underway
  * Regulation 30 disclosure of new business vertical → new location likely
  * Acquisition of a company in another city → integration space needed
  * ESOP/ESOPs disclosed → headcount growing → space pressure
  * Board resolution for capex > ₹50Cr → physical asset expansion
  * MOU with state government for manufacturing/IT park → large space commitment

- A filing that does NOT imply CRE:
  * Dividend announcement
  * Quarterly results (unless revenue scale implies space need)
  * Insider trading disclosure (Reg 3/Reg 7)
  * Credit rating change
  * Change in auditor
  * Promoter shareholding
  * Scheme of arrangement (pure financial)
  * Rights issue / preferential allotment (unless paired with expansion)

Signal scoring (0–100):
  - Direct CRE mention (lease, office, premises, sqft, area): +40
  - Implied expansion trigger (funding, hiring surge, new subsidiary, IPO): +25
  - Location specificity (city/micromarket named): +10
  - Size signal (₹Cr raised, headcount #, sqft): +10
  - Recency (< 24h): +5 bonus; < 7 days: +3 bonus
  - Noise flags (dividend, results only, promoter, auditor): hard block, score=0
  - Generic IPO template with no company name: hard block
"""

import re
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger("nlp.engine")

# ─────────────────────────────────────────────────────────────────
# HARD BLOCK: filing types that are never CRE signals
# ─────────────────────────────────────────────────────────────────
HARD_BLOCK_PATTERNS = [
    # Insider trading / shareholding
    r"regulation\s+3\b",
    r"regulation\s+7\b",
    r"insider\s+trading",
    r"promoter.{0,20}(?:shareholding|stake|holding)",
    r"pledge(?:d|ing)?\s+shares",
    r"creati(?:on|ng)\s+of\s+pledge",
    r"invocation\s+of\s+pledge",
    # Pure financial
    r"dividend\s+(?:declared|paid|announcement|record\s+date)",
    r"(?:final|interim)\s+dividend",
    r"buyback\s+of\s+(?:shares|equity|securities)",
    r"rights?\s+issue\s+(?:of|for|to)",
    r"preferential\s+(?:allotment|issue)",
    r"bonus\s+shares?\s+(?:issu|declar)",
    # Auditor / compliance admin
    r"(?:change|appointment|resignation)\s+(?:of|in)\s+(?:statutory\s+)?auditor",
    r"statutory\s+auditor",
    # Credit / rating
    r"credit\s+rating\s+(?:assigned|revised|reaffirmed|upgraded|downgraded)",
    r"rating\s+(?:action|rationale)",
    # Board meeting notices (unless expansion agenda)
    r"notice\s+of\s+(?:board|egm|agm)\s+meeting(?!\s+to\s+(?:approve|consider).{0,60}(?:expan|acqui|new\s+(?:office|facility|plant|unit|subsidiar)))",
    r"outcome\s+of\s+board\s+meeting(?!\s+.{0,60}(?:expan|acqui|new\s+(?:office|facility|plant|subsidiar|location|premises)))",
    # Quarterly results only
    r"(?:standalone|consolidated)\s+(?:unaudited\s+)?financial\s+results?\s+for\s+the\s+(?:quarter|half)",
    # Scheme of arrangement (pure financial restructuring)
    r"scheme\s+of\s+(?:arrangement|amalgamation|merger|demerger)(?!\s+.{0,80}(?:new\s+(?:office|facility|premises|location|city)))",
    # Generic IPO spam template
    r"^ipo\s*$",
    r"company_name.*ipo.*ipo.*ipo",
]

BLOCK_RE = re.compile("|".join(HARD_BLOCK_PATTERNS), re.IGNORECASE | re.DOTALL)

# ─────────────────────────────────────────────────────────────────
# DIRECT CRE SIGNALS — explicit space language
# ─────────────────────────────────────────────────────────────────
DIRECT_CRE = [
    r"\b(?:lease|leased|leasing|sub-lease)\b",
    r"\b(?:office|offices)\s+(?:space|premises|building|tower|park|complex|floor|campus)",
    r"\b(?:new|additional|registered|corporate|head)\s+(?:office|headquarters|hq)\b",
    r"\bpremises\s+(?:at|in|located|situated)",
    r"\bsq\.?\s*ft\.?|\bsqft\b|\bsquare\s+feet\b",
    r"\bcommercial\s+(?:space|property|premises|building|floor|unit)",
    r"\bit\s+(?:park|campus|sez)\b",
    r"\btechnology\s+(?:park|campus|hub)\b",
    r"\bco-working\s+space\b",
    r"\bfacility\s+(?:at|in|located|across)\b.{0,40}(?:mumbai|bengaluru|bangalore|hyderabad|pune|chennai|delhi|ncr|gurgaon|noida)",
    r"\bregistered\s+office\s+(?:shift|change|relocation|moved|transfer)",
    r"\bnew\s+(?:branch|location|centre|center)\s+(?:at|in|opened|inaugurated)",
    r"\bdata\s+(?:centre|center)\b",
    r"\bwarehouse\s+(?:at|in|lease|space)\b",
    r"\bfulfillment\s+(?:centre|center)\b",
]

DIRECT_CRE_RE = re.compile("|".join(DIRECT_CRE), re.IGNORECASE)

# ─────────────────────────────────────────────────────────────────
# IMPLIED EXPANSION TRIGGERS — "read between the lines"
# ─────────────────────────────────────────────────────────────────

# Funding events → headcount plan → space need (12–18 month lag)
FUNDING_TRIGGER = re.compile(
    r"(?:series\s+[a-g]|seed\s+round|pre-series|growth\s+capital|venture\s+(?:debt|funding)|"
    r"raised?\s+(?:rs\.?\s*|₹|inr\s*)[\d,.]+\s*(?:cr(?:ore)?|lakh|mn|million|bn|billion)|"
    r"funding\s+(?:round|of|raised)|"
    r"investors?\s+(?:infused|committed|invested|deployed|poured)\s+(?:rs\.?\s*|₹)[\d,.]+|"
    r"investment\s+of\s+(?:rs\.?\s*|₹)[\d,.]+\s*(?:cr(?:ore)?|lakh))",
    re.IGNORECASE,
)

# IPO/listing → mandatory HQ upgrade + compliance space
IPO_TRIGGER = re.compile(
    r"(?:filed?\s+(?:drhp|red\s+herring\s+prospectus)|"
    r"ipo\s+(?:size|proceeds|filing|approval|planned|slated)|"
    r"initial\s+public\s+offer(?:ing)?|"
    r"listed?\s+on\s+(?:bse|nse|stock\s+exchange)|"
    r"(?:bse|nse)\s+listing\s+(?:approval|date|ceremony)|"
    r"sebi\s+(?:approves?|cleared?|gave.{0,10}nod)\s+(?:ipo|listing))",
    re.IGNORECASE,
)

# New subsidiary / branch → first office or integration space
SUBSIDIARY_TRIGGER = re.compile(
    r"(?:incorporated|formed|established|set\s+up|registered|floated)\s+.{0,30}"
    r"(?:subsidiary|wholly.{0,5}owned|step-down|joint\s+venture|special\s+purpose\s+vehicle|spv|"
    r"private\s+limited|limited\s+liability\s+partnership|llp)|"
    r"new\s+(?:subsidiary|entity|company|venture|division)\s+(?:in|at|for)\s+"
    r"(?:mumbai|bengaluru|bangalore|hyderabad|pune|chennai|delhi|ncr|india|maharashtra|karnataka|telangana)",
    re.IGNORECASE,
)

# Headcount / hiring surge → space pressure within 12 months
HIRING_TRIGGER = re.compile(
    r"(?:hiring|recruit|appoint)\s+(?:[\d,]+\s+(?:employees?|professionals?|people|staff|engineers?|associates?)|"
    r"aggressively|rapidly|at\s+scale)|"
    r"headcount\s+(?:to\s+reach|growing\s+to|expansion\s+to|increase\s+to|doubled?|trebled?)\s+[\d,]+|"
    r"workforce\s+(?:expansion|grew|growing|will\s+reach)\s+[\d,]+|"
    r"[\d,]+\s+(?:new\s+)?(?:jobs?|positions?|roles?|hires?)\s+(?:in|at|across)\s+(?:india|mumbai|bengaluru|bangalore|hyderabad|pune|chennai|delhi|ncr)",
    re.IGNORECASE,
)

# GCC / global capability center setup → large office requirement
GCC_TRIGGER = re.compile(
    r"(?:global\s+capability\s+cent(?:er|re)|gcc|"
    r"global\s+(?:delivery|technology|engineering|shared\s+services)\s+cent(?:er|re)|"
    r"captive\s+(?:centre|center|unit|hub)|"
    r"india\s+(?:centre|center|hub|campus|operations?)\s+(?:for|of)\s+.{3,40}(?:global|international|worldwide)|"
    r"set\s+(?:up|ting\s+up)\s+.{0,20}(?:center|centre|hub|campus)\s+in\s+india)",
    re.IGNORECASE,
)

# Acquisition of another company → integration / consolidation space
ACQUISITION_TRIGGER = re.compile(
    r"(?:acqui(?:red?|ring|sition\s+of)|"
    r"merger\s+with\s+.{3,40}(?:pvt|ltd|private|limited|inc)|"
    r"takeover\s+of\s+.{3,40}(?:pvt|ltd|private|limited)|"
    r"strategic\s+(?:acquisition|investment|buyout))\s+.{0,40}"
    r"(?:mumbai|bengaluru|bangalore|hyderabad|pune|chennai|delhi|ncr|india)",
    re.IGNORECASE,
)

# Registered office change → active real estate decision
REGD_OFFICE_CHANGE = re.compile(
    r"(?:change|shift|relocation|transfer)\s+(?:of|in)\s+registered\s+office|"
    r"registered\s+office\s+(?:changed|shifted|relocated|moved|transferred)\s+(?:to|from)",
    re.IGNORECASE,
)

# Large capex disclosure → physical infrastructure
CAPEX_TRIGGER = re.compile(
    r"capital\s+expenditure\s+of\s+(?:rs\.?\s*|₹)[\d,.]+\s*(?:cr(?:ore)?|lakh)|"
    r"capex\s+(?:of|worth|valued\s+at)\s+(?:rs\.?\s*|₹)[\d,.]+\s*(?:cr(?:ore)?|lakh)|"
    r"invest(?:ing|ment)\s+(?:rs\.?\s*|₹)[\d,.]+\s*(?:cr(?:ore)?|lakh)\s+in\s+"
    r"(?:new|greenfield|brownfield|expanding|setting\s+up)\s+"
    r"(?:facility|plant|unit|campus|office|data\s+cent(?:er|re)|warehouse)",
    re.IGNORECASE,
)

# New business vertical / product line → new dedicated space
NEW_VERTICAL_TRIGGER = re.compile(
    r"launch(?:ing|ed)?\s+(?:new\s+)?(?:business\s+vertical|division|segment|product\s+line|service\s+offering)\s+"
    r"(?:in|for|at|across)\s+(?:india|mumbai|bengaluru|bangalore|hyderabad|pune|chennai|delhi|ncr)|"
    r"(?:entering|foray\s+into|expanding\s+into)\s+.{3,40}(?:market|segment|business)\s+"
    r"(?:in|across)\s+india",
    re.IGNORECASE,
)

# Government MOU / policy → large space commitment
GOVT_MOU_TRIGGER = re.compile(
    r"(?:signed?|executed?|entered\s+into)\s+(?:mou|memorandum\s+of\s+understanding|agreement)\s+"
    r"(?:with|for)\s+.{0,40}(?:government|state|ministry|authority|board)\s+.{0,40}"
    r"(?:(?:it|tech(?:nology)?|industrial|manufacturing|data\s+cent(?:re|er))\s+(?:park|hub|zone|cluster|corridor))|"
    r"(?:government|state|ministry|authority)\s+.{0,30}mou\s+.{0,30}"
    r"(?:land|space|premises|facility|campus)",
    re.IGNORECASE,
)

# ─────────────────────────────────────────────────────────────────
# LOCATION EXTRACTOR
# ─────────────────────────────────────────────────────────────────
CITY_PATTERNS = {
    "Mumbai": r"\b(?:mumbai|bombay|bkc|lower\s+parel|worli|andheri|malad|goregaon|powai|bandra|kurla|vikhroli|thane|navi\s+mumbai|airoli|belapur|kharghar|wadala|dadar)\b",
    "Bengaluru": r"\b(?:bengaluru|bangalore|whitefield|electronic\s+city|sarjapur|koramangala|hsr\s+layout|indiranagar|outer\s+ring\s+road|orr|manyata|hebbal)\b",
    "Hyderabad": r"\b(?:hyderabad|hitec\s+city|gachibowli|madhapur|kondapur|financial\s+district|cyberabad)\b",
    "Pune": r"\b(?:pune|hinjewadi|wakad|baner|viman\s+nagar|kharadi|magarpatta|hadapsar)\b",
    "Chennai": r"\b(?:chennai|tidel\s+park|perungudi|sholinganallur|omr|mount\s+road|nungambakkam)\b",
    "NCR": r"\b(?:delhi|ncr|gurgaon|gurugram|noida|greater\s+noida|cyber\s+city|sohna\s+road|cyber\s+hub)\b",
    "India": r"\b(?:pan.india|across\s+india|all\s+india|india-wide|india)\b",
}

CITY_RE = {city: re.compile(pat, re.IGNORECASE) for city, pat in CITY_PATTERNS.items()}

# ─────────────────────────────────────────────────────────────────
# MONEY / SIZE EXTRACTOR
# ─────────────────────────────────────────────────────────────────
MONEY_RE = re.compile(
    r"(?:rs\.?\s*|₹|inr\s*)([\d,]+(?:\.\d+)?)\s*(cr(?:ore)?|lakh|mn|million|bn|billion|k)",
    re.IGNORECASE,
)
SQFT_RE = re.compile(
    r"([\d,]+(?:\.\d+)?)\s*(?:lakh\s+)?(?:sq\.?\s*ft\.?|sqft|square\s+feet)",
    re.IGNORECASE,
)
HEADCOUNT_RE = re.compile(
    r"([\d,]+)\s+(?:employees?|professionals?|people|staff|engineers?|associates?|hires?|jobs?|positions?|roles?)",
    re.IGNORECASE,
)


def normalize_money(val_str: str, unit: str) -> Optional[float]:
    """Return value in crore."""
    try:
        v = float(val_str.replace(",", ""))
        u = unit.lower()
        if u in ("cr", "crore"):
            return v
        elif u == "lakh":
            return v / 100
        elif u in ("mn", "million"):
            return v * 0.012  # approx INR mn to Cr
        elif u in ("bn", "billion"):
            return v * 12.0
        elif u == "k":
            return v / 100000
        return v
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────
# SIGNAL TYPE CLASSIFIER
# ─────────────────────────────────────────────────────────────────
def classify_signal_type(text: str) -> str:
    """Map trigger pattern to demand signal_type."""
    t = text.lower()
    if DIRECT_CRE_RE.search(text):
        if re.search(r"\b(?:lease|leasing|sub-lease)\b", text, re.I):
            return "LEASE"
        if re.search(r"\b(?:office|hq|headquarters|campus|tower|park)\b", text, re.I):
            return "OFFICE"
        if re.search(r"\b(?:warehouse|fulfillment|logistics|cold\s+storage)\b", text, re.I):
            return "WAREHOUSE"
        if re.search(r"\bdata\s+cent(?:er|re)\b", text, re.I):
            return "DATACENTER"
        return "OFFICE"
    if GCC_TRIGGER.search(text):
        return "GCC"
    if REGD_OFFICE_CHANGE.search(text):
        return "RELOCATE"
    if SUBSIDIARY_TRIGGER.search(text):
        return "EXPAND"
    if ACQUISITION_TRIGGER.search(text):
        return "EXPAND"
    if FUNDING_TRIGGER.search(text):
        return "FUNDING"
    if IPO_TRIGGER.search(text):
        return "IPO_LISTING"
    if HIRING_TRIGGER.search(text):
        return "HIRING"
    if CAPEX_TRIGGER.search(text):
        return "EXPAND"
    if NEW_VERTICAL_TRIGGER.search(text):
        return "EXPAND"
    if GOVT_MOU_TRIGGER.search(text):
        return "EXPAND"
    return "SIGNAL"


def extract_location(text: str) -> str:
    """Return most specific city match, or 'India'."""
    for city, rx in CITY_RE.items():
        if city == "India":
            continue
        if rx.search(text):
            return city
    if CITY_RE["India"].search(text):
        return "India"
    return "India"


def extract_funding_cr(text: str) -> Optional[float]:
    m = MONEY_RE.search(text)
    if m:
        return normalize_money(m.group(1), m.group(2))
    return None


def extract_sqft(text: str) -> Optional[int]:
    m = SQFT_RE.search(text)
    if m:
        try:
            v = float(m.group(1).replace(",", ""))
            # If preceded by "lakh" it's already handled by regex
            return int(v)
        except Exception:
            pass
    return None


def extract_headcount(text: str) -> Optional[int]:
    m = HEADCOUNT_RE.search(text)
    if m:
        try:
            return int(m.group(1).replace(",", ""))
        except Exception:
            pass
    return None


# ─────────────────────────────────────────────────────────────────
# SCORE FUNCTION
# ─────────────────────────────────────────────────────────────────
def score_signal(
    company_name: str,
    headline: str,
    body: str = "",
    source: str = "",
    detected_at: Optional[datetime] = None,
) -> dict:
    """
    Return a dict with:
      is_cre: bool — should this be stored as a demand signal at all?
      confidence_score: int 0–100
      signal_type: str
      urgency: str CRITICAL|HIGH|MEDIUM|LOW
      location: str
      funding_amount_cr: float|None
      sqft_mentioned: int|None
      why_cre: str — human-readable reason
      suggested_action: str
    """
    full_text = f"{company_name} {headline} {body}"

    # ── 1. HARD BLOCK CHECK ──────────────────────────────────────
    # Block generic "IPO" company name with boilerplate headline
    if company_name.strip().upper() in ("IPO", "FUNDING", "HIRING", "SIGNAL", "UNKNOWN"):
        return _blocked("Generic placeholder company name — not a real signal")

    if BLOCK_RE.search(full_text):
        matched = BLOCK_RE.search(full_text)
        return _blocked(f"Noise pattern matched: '{matched.group()[:60]}'")

    # ── 2. TRIGGER DETECTION ────────────────────────────────────
    has_direct = bool(DIRECT_CRE_RE.search(full_text))
    has_funding = bool(FUNDING_TRIGGER.search(full_text))
    has_ipo = bool(IPO_TRIGGER.search(full_text))
    has_subsidiary = bool(SUBSIDIARY_TRIGGER.search(full_text))
    has_hiring = bool(HIRING_TRIGGER.search(full_text))
    has_gcc = bool(GCC_TRIGGER.search(full_text))
    has_acquisition = bool(ACQUISITION_TRIGGER.search(full_text))
    has_regd_change = bool(REGD_OFFICE_CHANGE.search(full_text))
    has_capex = bool(CAPEX_TRIGGER.search(full_text))
    has_vertical = bool(NEW_VERTICAL_TRIGGER.search(full_text))
    has_govt_mou = bool(GOVT_MOU_TRIGGER.search(full_text))

    trigger_count = sum([
        has_direct, has_funding, has_ipo, has_subsidiary, has_hiring,
        has_gcc, has_acquisition, has_regd_change, has_capex, has_vertical, has_govt_mou,
    ])

    if trigger_count == 0:
        return _blocked("No CRE trigger detected — filing has no expansion/space signal")

    # ── 3. BASE SCORE ────────────────────────────────────────────
    score = 0
    reasons = []
    actions = []

    if has_direct:
        score += 40
        reasons.append("Explicit space/lease language in filing")
        actions.append("Confirm requirement details and move to shortlisting")
    if has_gcc:
        score += 35
        reasons.append("GCC / global capability center setup confirmed")
        actions.append("Immediate: introduce Grade A 100k+ sqft options in target city")
    if has_regd_change:
        score += 30
        reasons.append("Registered office relocation — active real estate decision in progress")
        actions.append("Call directly — they are in the market right now")
    if has_ipo:
        score += 28
        reasons.append("IPO/DRHP filing — HQ upgrade and compliance space required within 30–90 days of listing")
        actions.append("Reach out within 30 days — IPO window is the ideal entry for office conversation")
    if has_acquisition:
        score += 25
        reasons.append("Acquisition in target geography — integration / consolidation space likely within 6 months")
        actions.append("Introduce co-location options near acquired entity's current premises")
    if has_subsidiary:
        score += 22
        reasons.append("New subsidiary incorporated — first office requirement imminent")
        actions.append("First-mover outreach — they have no incumbent broker for this entity")
    if has_capex:
        score += 22
        reasons.append("Large capex disclosed for new facility / plant / campus")
        actions.append("Introduce pre-leased or BTS options in announced geography")
    if has_funding:
        score += 20
        reasons.append("Funding round — headcount expansion will drive space demand within 12–18 months")
        actions.append("Send CRE intro deck with Grade A availability in their primary city")
    if has_vertical:
        score += 18
        reasons.append("New business vertical / division — dedicated workspace likely required")
        actions.append("Position as strategic partner for new vertical's office setup")
    if has_govt_mou:
        score += 20
        reasons.append("Government MOU for tech/industrial park — large space commitment")
        actions.append("Connect with land acquisition / build-to-suit teams")
    if has_hiring:
        score += 15
        reasons.append("Significant hiring surge — space pressure within 12 months")
        actions.append("Route to leasing team with city-specific Grade A availability")

    # ── 4. MODIFIERS ────────────────────────────────────────────
    location = extract_location(full_text)
    if location != "India":
        score += 10
        reasons.append(f"City-specific: {location}")

    funding_cr = extract_funding_cr(full_text)
    if funding_cr:
        if funding_cr >= 500:
            score += 15
            reasons.append(f"Large raise: ₹{funding_cr:.0f}Cr — significant headcount/space implication")
        elif funding_cr >= 100:
            score += 10
            reasons.append(f"Meaningful raise: ₹{funding_cr:.0f}Cr")
        elif funding_cr >= 20:
            score += 5

    sqft = extract_sqft(full_text)
    if sqft:
        score += 10
        reasons.append(f"Explicit sqft mentioned: {sqft:,} sqft")

    headcount = extract_headcount(full_text)
    if headcount and headcount >= 100:
        score += 8
        reasons.append(f"Headcount scale: {headcount:,} people")

    # Multiple triggers compound = higher conviction
    if trigger_count >= 3:
        score = min(score + 10, 100)
        reasons.append("Multiple independent CRE triggers — high conviction")
    elif trigger_count == 2:
        score = min(score + 5, 100)

    # Recency bonus
    if detected_at:
        now = datetime.now(timezone.utc)
        if detected_at.tzinfo is None:
            detected_at = detected_at.replace(tzinfo=timezone.utc)
        age_hours = (now - detected_at).total_seconds() / 3600
        if age_hours < 24:
            score = min(score + 5, 100)
        elif age_hours < 168:
            score = min(score + 3, 100)

    score = max(0, min(score, 100))

    # ── 5. URGENCY ───────────────────────────────────────────────
    if has_regd_change or (has_direct and score >= 70):
        urgency = "CRITICAL"
    elif has_ipo or has_gcc or has_direct or score >= 60:
        urgency = "HIGH"
    elif score >= 35:
        urgency = "MEDIUM"
    else:
        urgency = "LOW"

    # Low-score implied signals are still signals — don't block, just set LOW
    if score < 20 and not has_direct:
        return _blocked(f"Score {score} too low and no direct CRE language")

    # ── 6. COMPOSE WHY + ACTION ─────────────────────────────────
    signal_type = classify_signal_type(full_text)
    why_cre = "; ".join(reasons[:3])  # top 3 reasons
    suggested_action = actions[0] if actions else "Review and assess CRE opportunity"

    return {
        "is_cre": True,
        "confidence_score": score,
        "signal_type": signal_type,
        "urgency": urgency,
        "location": location,
        "funding_amount_cr": funding_cr,
        "sqft_mentioned": sqft,
        "headcount": headcount,
        "why_cre": why_cre,
        "suggested_action": suggested_action,
    }


def _blocked(reason: str) -> dict:
    logger.debug("Blocked: %s", reason)
    return {"is_cre": False, "block_reason": reason}


# ─────────────────────────────────────────────────────────────────
# DEDUP: IPO SPAM FILTER
# ─────────────────────────────────────────────────────────────────
_ipo_template_fingerprint = re.compile(
    r"ipo.{0,10}(?:listing|hq upgrade|compliance space required|window for office)",
    re.IGNORECASE,
)

def is_ipo_spam(company_name: str, why_cre: str) -> bool:
    """
    Returns True if this is the generic IPO template being stamped onto every
    IPO filing without real company intelligence.
    """
    if company_name.strip().upper() == "IPO":
        return True
    if _ipo_template_fingerprint.search(why_cre or ""):
        # Check if company name is actually the company or just "IPO"
        if len(company_name.strip()) < 4:
            return True
    return False
