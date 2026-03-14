"""
nlp/engine.py — Nexus Asia Intel NLP Engine
Combines the best of Distress Radar + Prop Intel NLP layers:
  - spaCy NER for company extraction (Prop Intel's approach)
  - Verb+noun signal combos (Prop Intel's signal_classifier)
  - Funding intent intelligence (Prop Intel's cre_intent)
  - Distress keyword detection + severity (Distress Radar's base.py)
  - Hard noise blocklist (Prop Intel's signal_classifier)
  - Indian location corpus (both repos merged)
"""
from __future__ import annotations
import re
import logging
from typing import Optional

logger = logging.getLogger("nexus.nlp")

# ─── Try loading spaCy — graceful fallback to regex if not installed ─────────
try:
    import spacy
    _nlp = spacy.load("en_core_web_sm")
    SPACY_AVAILABLE = True
except Exception:
    _nlp = None
    SPACY_AVAILABLE = False
    logger.warning("spaCy not available — using regex NER fallback")


# ═══════════════════════════════════════════════════════════════════════════
# DISTRESS SIGNALS (supply-side)
# ═══════════════════════════════════════════════════════════════════════════

DISTRESS_KEYWORDS = {
    "insolvency":       ["insolvency", "insolvent", "bankruptcy", "bankrupt"],
    "cirp":             ["cirp", "corporate insolvency resolution process", "resolution professional",
                         "nclt", "national company law tribunal", "ibc", "insolvency code", "ibbi"],
    "liquidation":      ["liquidation", "liquidator", "winding up", "wound up"],
    "sarfaesi":         ["sarfaesi", "symbolic possession", "physical possession", "secured creditor notice"],
    "default":          ["default", "defaulted", "npa", "non-performing", "stressed loan", "bad loan"],
    "distressed_asset": ["distressed asset", "distressed sale", "stressed asset"],
    "restructuring":    ["restructuring", "debt restructuring", "ots", "one time settlement", "haircut"],
    "debt_resolution":  ["debt resolution", "resolution plan", "settlement plan"],
    "creditor_action":  ["creditor action", "lender action", "debt recovery tribunal", "drt", "enforcement action"],
    "auction":          ["auction", "e-auction", "bank auction", "asset auction", "property auction", "reserve price"],
    "regulatory":       ["ibbi", "insolvency board", "resolution applicant"],
}

ALL_DISTRESS_KEYWORDS = [kw for kws in DISTRESS_KEYWORDS.values() for kw in kws]

SEVERITY_MAP = {
    "liquidation": "critical",
    "cirp": "critical",
    "sarfaesi": "high",
    "auction": "high",
    "insolvency": "high",
    "default": "medium",
    "distressed_asset": "medium",
    "restructuring": "medium",
    "debt_resolution": "medium",
    "creditor_action": "medium",
    "regulatory": "low",
    "financial_media": "low",
    "other": "low",
}

def detect_distress_keywords(text: str) -> list[tuple[str, str]]:
    """Scan text for distress keywords. Returns list of (keyword, category)."""
    text_lower = text.lower()
    found, seen = [], set()
    for category, keywords in DISTRESS_KEYWORDS.items():
        for kw in keywords:
            if kw in text_lower and kw not in seen:
                found.append((kw, category))
                seen.add(kw)
    return found

def get_severity(category: str) -> str:
    return SEVERITY_MAP.get(category, "medium")


# ═══════════════════════════════════════════════════════════════════════════
# DEMAND SIGNALS (demand-side)
# ═══════════════════════════════════════════════════════════════════════════

SIGNAL_COMBOS = [
    (["leased", "signed lease", "took up", "taken up", "rented", "rental agreement",
      "mou signed", "loi signed", "letter of intent", "signed a deal", "inked a deal"],
     ["office", "office space", "sq ft", "sqft", "square feet", "floor", "campus",
      "facility", "workspace", "co-working", "coworking", "commercial space", "premises",
      "tower", "block", "wing"],
     "LEASE", 80),
    (["opened", "opening", "inaugurated", "launched", "set up", "setting up",
      "new office", "new campus", "new hq", "new headquarters", "new facility",
      "new centre", "new center", "commissioned"],
     ["office", "campus", "headquarters", "hq", "facility", "centre", "center",
      "tech park", "it park", "workspace", "co-working"],
     "OFFICE", 75),
    (["relocated", "relocating", "relocation", "moved to", "moving to", "shifted to",
      "shifting to", "new address", "new location", "changed office", "new premises"],
     ["office", "campus", "hq", "headquarters", "facility", "premises", "workspace"],
     "RELOCATE", 70),
    (["expanding", "expansion", "expand", "scaling up", "additional space", "more space",
      "extra space", "doubling", "tripling", "new wing", "annex", "additional floor"],
     ["office", "campus", "facility", "space", "sq ft", "sqft", "square feet",
      "seats", "workstation", "premises"],
     "EXPAND", 65),
    (["hiring", "recruiting", "recruitment", "headcount", "workforce", "employees",
      "team size", "adding", "onboarding"],
     ["bengaluru", "mumbai", "hyderabad", "pune", "delhi", "ncr", "gurugram",
      "noida", "chennai", "india"],
     "HIRING", 40),
    (["raised", "funding", "series a", "series b", "series c", "series d",
      "pre-series", "seed round", "investment", "backed by", "crore raised",
      "mn raised", "million raised", "unicorn"],
     ["startup", "company", "firm", "platform", "ventures", "technologies",
      "solutions", "india"],
     "FUNDING", 35),
    (["data center", "datacenter", "colocation", "hyperscale", "edge datacenter"],
     ["mumbai", "india", "navi mumbai", "pune", "bengaluru"],
     "DATACENTER", 70),
    (["warehouse", "fulfillment centre", "logistics hub", "cold storage", "3pl",
      "dark store", "distribution centre"],
     ["mumbai", "bhiwandi", "navi mumbai", "pune", "nhava sheva", "jnpt"],
     "WAREHOUSE", 65),
]

HARD_NOISE = [
    r'\bsong\b', r'\balbum\b', r'\bfilm\b', r'\bmovie\b', r'\bactor\b', r'\bactress\b',
    r'\bbollywood\b', r'\blyric', r'\bviral video\b', r'\bcelebrity\b',
    r'\bcrude oil\b', r'\bpetroleum\b', r'\bopec\b', r'\bbrent\b', r'\boil price',
    r'\belection\b', r'\blok sabha\b', r'\brajya sabha\b', r'\bbjp\b',
    r'\bsensex\b', r'\bnifty\b', r'\bshare price\b', r'\bstock market\b',
    r'\brepo rate\b', r'\bmonetary policy\b',
    r'\bcricket\b', r'\bipl\b', r'\bfifa\b',
]

INDIA_LOCATIONS = [
    "mumbai", "navi mumbai", "thane", "pune", "delhi", "ncr", "gurugram", "gurgaon",
    "noida", "bengaluru", "bangalore", "hyderabad", "chennai", "kolkata", "ahmedabad",
    "surat", "jaipur", "lucknow", "india", "indian", "bkc", "lower parel", "andheri",
    "powai", "whitefield", "hsr layout", "koramangala", "cyberabad", "hitec city",
    "gachibowli", "bandra", "worli", "nariman point", "vikhroli", "goregaon", "malad",
    "belapur", "airoli", "bhiwandi", "electronic city", "sarjapur", "bellandur",
    "cybercity", "dlf", "unitech", "kharadi", "hinjewadi", "magarpatta", "wakad",
    "chandigarh", "mohali", "kochi", "indore", "nagpur", "gift city",
]

FUNDING_ROUNDS = {
    "seed": {"urgency": "LOW", "confidence": 35, "reason": "Early-stage — first proper office in 6-12 months"},
    "pre-series a": {"urgency": "MEDIUM", "confidence": 45, "reason": "Pre-Series A — co-working likely insufficient"},
    "series a": {"urgency": "MEDIUM", "confidence": 55, "reason": "Series A — 20-50 person team, need dedicated office"},
    "series b": {"urgency": "HIGH", "confidence": 70, "reason": "Series B — aggressive hiring, space requirement imminent"},
    "series c": {"urgency": "HIGH", "confidence": 75, "reason": "Series C — multi-city expansion planned"},
    "series d": {"urgency": "HIGH", "confidence": 80, "reason": "Series D+ — large campus or HQ upgrade likely"},
    "pre-ipo": {"urgency": "HIGH", "confidence": 85, "reason": "Pre-IPO — prestigious address needed before listing"},
    "ipo": {"urgency": "HIGH", "confidence": 85, "reason": "IPO/listing — HQ upgrade and compliance space required"},
}

def _has_noise(text: str) -> bool:
    hits = sum(1 for pat in HARD_NOISE if re.search(pat, text, re.IGNORECASE))
    return hits >= 2

def _check_sqft(text: str) -> bool:
    return bool(re.search(r'\d[\d,]*\s*(?:sq\.?\s*ft|sqft|square\s*feet|lakh\s*sq|crore\s*sq)', text, re.IGNORECASE))

def _parse_funding_cr(text: str) -> float:
    text = text.lower()
    patterns = [
        (r'(?:rs\.?|inr|₹)\s*([\d,]+(?:\.\d+)?)\s*crore', 1.0),
        (r'(?:rs\.?|inr|₹)\s*([\d,]+(?:\.\d+)?)\s*cr\b', 1.0),
        (r'(?:rs\.?|inr|₹)\s*([\d,]+(?:\.\d+)?)\s*lakh', 0.01),
        (r'\$\s*([\d,]+(?:\.\d+)?)\s*million', 8.3),
        (r'\$\s*([\d,]+(?:\.\d+)?)\s*mn', 8.3),
        (r'\$\s*([\d,]+(?:\.\d+)?)\s*billion', 8300.0),
        (r'([\d,]+(?:\.\d+)?)\s*crore', 1.0),
    ]
    for pattern, multiplier in patterns:
        m = re.search(pattern, text)
        if m:
            try:
                return float(m.group(1).replace(",", "")) * multiplier
            except ValueError:
                pass
    return 0.0

def classify_demand_signal(article: dict) -> Optional[dict]:
    """
    Returns a signal dict or None if not CRE-relevant.
    Combines Prop Intel's verb+noun combo logic with sqft shortcut.
    """
    title   = (article.get("title") or "").lower()
    summary = (article.get("summary") or "").lower()
    text    = (title + " " + summary + " " + (article.get("text") or "")[:2000]).lower()

    if _has_noise(text):
        return None

    sqft_hit     = _check_sqft(text)
    location_hit = any(loc in text for loc in INDIA_LOCATIONS)

    # Check for funding round intent first
    for round_name, round_data in FUNDING_ROUNDS.items():
        if round_name in text:
            funding_cr = _parse_funding_cr(text)
            score = round_data["confidence"]
            if funding_cr >= 100:
                score = min(score + 25, 100)
            elif funding_cr >= 50:
                score = min(score + 15, 100)
            if location_hit:
                score = min(score + 10, 100)
            return {
                "signal_type": "FUNDING",
                "confidence": score,
                "urgency": round_data["urgency"],
                "why_cre": round_data["reason"],
                "suggested_action": "Reach out within 30 days — window for office conversations is now",
                "funding_amount_cr": funding_cr,
                "location_hit": location_hit,
                "sqft_mentioned": sqft_hit,
                "matched_phrases": [round_name],
            }

    best_match, best_score = None, 0
    for verbs, nouns, sig_type, base_score in SIGNAL_COMBOS:
        verb_hits = [v for v in verbs if v in text]
        noun_hits = [n for n in nouns if n in text]
        if not verb_hits or not noun_hits:
            continue
        score = base_score + len(verb_hits) * 5 + len(noun_hits) * 5
        if sqft_hit:
            score += 20
        if location_hit:
            score += 15
        score = min(score, 100)
        if score > best_score:
            best_score = score
            urgency = "HIGH" if score >= 70 else "MEDIUM" if score >= 50 else "LOW"
            best_match = {
                "signal_type": sig_type,
                "confidence": score,
                "urgency": urgency,
                "why_cre": f"{sig_type.lower()} signal detected via {', '.join(verb_hits[:2])}",
                "suggested_action": _get_suggested_action(sig_type),
                "sqft_mentioned": sqft_hit,
                "location_hit": location_hit,
                "matched_phrases": verb_hits[:3] + noun_hits[:3],
            }

    if not best_match and sqft_hit and location_hit:
        best_match = {
            "signal_type": "OFFICE",
            "confidence": 60,
            "urgency": "MEDIUM",
            "why_cre": "Sq ft measurement mentioned with India location — strong CRE signal",
            "suggested_action": "Investigate company for space requirements",
            "sqft_mentioned": True,
            "location_hit": True,
            "matched_phrases": ["sq ft", "india location"],
        }

    return best_match

def _get_suggested_action(signal_type: str) -> str:
    actions = {
        "LEASE":    "Immediate outreach — transaction is likely imminent, confirm size and micro-market",
        "OFFICE":   "Call within 48 hours — new office setup, recommend locations and availability",
        "RELOCATE": "Urgent: company is actively moving, offer shortlist of Grade A options",
        "EXPAND":   "Send market report with available Grade A spaces in their current location + adjacent micro-markets",
        "HIRING":   "Reach out in 30 days when hiring surge converts to space inquiry",
        "FUNDING":  "Send CRE intro deck + cap rate analysis — timing is right post-funding",
        "DATACENTER": "Connect with data center advisory team — specialist asset",
        "WAREHOUSE": "Route to industrial/logistics team",
    }
    return actions.get(signal_type, "Investigate further and qualify requirement")


# ═══════════════════════════════════════════════════════════════════════════
# ENTITY EXTRACTION (company names + locations)
# ═══════════════════════════════════════════════════════════════════════════

NOISE_ORGS = {
    "times of india", "economic times", "hindustan times", "the hindu",
    "business standard", "livemint", "mint", "ndtv", "cnbc", "bloomberg",
    "reuters", "pti", "ani", "inc42", "yourstory", "entrackr", "moneycontrol",
    "financial express", "business today",
    "rbi", "sebi", "mca", "bse", "nse", "rera", "mcgm", "bbmp",
    "government of india", "ministry", "supreme court", "high court",
    "income tax", "gst council", "ibbi", "nclt", "nclat", "drt",
    "india", "indian", "company", "startup", "firm", "group",
    "the company", "sources", "analysts", "experts",
}

KNOWN_CRE_COMPANIES = {
    "infosys", "tcs", "wipro", "hcl", "tech mahindra", "ltimindtree",
    "cognizant", "accenture", "ibm", "capgemini", "mphasis",
    "jpmorgan", "goldman sachs", "morgan stanley", "citi", "hsbc",
    "google", "amazon", "microsoft", "apple", "meta", "flipkart",
    "swiggy", "zomato", "paytm", "zepto", "blinkit", "meesho",
    "razorpay", "groww", "upstox", "zerodha", "cred",
}

COMPANY_PATTERNS = [
    r'\b([A-Z][A-Za-z\s&]+(?:Ltd\.?|Limited|Pvt\.?\s*Ltd\.?|Private\s+Limited|'
    r'Industries|Corporation|Corp\.?|Inc\.?|LLP|Holdings|Enterprises|'
    r'Infrastructure|Finance|Capital|Solutions|Technologies|Energy|Power|'
    r'Realty|Real\s*Estate|Steel|Cement|Chemicals|Pharma|Textiles))\b',
]

def extract_company_names(text: str) -> list[str]:
    """
    Best-effort company name extraction.
    Uses spaCy NER if available, falls back to regex patterns.
    """
    companies = []

    if SPACY_AVAILABLE and _nlp:
        try:
            doc = _nlp(text[:5000])
            for ent in doc.ents:
                if ent.label_ == "ORG":
                    name = ent.text.strip()
                    if len(name) < 3:
                        continue
                    if name.lower() in NOISE_ORGS:
                        continue
                    companies.append(name)
        except Exception as e:
            logger.warning(f"spaCy NER failed: {e}")

    # Always also run regex — spaCy misses many Indian company names
    for pattern in COMPANY_PATTERNS:
        matches = re.findall(pattern, text)
        companies.extend([m.strip() for m in matches if len(m.strip()) > 4])

    # Deduplicate, prefer known companies
    seen, unique = set(), []
    known_hits = [c for c in companies if c.lower() in KNOWN_CRE_COMPANIES]
    rest = [c for c in companies if c.lower() not in KNOWN_CRE_COMPANIES]
    for c in known_hits + rest:
        key = c.lower()
        if key not in seen and key not in NOISE_ORGS:
            seen.add(key)
            unique.append(c)
    return unique[:5]

def extract_location(text: str) -> Optional[str]:
    text_lower = text.lower()
    # Check specific micro-markets first
    MMR_MARKETS = [
        "bkc", "bandra kurla", "lower parel", "worli", "andheri", "powai",
        "malad", "goregaon", "kurla", "vikhroli", "thane", "navi mumbai",
        "airoli", "belapur", "kharghar", "vashi",
    ]
    for market in MMR_MARKETS:
        if market in text_lower:
            return market.title()
    for loc in INDIA_LOCATIONS:
        if loc in text_lower:
            return loc.title()
    return None

def extract_sqft(text: str) -> Optional[int]:
    m = re.search(r'([\d,]+)\s*(?:sq\.?\s*ft|sqft|square\s*feet)', text, re.IGNORECASE)
    if m:
        try:
            return int(m.group(1).replace(",", ""))
        except:
            pass
    # Handle "X lakh sq ft"
    m2 = re.search(r'([\d.]+)\s*lakh\s*sq', text, re.IGNORECASE)
    if m2:
        try:
            return int(float(m2.group(1)) * 100000)
        except:
            pass
    return None

def extract_price_crore(text: str) -> Optional[float]:
    price_re = re.compile(
        r'(?:rs\.?|₹|inr|reserve\s+price[:\s]*)?\s*([\d,]+(?:\.\d+)?)\s*(crore|cr\.?|lakh|lac)',
        re.IGNORECASE
    )
    m = price_re.search(text)
    if m:
        try:
            val = float(m.group(1).replace(",", ""))
            unit = m.group(2).lower()
            if "lakh" in unit or "lac" in unit:
                return round(val / 100, 2)
            return round(val, 2)
        except:
            pass
    return None
