"""nlp/text_cleaner.py — text normalisation utilities"""
import re, hashlib

def clean_text(text: str) -> str:
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'<[^>]+>', ' ', text)
    return text.strip()

def normalize_company_name(name: str) -> str:
    if not name:
        return ""
    suffixes = [r'\bLtd\.?\b', r'\bLimited\b', r'\bPvt\.?\b', r'\bPrivate\b',
                r'\bInc\.?\b', r'\bCorp\.?\b', r'\bLLP\b', r'\bllp\b']
    for s in suffixes:
        name = re.sub(s, '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+', ' ', name).strip().lower()
    return name

def deduplicate(records: list, key: str = "url") -> list:
    seen, unique = set(), []
    for r in records:
        identifier = hashlib.md5((r.get(key) or "").encode()).hexdigest()
        if identifier not in seen:
            seen.add(identifier)
            unique.append(r)
    return unique

def is_junk_company(name: str) -> bool:
    JUNK = [
        "href=", "&#", "cin:", "dalal street", "5th floor", "limited cin",
        "stock exchange", "bse limited", "nse limited", "listing department",
        "p.j. tower", "g-block", "g block", "khasra", "plot ", "kisl/",
        "assemblies limited", "compliance officer", "registered office",
        "secretarial audit",
    ]
    name_lower = name.lower()
    return any(j in name_lower for j in JUNK) or len(name.strip()) < 3
