# NEXUS ASIA INTEL — The Godfather
### One platform. Both sides of every CRE deal in India.

> **Supply + Demand + Match on a single screen.**  
> No other CRE intelligence tool in India does this.

---

## What this is

Nexus Asia Intel unifies two previously separate repos:
- **Nexus Asia Distress Radar** → who is being *forced to sell* (supply-side)
- **Nexus Prop Intel** → who *needs space* (demand-side)

The merger creates the only Indian CRE platform that simultaneously tracks distressed sellers and active buyers, then **cross-matches them into deal opportunities automatically**.

---

## Architecture

```
nexus-asia-intel/
├── main.py                    # Master orchestrator (run everything from here)
├── requirements.txt
├── index.html                 # The Godfather dashboard
│
├── crawlers/
│   ├── base.py                # Unified base crawler with Firecrawl bypass
│   ├── supply.py              # All supply-side crawlers
│   │   ├── IBAPIAuctionCrawler          — RBI bank auction API
│   │   ├── BankAuctionsCoInCrawler      — Third-party aggregator
│   │   ├── MultiPSUBankCrawler          — 8 PSU bank portals
│   │   ├── DRTSARFAESICrawler           — DRT benches + NPA law firms
│   │   ├── IBBINCLTCrawler              — IBBI/NCLT Google News
│   │   ├── NARCLARCCrawler              — NARCL + 4 ARCs
│   │   └── FinancialMediaCrawler        — ET, BS, Mint, Moneycontrol
│   ├── demand.py              # All demand-side crawlers
│   │   ├── BSEFilingCrawler             — BSE JSON API (CRE-filtered)
│   │   ├── NSEFilingCrawler             — NSE announcements
│   │   ├── LinkedInHiringCrawler        — Job surge detection
│   │   ├── RSSNewsCrawler               — 12 RSS feeds
│   │   └── MCAIncorporationCrawler      — NEW: new company registrations
│   └── firecrawl_client.py    # Anti-bot bypass (Cloudflare, govt firewalls)
│
├── nlp/
│   ├── engine.py              # Unified NLP: spaCy NER + verb+noun combos + distress keywords
│   └── text_cleaner.py        # Dedup, normalize, junk filter
│
├── db/
│   ├── schema.sql             # Master Supabase schema (9 tables, 6 views)
│   └── client.py              # All DB operations (insert, upsert, deal matching)
│
├── enrichment/
│   └── engine.py              # Deal scoring + cap rate engine + cross-signal matching
│
├── notifier/
│   └── alerts.py              # Slack + Telegram + Email digest
│
├── scheduler/
│   └── cron.py                # Local deployment scheduler (APScheduler)
│
└── .github/workflows/
    └── crawl.yml              # GitHub Actions (runs every 30 minutes)
```

---

## Setup

### 1. Supabase
1. Create a new Supabase project at [supabase.com](https://supabase.com)
2. Run `db/schema.sql` in the SQL Editor
3. Copy your Project URL and anon key

### 2. Environment variables
Create a `.env` file (never commit this):
```bash
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_ANON_KEY=eyJ...
SUPABASE_SERVICE_ROLE_KEY=eyJ...  # for writes
FIRECRAWL_API_KEY=fc-...          # get at firecrawl.dev (500 free pages/month)
SLACK_WEBHOOK_URL=https://hooks.slack.com/...
ALERT_EMAIL_TO=you@firm.com
SMTP_HOST=smtp.gmail.com
SMTP_USER=you@gmail.com
SMTP_PASSWORD=app-password
TELEGRAM_BOT_TOKEN=...            # optional
TELEGRAM_CHAT_ID=...              # optional
```

### 3. Install
```bash
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

### 4. Run
```bash
# Full pipeline (supply + demand + enrichment + deal matching)
python main.py

# Specific crawler group
python main.py --group bank_auction
python main.py --group legal
python main.py --group arc
python main.py --group demand

# Test without writing to DB
python main.py --dry-run

# Enrichment + deal matching only
python main.py --enrich-only

# Send daily digest
python main.py --digest
```

### 5. GitHub Actions (automated)
1. Add all env vars as GitHub Secrets
2. Push to repo — Actions runs every 30 minutes automatically

### 6. Dashboard
Open `index.html` in any browser. Enter Supabase URL + anon key in the Config tab.

---

## Key Features vs Original Repos

| Feature | Distress Radar | Prop Intel | **Nexus Intel** |
|---|:---:|:---:|:---:|
| Bank auction crawlers (IBAPI, 8 PSU banks) | ✓ | — | ✓ |
| DRT + SARFAESI + NPA law firm feeds | ✓ | — | ✓ |
| NARCL + 4 ARC portfolio crawlers | ✓ | — | ✓ |
| Firecrawl anti-bot bypass | ✓ | — | ✓ |
| Cap rate engine (Mumbai micro-markets) | ✓ | — | ✓ |
| Pre-leased asset tracker | ✓ | — | ✓ |
| BSE/NSE filing crawler | — | ✓ | ✓ |
| LinkedIn hiring surge detection | — | ✓ | ✓ |
| spaCy NER company extraction | — | ✓ | ✓ |
| Verb+noun signal combos (NLP) | — | ✓ | ✓ |
| Funding intent intelligence | — | ✓ | ✓ |
| Noise blocklist (Bollywood, oil, politics) | — | ✓ | ✓ |
| MCA incorporation crawler | — | — | **NEW** |
| Cross-signal deal matching engine | — | — | **NEW** |
| Unified supply + demand dashboard | — | — | **NEW** |
| Deal match alerts (Slack + Telegram) | partial | — | **NEW** |
| Combined deal scoring model | — | — | **NEW** |

---

## The Edge

Every IPC and PE fund in India gets deal flow from **one side only**: either distressed sellers or active buyers. 

This platform is the only one tracking **both simultaneously** and matching them. When a bank puts up a SARFAESI notice for a 45,000 sqft commercial floor in Andheri and a fast-growing fintech just raised Series B, the match algorithm surfaces the opportunity before either party has made a call.

That is the Godfather's edge.

---

*Nexus Asia Intel v1.0 — Built for CRE professionals who want to be first.*
