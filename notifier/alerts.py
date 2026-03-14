"""
notifier/alerts.py — Nexus Asia Intel multi-channel alert system

Alert channels:
  1. Slack webhook    — immediate deal alerts (deal_score >= 70 OR demand urgency=HIGH)
  2. Email digest     — daily summary of all new signals
  3. Telegram bot     — mobile push for critical alerts
  4. Deal match alert — when supply + demand cross-match is created

Triggers:
  - Commercial MMR asset, deal_score >= 70       → Slack + Telegram (immediate)
  - Bank auction, price 10–150Cr                 → Slack with auction date
  - DRT pre-auction signal                        → Slack early warning
  - ARC portfolio asset, commercial + MMR         → Slack
  - LEASE or EXPAND demand signal, confidence≥75  → Slack
  - New deal match, match_score >= 60             → Slack
  - Daily digest: all signals from last 24h       → Email
"""
from __future__ import annotations
import os, smtplib, logging, json
from datetime import datetime, timezone, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

import requests

from db.client import db_get

logger = logging.getLogger("nexus.notifier")

SLACK_WEBHOOK     = os.environ.get("SLACK_WEBHOOK_URL", "")
ALERT_EMAIL_TO    = os.environ.get("ALERT_EMAIL_TO", "")
SMTP_HOST         = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT         = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER         = os.environ.get("SMTP_USER", "")
SMTP_PASS         = os.environ.get("SMTP_PASSWORD", "")
TG_TOKEN          = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID        = os.environ.get("TELEGRAM_CHAT_ID", "")

SEVERITY_EMOJI = {"critical": "🔴", "high": "🟠", "medium": "🟡", "low": "⚪"}
URGENCY_EMOJI  = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "⚪"}


# ─── Slack ───────────────────────────────────────────────────────────────────

def _send_slack(blocks: list) -> bool:
    if not SLACK_WEBHOOK:
        return False
    try:
        r = requests.post(SLACK_WEBHOOK, json={"blocks": blocks}, timeout=10)
        return r.status_code == 200
    except Exception as e:
        logger.error(f"Slack send failed: {e}")
        return False

def _slack_supply_block(event: dict) -> list:
    emoji = SEVERITY_EMOJI.get(event.get("severity", "medium"), "⚪")
    score = event.get("deal_score", 0)
    price = f"₹{event['price_crore']}Cr" if event.get("price_crore") else "Price TBC"
    return [
        {"type": "header", "text": {"type": "plain_text", "text": f"{emoji} Supply Alert — Deal Score {score}"}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*Company:* {event.get('company_name', 'Unknown')}"},
            {"type": "mrkdwn", "text": f"*Location:* {event.get('location', 'Unknown')}"},
            {"type": "mrkdwn", "text": f"*Channel:* {event.get('channel', 'Unknown').replace('_', ' ').title()}"},
            {"type": "mrkdwn", "text": f"*Price:* {price}"},
            {"type": "mrkdwn", "text": f"*Asset Class:* {event.get('asset_class', 'Unknown')}"},
            {"type": "mrkdwn", "text": f"*MMR:* {'Yes ✓' if event.get('is_mmr') else 'No'}"},
        ]},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"_{event.get('headline', '')[:200]}_"}},
        {"type": "actions", "elements": [
            {"type": "button", "text": {"type": "plain_text", "text": "View Source"},
             "url": event.get("url", "#"), "style": "primary"},
        ]},
        {"type": "divider"},
    ]

def _slack_demand_block(signal: dict) -> list:
    emoji = URGENCY_EMOJI.get(signal.get("urgency", "MEDIUM"), "🟡")
    return [
        {"type": "header", "text": {"type": "plain_text", "text": f"{emoji} Demand Signal — {signal.get('signal_type')} ({signal.get('confidence_score')}% confidence)"}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*Company:* {signal.get('company_name', 'Unknown')}"},
            {"type": "mrkdwn", "text": f"*Location:* {signal.get('location', 'Unknown')}"},
            {"type": "mrkdwn", "text": f"*Urgency:* {signal.get('urgency')}"},
            {"type": "mrkdwn", "text": f"*Source:* {signal.get('data_source', 'RSS')}"},
        ]},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*Why CRE:* {signal.get('why_cre', '')}"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*Action:* {signal.get('suggested_action', '')}"}},
        {"type": "divider"},
    ]

def _slack_match_block(match: dict) -> list:
    return [
        {"type": "header", "text": {"type": "plain_text", "text": f"🤝 Deal Match — Score {match.get('match_score')}"}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*Supply:* {match.get('supply_company', 'Unknown')} ({match.get('asset_class', '')})"},
            {"type": "mrkdwn", "text": f"*Demand:* {match.get('demand_company', 'Unknown')} ({match.get('signal_type', '')})"},
            {"type": "mrkdwn", "text": f"*Location:* {match.get('supply_location', 'Unknown')}"},
            {"type": "mrkdwn", "text": f"*Match reason:* {match.get('match_reason', '')}"},
        ]},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*Broker Action:* {match.get('broker_action', '')}"}},
        {"type": "divider"},
    ]


# ─── Telegram ────────────────────────────────────────────────────────────────

def _send_telegram(message: str) -> bool:
    if not TG_TOKEN or not TG_CHAT_ID:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": message, "parse_mode": "Markdown"},
            timeout=10,
        )
        return r.status_code == 200
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")
        return False


# ─── Email digest ────────────────────────────────────────────────────────────

def _send_email(subject: str, html_body: str) -> bool:
    if not all([ALERT_EMAIL_TO, SMTP_HOST, SMTP_USER, SMTP_PASS]):
        return False
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = SMTP_USER
        msg["To"]      = ALERT_EMAIL_TO
        msg.attach(MIMEText(html_body, "html"))
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_USER, ALERT_EMAIL_TO.split(","), msg.as_string())
        return True
    except Exception as e:
        logger.error(f"Email send failed: {e}")
        return False

def _email_digest_html(supply_events: list, demand_signals: list, deal_matches: list) -> str:
    now = datetime.now(timezone.utc).strftime("%d %b %Y")
    rows_supply = "".join(
        f"<tr><td>{e.get('company_name','')}</td><td>{e.get('location','')}</td>"
        f"<td>{e.get('channel','').replace('_',' ').title()}</td>"
        f"<td>₹{e.get('price_crore','')}Cr</td><td>{e.get('deal_score','')}</td>"
        f"<td><a href='{e.get('url','')}'>View</a></td></tr>"
        for e in supply_events[:20]
    )
    rows_demand = "".join(
        f"<tr><td>{s.get('company_name','')}</td><td>{s.get('signal_type','')}</td>"
        f"<td>{s.get('urgency','')}</td><td>{s.get('location','')}</td>"
        f"<td>{s.get('confidence_score','')}</td></tr>"
        for s in demand_signals[:20]
    )
    rows_matches = "".join(
        f"<tr><td>{m.get('supply_company','')}</td><td>{m.get('demand_company','')}</td>"
        f"<td>{m.get('match_score','')}</td><td>{m.get('match_reason','')}</td>"
        f"<td>{m.get('broker_action','')}</td></tr>"
        for m in deal_matches[:10]
    )
    return f"""
    <html><head><style>
      body{{font-family:Arial,sans-serif;font-size:13px;color:#333}}
      h1{{color:#1a1a2e;font-size:18px}} h2{{color:#16213e;font-size:15px;margin-top:24px}}
      table{{border-collapse:collapse;width:100%}} th{{background:#1a1a2e;color:#fff;padding:6px 8px;text-align:left}}
      td{{padding:5px 8px;border-bottom:1px solid #eee}} tr:hover td{{background:#f9f9f9}}
    </style></head><body>
    <h1>Nexus Asia Intel — Daily Digest ({now})</h1>
    <p>Supply signals: <b>{len(supply_events)}</b> &nbsp;|&nbsp;
       Demand signals: <b>{len(demand_signals)}</b> &nbsp;|&nbsp;
       Deal matches: <b>{len(deal_matches)}</b></p>
    <h2>🔴 Top Supply Signals</h2>
    <table><tr><th>Company</th><th>Location</th><th>Channel</th><th>Price</th><th>Score</th><th>Link</th></tr>
    {rows_supply}</table>
    <h2>🟢 Top Demand Signals</h2>
    <table><tr><th>Company</th><th>Signal</th><th>Urgency</th><th>Location</th><th>Confidence</th></tr>
    {rows_demand}</table>
    <h2>🤝 Deal Matches</h2>
    <table><tr><th>Supply</th><th>Demand</th><th>Score</th><th>Reason</th><th>Action</th></tr>
    {rows_matches}</table>
    <p style="color:#999;font-size:11px;margin-top:24px">Nexus Asia Intel — automated CRE intelligence platform</p>
    </body></html>"""


# ─── Alert dispatcher ─────────────────────────────────────────────────────────

def alert_supply_event(event: dict):
    """Send immediate alert for a high-priority supply-side distress event."""
    deal_score = event.get("deal_score", 0)
    if deal_score < 70:
        return
    blocks = _slack_supply_block(event)
    _send_slack(blocks)
    if deal_score >= 85 or event.get("severity") == "critical":
        msg = (
            f"🔴 *CRITICAL SUPPLY ALERT*\n"
            f"Company: {event.get('company_name')}\n"
            f"Location: {event.get('location')} | Score: {deal_score}\n"
            f"Channel: {event.get('channel')}\n"
            f"{event.get('headline','')[:200]}"
        )
        _send_telegram(msg)

def alert_demand_signal(signal: dict):
    """Send alert for high-urgency demand signal."""
    if signal.get("urgency") not in ("HIGH", "CRITICAL"):
        return
    if (signal.get("confidence_score") or 0) < 70:
        return
    _send_slack(_slack_demand_block(signal))

def alert_deal_match(match: dict):
    """Alert when a meaningful supply-demand match is created."""
    if (match.get("match_score") or 0) < 60:
        return
    _send_slack(_slack_match_block(match))
    msg = (
        f"🤝 *DEAL MATCH — Score {match.get('match_score')}*\n"
        f"Supply: {match.get('supply_company')}\n"
        f"Demand: {match.get('demand_company')}\n"
        f"Action: {match.get('broker_action','')}"
    )
    _send_telegram(msg)

def send_daily_digest():
    """Compile and send the daily email + Slack digest."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    supply   = db_get("distress_events",  {"detected_at": f"gte.{cutoff}", "is_duplicate": "eq.false", "order": "deal_score.desc", "limit": "30"})
    demand   = db_get("demand_signals",   {"detected_at": f"gte.{cutoff}", "is_duplicate": "eq.false", "order": "confidence_score.desc", "limit": "30"})
    matches  = db_get("v_deal_matches_full", {"matched_at": f"gte.{cutoff}", "order": "match_score.desc", "limit": "20"})

    # Email
    html = _email_digest_html(supply, demand, matches)
    _send_email(f"Nexus Asia Intel Daily Digest — {datetime.now().strftime('%d %b %Y')}", html)

    # Slack summary block
    summary_blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": f"📊 Daily Digest — {datetime.now().strftime('%d %b %Y')}"}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*Supply signals:* {len(supply)}"},
            {"type": "mrkdwn", "text": f"*Demand signals:* {len(demand)}"},
            {"type": "mrkdwn", "text": f"*Deal matches:* {len(matches)}"},
            {"type": "mrkdwn", "text": f"*Top deal score:* {supply[0].get('deal_score') if supply else 'N/A'}"},
        ]},
        {"type": "divider"},
    ]
    if supply:
        summary_blocks.extend(_slack_supply_block(supply[0]))
    if matches:
        summary_blocks.extend(_slack_match_block(matches[0]))
    _send_slack(summary_blocks)
    logger.info(f"Daily digest sent: {len(supply)} supply, {len(demand)} demand, {len(matches)} matches")
