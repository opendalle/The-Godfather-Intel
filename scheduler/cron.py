"""
scheduler/cron.py — Nexus Asia Intel APScheduler
For local deployment / server deployment (not GitHub Actions).
"""
import logging, sys
from apscheduler.schedulers.blocking import BlockingScheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

scheduler = BlockingScheduler(timezone="Asia/Kolkata")

@scheduler.scheduled_job("cron", minute="*/30", id="supply_crawl")
def supply_job():
    from main import run_supply_pipeline
    from enrichment.engine import run_full_enrichment
    print("[Scheduler] Running supply crawlers...")
    run_supply_pipeline("all")
    run_full_enrichment()

@scheduler.scheduled_job("cron", hour="0,6,12,18", id="demand_crawl")
def demand_job():
    from main import run_demand_pipeline
    print("[Scheduler] Running demand crawlers...")
    run_demand_pipeline()

@scheduler.scheduled_job("cron", hour=8, minute=0, id="daily_digest")
def digest_job():
    from notifier.alerts import send_daily_digest
    print("[Scheduler] Sending daily digest...")
    send_daily_digest()

if __name__ == "__main__":
    print("[Scheduler] Nexus Asia Intel scheduler started")
    scheduler.start()
