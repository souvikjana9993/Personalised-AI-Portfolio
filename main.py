import logging
import asyncio

import uvicorn
from fastapi import FastAPI, HTTPException

from apscheduler.schedulers.background import BackgroundScheduler

from config import scheduler, REFRESH_INTERVAL_MINUTES, APP_CONFIG,SCRAPER_DATE_RANGE

from routers import zerodha_router, paytm_router, equity_router, nps_router

from scrapers.gmail_scraper import refresh_data as refresh_zerodha_data
from scrapers.paytm_scraper import refresh_data as refresh_paytm_data
from scrapers.equity_scraper import refresh_data as refresh_equity_data
from scrapers.nps_scraper import refresh_data as refresh_nps_data

from parsers.nps_parser import main as run_nps_parsing_coroutine
from parsers.zerodha_parser import main as run_zerodha_parsing_coroutine

logging.basicConfig()
logging.getLogger('apscheduler').setLevel(logging.DEBUG)

app = FastAPI(
    title=APP_CONFIG["title"],
    description=APP_CONFIG["description"],
    version=APP_CONFIG["version"],
)

app.include_router(zerodha_router.router, prefix="/zerodha", tags=["zerodha"])
app.include_router(paytm_router.router, prefix="/paytm", tags=["paytm"])
app.include_router(equity_router.router, prefix="/equity", tags=["equity"])
app.include_router(nps_router.router, prefix="/nps", tags=["equity"])

async def refresh_all_scrapers():
    try:
        # Pass date range to each scraper's refresh function
        start_date = SCRAPER_DATE_RANGE["start_date"]
        end_date = SCRAPER_DATE_RANGE["end_date"]

        # Refresh each scraper with date range
        refresh_nps_data(start_date=start_date, end_date=end_date)
        refresh_zerodha_data(start_date=start_date, end_date=end_date)
        refresh_paytm_data(start_date=start_date, end_date=end_date) 
        refresh_equity_data(start_date=start_date, end_date=end_date)

        # Run parsers with async tasks
        asyncio.create_task(run_nps_parsing_coroutine())
        asyncio.create_task(run_zerodha_parsing_coroutine())
        
        print("Finished refreshing all scrapers.")
    except Exception as e:
        print(f"Error during refresh: {e}")

@app.on_event("startup")
async def start_scheduler():
    """Starts the background scheduler to refresh data periodically."""
    scheduler.add_job(
        refresh_all_scrapers, "interval", minutes=REFRESH_INTERVAL_MINUTES
    )
    scheduler.start()
    # Run the refresh on startup as well
    await refresh_all_scrapers()

@app.on_event("shutdown")
def stop_scheduler():
    """Shuts down the background scheduler."""
    scheduler.shutdown()

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        loop="uvloop",
        workers=1,
    )