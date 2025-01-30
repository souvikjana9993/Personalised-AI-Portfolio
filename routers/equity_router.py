from fastapi import APIRouter, Query, HTTPException
from schemas.equity_schemas import ScrapeRequest, default_start_date, default_end_date
from scrapers.equity_scraper import get_emails_by_subject, refresh_data
from config import SCRAPER_CONFIG
from datetime import date
import os

router = APIRouter()

@router.get("/scrape/")
async def scrape_emails(
    email_id: str = Query(..., description="The Gmail address to scrape"),
    start_date: date = Query(default=default_start_date()),
    end_date: date = Query(default=default_end_date())
):
    try:
        subject_substring = SCRAPER_CONFIG["equity"]["subject_substring"]
        output_dir = get_emails_by_subject(email_id, subject_substring, start_date, end_date)
        
        if output_dir and os.path.exists(output_dir):
            files = [f for f in os.listdir(output_dir) if f.endswith('.pdf')]
            return {
                "message": f"Successfully saved {len(files)} PDF files",
                "directory": output_dir,
                "files": files
            }
        else:
            return {"message": "No contract notes found"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/refresh/")
async def refresh():
    try:
        refresh_data()
        return {"message": "Equity contract notes refreshed successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))