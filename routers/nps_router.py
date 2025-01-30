import json
import os
from typing import Optional
from datetime import date
from fastapi import APIRouter, Query, HTTPException, Response

from schemas.nps_schemas import ScrapeRequest, default_start_date, default_end_date
from scrapers.nps_scraper import get_emails_by_subject,refresh_data
from config import SCRAPER_CONFIG

router = APIRouter()

@router.get("/scrape/",
            summary="Scrape NPS monthly statement emails",
            description="Retrieves emails with NPS monthly statements and extracts the attachments.")
async def scrape_emails(
    email_id: str = Query(..., description="The Gmail address to scrape"),
    start_date: date = Query(
        default=default_start_date(),
        description="Start date for email filter (YYYY-MM-DD). Defaults to 1 year ago."
    ),
    end_date: date = Query(
        default=default_end_date(),
        description="End date for email filter (YYYY-MM-DD). Defaults to today."
    )
):
    if email_id != "souvikjana1993@gmail.com":
        raise HTTPException(status_code=400, detail="This endpoint only supports souvikjana1993@gmail.com")

    try:
        subject_substring = SCRAPER_CONFIG["nps"]["subject_substring"]
        output_dir = get_emails_by_subject(email_id, subject_substring, start_date, end_date)
        
        if output_dir and os.path.exists(output_dir):
            files = [f for f in os.listdir(output_dir) if f.endswith('.pdf')]
            return {
                "message": f"Successfully saved {len(files)} PDF files",
                "directory": output_dir,
                "files": files
            }
        else:
            return {"message": "No NPS statements found"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/refresh/")
async def refresh():
    """
    Manually triggers a data refresh for the NPS scraper.
    
    Returns:
        A message indicating the result of the refresh operation.
    """
    try:
        refresh_data()
        return {"message": "NPS statements refreshed successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))