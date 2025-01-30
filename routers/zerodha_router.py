import json
import os
from typing import Optional
from datetime import date


from fastapi import APIRouter, Query, HTTPException, Response

from schemas.zerodha_schemas import ScrapeRequest,default_start_date,default_end_date
from scrapers.gmail_scraper import get_emails_by_subject, refresh_data
from config import SCRAPER_CONFIG

router = APIRouter()

@router.get("/scrape/",
            summary="Scrape Zerodha investment statements",
            description="Retrieves emails from a given Gmail address that contain investment statements from Zerodha.")
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
    """
    Scrapes Gmail for investment statements.

    Args:
        email_id: The Gmail address to scrape.
        start_date: Start date filter, defaults to 1 year ago
        end_date: End date filter, defaults to today

    Returns:
        The extracted data as JSON, or an error message.
    """
    try:
        subject_substring = SCRAPER_CONFIG["gmail"]["subject_substring"]
        json_file_path = get_emails_by_subject(email_id, subject_substring, start_date=start_date, end_date=end_date)
        if json_file_path:
            # Read the JSON data from the file
            with open(json_file_path, "r") as f:
                json_data = json.load(f)

            # Return the JSON data directly
            return Response(content=json.dumps(json_data, indent=2), media_type="application/json")
        else:
            return Response(content="No tables found in emails.", media_type="text/plain")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")

@router.post("/refresh/")
async def refresh():
    """
    Manually triggers a data refresh for the Zerodha scraper.

    Returns:
        A message indicating the result of the refresh operation.
    """
    try:
        refresh_data()
        return {"message": "Zerodha data refreshed successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred during refresh: {e}")