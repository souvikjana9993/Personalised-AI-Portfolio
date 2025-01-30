import json
import os
from typing import Optional
from datetime import date
from fastapi import APIRouter, Query, HTTPException, Response

from schemas.paytm_schemas import ScrapeRequest,default_start_date,default_end_date
from scrapers.paytm_scraper import get_emails_by_subject, refresh_data
from config import SCRAPER_CONFIG

router = APIRouter()

@router.get("/scrape/",
            summary="Scrape Paytm Money emails for order details",
            description="Retrieves emails from a given Gmail address with the subject 'Order Sent to AMC' and extracts order information.")
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
        subject_substring = SCRAPER_CONFIG["paytm"]["subject_substring"]
        json_file_path = get_emails_by_subject(email_id, subject_substring, start_date, end_date)
        if json_file_path:
            with open(json_file_path, "r") as f:
                json_data = json.load(f)
            return Response(content=json.dumps(json_data, indent=2), media_type="application/json")
        else:
            return Response(content="No relevant emails found.", media_type="text/plain")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")

@router.post("/refresh/")
async def refresh():
    """
    Manually triggers a data refresh for the Paytm Money scraper.

    Returns:
        A message indicating the result of the refresh operation.
    """
    try:
        refresh_data()
        return {"message": "Paytm Money data refreshed successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred during refresh: {e}")