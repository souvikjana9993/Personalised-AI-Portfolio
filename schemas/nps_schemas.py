from datetime import date, datetime, timedelta
from pydantic import BaseModel

def default_start_date():
    return (datetime.now() - timedelta(days=365)).date()

def default_end_date():
    return datetime.now().date()

class ScrapeRequest(BaseModel):
    email_id: str
    start_date: date = default_start_date()
    end_date: date = default_end_date()