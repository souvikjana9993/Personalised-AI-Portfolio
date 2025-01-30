from datetime import date, datetime, timedelta
from pydantic import BaseModel, EmailStr, Field

def default_start_date() -> date:
    """Default to 1 year ago"""
    return (datetime.now() - timedelta(days=365)).date()

def default_end_date() -> date:
    """Default to today"""
    return datetime.now().date()

class ScrapeRequest(BaseModel):
    email_id: EmailStr
    start_date: date = Field(default_factory=default_start_date)
    end_date: date = Field(default_factory=default_end_date)