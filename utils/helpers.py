from datetime import datetime
import email.utils

def parse_email_date(date_str: str) -> datetime:
    """Parse email date string in various formats to datetime object"""
    try:
        # First try RFC 2822 format (email standard)
        return datetime.fromtimestamp(email.utils.mktime_tz(email.utils.parsedate_tz(date_str)))
    except Exception:
        # Try various common formats
        formats = [
            '%a, %d %b %Y %H:%M:%S %z',  # RFC 2822
            '%d %b %Y %H:%M:%S %z',      # 7 Apr 2022 13:08:55 +0530
            '%a %b %d %H:%M:%S %Y %z',   # Alternative format
            '%Y-%m-%d %H:%M:%S %z'       # ISO-like format
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
                
        # If all fails, try parsing with email.utils and default to UTC
        try:
            timestamp = email.utils.mktime_tz(email.utils.parsedate_tz(date_str))
            return datetime.fromtimestamp(timestamp)
        except:
            raise ValueError(f"Unable to parse date: {date_str}")

def generate_record_id(timestamp: str, fund_name: str, subject: str = "") -> str:
    """Generate a unique ID for a mutual fund transaction record"""
    if isinstance(timestamp, str):
        try:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        except ValueError:
            dt = parse_email_date(timestamp)
    else:
        dt = timestamp
    
    date_str = dt.strftime('%Y%m%d%H%M%S')
    # Remove spaces and special characters from fund name and subject
    fund_clean = ''.join(e for e in fund_name if e.isalnum())
    subject_clean = ''.join(e for e in subject if e.isalnum())
    return f"{date_str}_{fund_clean}_{subject_clean}"