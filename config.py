import os
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

# --- Scheduler Configuration ---
REFRESH_INTERVAL_MINUTES = 5

scheduler = BackgroundScheduler()


SCRAPER_DATE_RANGE = {
    "start_date": (datetime.now() - timedelta(days=90)).date(),  # Default to last 30 days
    "end_date": datetime.now().date()
}

# --- Data Source to Email Mappings ---
DATA_SOURCE_MAPPINGS = {
    "zerodha": {
        "emails": [os.getenv('EMAIL_ME'), os.getenv('EMAIL_MAA')],  # 
        "service_name": "gmail",
        "scopes": ['https://www.googleapis.com/auth/gmail.readonly'],
        "output_dir": "data/zerodha/{email}",
        "subject_substring": "Coin by Zerodha - Allotment Report"
    },
    "paytm": {
        "emails": [os.getenv('EMAIL_ME')],  # 
        "service_name": "paytm",
        "scopes": ['https://www.googleapis.com/auth/gmail.readonly'],
        "output_dir": "data/paytmmoney/{email}",
        "subject_substring": "Order Sent to AMC"
    },
    "equity": {
        "emails": [os.getenv('EMAIL_ME'),os.getenv('EMAIL_BABA'),os.getenv('EMAIL_MAA')],  #
        "service_name": "contract_notes",
        "scopes": ['https://www.googleapis.com/auth/gmail.readonly'],
        "subject_substring": "Combined Equity Contract Note for",
        "output_dir": "data/equity/{email}/contract_notes"
    },
    "nps": {
        "emails": [os.getenv('EMAIL_ME')],
        "service_name": "nps",
        "scopes": ['https://www.googleapis.com/auth/gmail.readonly'],
        "subject_substring": "Monthly Transaction Statement of your NPS account for the period",
        "output_dir": "data/nps/{email}/transactions"
    }
}

# --- App Configuration ---
APP_CONFIG = {
    "title": "Modular Scraper API",
    "description": "API to scrape data from various sources based on user-defined configurations.",
    "version": "0.1.0",
}

# --- Account Configurations ---
ACCOUNTS_CONFIG = {
    "accounts": {
        os.getenv('EMAIL_ME'): {
            "credentials_file": os.getenv('CREDENTIALS_ME'),
            "services": {
                "gmail": {"token_file": os.getenv('GMAIL_TOKEN_ME')},
                "contract_notes": {"token_file": os.getenv('CN_TOKEN_ME')},
                "paytm": {"token_file": os.getenv('PAYTM_TOKEN_ME')},
                "nps": {"token_file": os.getenv('NPS_TOKEN_ME')}
            }
        },
        os.getenv('EMAIL_MAA'): {
            "credentials_file": os.getenv('CREDENTIALS_MAA'),
            "services": {
                "gmail": {"token_file": os.getenv('GMAIL_TOKEN_MAA')},
                "contract_notes": {"token_file": os.getenv('CN_TOKEN_MAA')},
                "paytm": {"token_file": os.getenv('PAYTM_TOKEN_MAA')}
            }
        },
        os.getenv('EMAIL_BABA'): {
            "credentials_file": os.getenv('CREDENTIALS_BABA'),
            "services": {
                "gmail": {"token_file": os.getenv('GMAIL_TOKEN_BABA')},
                "contract_notes": {"token_file": os.getenv('CN_TOKEN_BABA')},
                "paytm": {"token_file": os.getenv('PAYTM_TOKEN_BABA')}
            }
        }
    }
}

# For backward compatibility
SCRAPER_CONFIG = {
    "gmail": DATA_SOURCE_MAPPINGS["zerodha"],
    "paytm": DATA_SOURCE_MAPPINGS["paytm"],
    "equity": DATA_SOURCE_MAPPINGS["equity"],
    "nps": DATA_SOURCE_MAPPINGS["nps"]
}