import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from config import SCRAPER_CONFIG,ACCOUNTS_CONFIG

def get_account_credentials(email: str, service_name: str) -> tuple[str, str]:
    """Get credentials and token file paths for a specific email account and service."""
    account_config = ACCOUNTS_CONFIG["accounts"].get(email)
    if not account_config:
        raise ValueError(f"No configuration found for email: {email}")
    
    if service_name not in account_config["services"]:
        raise ValueError(f"No service configuration found for {service_name}")
    
    return (
        account_config["credentials_file"],
        account_config["services"][service_name]["token_file"]
    )


def authenticate_gmail(email: str, service_name: str, scopes: list[str]):
    """Authenticates with Gmail using OAuth 2.0 for a specific email account and service."""
    credentials_file, token_file = get_account_credentials(email, service_name)
    creds = None
    
    if os.path.exists(token_file):
        try:
            creds = Credentials.from_authorized_user_file(token_file, scopes)
        except ValueError:
            os.remove(token_file)
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_file,
                scopes=scopes,
                redirect_uri='http://localhost:8070'
            )
            creds = flow.run_local_server(
                port=8070,
                access_type='offline',
                prompt='consent'
            )
            
        # Save the credentials
        with open(token_file, 'w') as token:
            token.write(creds.to_json())
    
    return creds

def get_gmail_service(scraper_name: str, email: str):
    """Returns a Gmail service object for the specified scraper and email account."""
    config = SCRAPER_CONFIG.get(scraper_name)
    if not config:
        raise ValueError(f"Configuration not found for scraper: {scraper_name}")

    creds = authenticate_gmail(email, config["service_name"], config["scopes"])
    return build('gmail', 'v1', credentials=creds)