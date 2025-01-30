import base64
import json
import os
from datetime import date
from typing import Optional, Dict, Any, List

from googleapiclient.errors import HttpError
from config import SCRAPER_CONFIG, ACCOUNTS_CONFIG,DATA_SOURCE_MAPPINGS
from utils.google_auth import get_gmail_service
from utils.helpers import generate_record_id, parse_email_date
import tempfile
from dotenv import load_dotenv
from pikepdf import Pdf

load_dotenv()

equity_config = SCRAPER_CONFIG["equity"]

def save_attachment(data: str, filename: str, output_dir: str) -> str:
    """
    Save and decrypt PDF attachment if encrypted.
    
    Args:
        data: Base64 encoded PDF data
        filename: Name of the file to save
        output_dir: Directory to save the file
        
    Returns:
        Path to the saved decrypted PDF file
    """
    # First save the raw PDF to a temporary file
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(base64.urlsafe_b64decode(data))
        temp_path = temp_file.name

    try:
        # Determine which password to use based on email ID in filename
        password = None
        if os.getenv('EMAIL_BABA') in output_dir:
            password = os.getenv('CN_FILE_PASSWORD_BABA')
        elif os.getenv('EMAIL_MAA') in output_dir:
            password = os.getenv('CN_FILE_PASSWORD_MAA')
        else:  # Default to ME
            password = os.getenv('CN_FILE_PASSWORD_ME')

        # Try to open and decrypt the PDF
        with Pdf.open(temp_path, password=password) as pdf:
            filepath = os.path.join(output_dir, filename)
            pdf.save(filepath)
            return filepath
            
    except Exception as e:
        print(f"Error processing PDF {filename}: {str(e)}")
        # If decryption fails, save the original encrypted file
        filepath = os.path.join(output_dir, filename)
        with open(filepath, 'wb') as f:
            f.write(base64.urlsafe_b64decode(data))
        return filepath
        
    finally:
        # Clean up temp file
        if os.path.exists(temp_path):
            os.unlink(temp_path)

def get_emails_by_subject(email_id: str, subject_substring: str, start_date: date = None, end_date: date = None) -> Optional[str]:
    try:
        service = get_gmail_service("equity", email_id)
        query = [f"subject:{subject_substring}", f"to:{email_id}"]
        
        if start_date:
            query.append(f"after:{int(start_date.strftime('%s'))}")
        if end_date:
            query.append(f"before:{int(end_date.strftime('%s'))}")
            
        results = service.users().messages().list(
            userId='me', 
            q=" ".join(query)
        ).execute()
        
        messages = results.get('messages', [])
        output_dir = equity_config.get("output_dir").format(email=email_id)
        os.makedirs(output_dir, exist_ok=True)

        saved_files = []
        for message in messages:
            msg = service.users().messages().get(userId='me', id=message['id']).execute()
            
            if 'parts' in msg['payload']:
                for part in msg['payload']['parts']:
                    if part['filename'] and part['filename'].endswith('.pdf'):
                        if 'data' in part['body']:
                            data = part['body']['data']
                        else:
                            att = service.users().messages().attachments().get(
                                userId='me', 
                                messageId=message['id'],
                                id=part['body']['attachmentId']
                            ).execute()
                            data = att['data']
                            
                        filepath = save_attachment(data, part['filename'], output_dir)
                        saved_files.append(filepath)

        return output_dir if saved_files else None

    except HttpError as error:
        print(f"An error occurred: {error}")
        return None

def refresh_data(start_date: date = None, end_date: date = None):
    """Refreshes the data only for authorized equity accounts."""
    print("Refreshing Equity Contract Notes data...")
    
    authorized_emails = DATA_SOURCE_MAPPINGS["equity"]["emails"]
    subject_substring = DATA_SOURCE_MAPPINGS["equity"]["subject_substring"]
    
    for email in authorized_emails:
        try:
            output_dir = get_emails_by_subject(email, subject_substring,start_date=start_date, end_date=end_date)
            if output_dir:
                print(f"Equity data refresh complete for {email}")
            else:
                print(f"No new data found for {email}")
        except Exception as e:
            print(f"Error refreshing data for {email}: {e}")