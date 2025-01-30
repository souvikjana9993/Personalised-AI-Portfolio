import base64
import json
import os
from datetime import date,datetime
from typing import Optional, Dict, Any, List

from bs4 import BeautifulSoup
from googleapiclient.errors import HttpError

from config import SCRAPER_CONFIG,ACCOUNTS_CONFIG,DATA_SOURCE_MAPPINGS
from utils.google_auth import get_gmail_service
from utils.helpers import generate_record_id,parse_email_date

paytm_config = SCRAPER_CONFIG["paytm"]

def extract_order_details(html_content: str, received_datetime: datetime) -> Optional[Dict[str, Any]]:
    """
    Extracts the order value, fund name, and other relevant details from the email HTML.

    Args:
        html_content: The HTML content of the email as a string.
        received_datetime: The datetime the email was received.

    Returns:
        A dictionary containing the extracted details, or None if the HTML is invalid.
    """
    soup = BeautifulSoup(html_content, 'html.parser')

    try:
        # Extract order value
        order_value_span = soup.find('span', style="font-weight: 300; font-size:28px; font-weight: 600;")
        order_value = order_value_span.text if order_value_span else None

        # Clean the order value
        if order_value:
            order_value = order_value.replace('\u20b9', '').strip()  # Remove Rupee symbol and whitespace

        # Extract fund name
        fund_name_p = soup.find('p',
                               style="margin:0px;display:inline-block;color: #141B2F; font-size: 12px; font-weight: 600")
        fund_name = fund_name_p.text.replace("SIP", "").strip() if fund_name_p else None

        return {
            "order_value": order_value,
            "fund_name": fund_name,
            "received_datetime": received_datetime.isoformat()  # Store received datetime
        }

    except Exception as e:
        print(f"Error during parsing: {e}")
        return None

def get_emails_by_subject(email_id: str, subject_substring: str, start_date: date = None, end_date: date = None) -> Optional[str]:
    """
    Retrieves emails and updates the JSON file with new records.
    
    Args:
        email_id: Email address to search
        subject_substring: Subject to search for
        start_date: Optional start date filter
        end_date: Optional end date filter
    """
    try:
        service = get_gmail_service("gmail", email_id)
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

        # Setup output directory and file
        output_dir = paytm_config.get("output_dir").format(email=email_id)
        os.makedirs(output_dir, exist_ok=True)
        json_file_path = os.path.join(output_dir, "transactions.json")

        # Load existing data if file exists
        existing_data = {}
        if os.path.exists(json_file_path):
            with open(json_file_path, 'r') as f:
                existing_data = {record['id']: record for record in json.load(f)}

        for message in messages:
            msg = service.users().messages().get(userId='me', id=message['id']).execute()
            headers = msg['payload']['headers']
            received_date = next((h['value'] for h in headers if h['name'] == 'Date'), None)
            if received_date:
                try:
                    received_date = parse_email_date(received_date)
                except ValueError as e:
                    print(f"Warning: Could not parse date '{received_date}': {e}")
                    continue

            if 'data' in msg['payload']['body']:
                data = msg['payload']['body']['data']
            else:
                data = msg['payload']['parts'][0]['body']['data']

            decoded_body = base64.urlsafe_b64decode(data).decode('utf-8')
            extracted_data = extract_order_details(decoded_body, received_date)
            
            if extracted_data:
                # Add subject information
                extracted_data['email_subject'] = subject_substring
                # Generate unique ID with subject
                record_id = generate_record_id(extracted_data['received_datetime'], 
                                            extracted_data['fund_name'],
                                            subject_substring)
                extracted_data['id'] = record_id
                existing_data[record_id] = extracted_data

        # Save updated data
        if existing_data:
            with open(json_file_path, 'w') as f:
                json.dump(list(existing_data.values()), f, indent=2)
            return json_file_path

        return None

    except HttpError as error:
        print(f"An error occurred: {error}")
        return None

def refresh_data(start_date: date = None, end_date: date = None):
    """Refreshes the data only for authorized Paytm accounts."""
    print("Refreshing Paytm Money data...")
    
    authorized_emails = DATA_SOURCE_MAPPINGS["paytm"]["emails"]
    subject_substring = DATA_SOURCE_MAPPINGS["paytm"]["subject_substring"]
    
    for email in authorized_emails:
        try:
            json_file_path = get_emails_by_subject(email, subject_substring,start_date=start_date, end_date=end_date)
            if json_file_path:
                print(f"Paytm data refresh complete for {email}")
            else:
                print(f"No new data found for {email}")
        except Exception as e:
            print(f"Error refreshing data for {email}: {e}")