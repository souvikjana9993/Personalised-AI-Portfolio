import base64
import json
import os
from datetime import date
from typing import Optional, Dict, Any, List

import pandas as pd
from bs4 import BeautifulSoup
from googleapiclient.errors import HttpError

from config import SCRAPER_CONFIG,ACCOUNTS_CONFIG,DATA_SOURCE_MAPPINGS
from utils.google_auth import get_gmail_service
from utils.helpers import generate_record_id,parse_email_date

gmail_config = SCRAPER_CONFIG["gmail"]


def extract_table_data(html_content: str) -> Optional[List[Dict[str, Any]]]:
    """
    Extracts table data from HTML content using BeautifulSoup and returns it as a list of dictionaries.

    Args:
        html_content: The HTML content as a string.

    Returns:
        A list of dictionaries representing the table data, or None if no matching table is found.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.find('table', {
        'style': 'cellspacing:0;color:#000000;font-family:Ubuntu, Helvetica, Arial, sans-serif;font-size:13px;line-height:22px;table-layout:auto;width:100%; min-width: 700px;'})

    if table:
        headers = [th.text.strip() for th in table.find_all('th')]
        rows = []
        for tr in table.find_all('tr', {'class': 'fund_list'}):
            row_data = [td.text.strip() for td in tr.find_all('td')]
            rows.append(dict(zip(headers, row_data)))
        return rows
    else:
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
        output_dir = gmail_config.get("output_dir").format(email=email_id)
        os.makedirs(output_dir, exist_ok=True)
        json_file_path = os.path.join(output_dir, "transactions.json")

        # Load existing data if file exists
        existing_data = {}
        if os.path.exists(json_file_path):
            with open(json_file_path, 'r') as f:
                existing_data = {record['id']: record for record in json.load(f)}

        # Process new data
        for message in messages:
            msg = service.users().messages().get(userId='me', id=message['id']).execute()
            # Extract date from headers
            headers = msg['payload']['headers']
            received_date = next((h['value'] for h in headers if h['name'] == 'Date'), None)
            if received_date:
                try:
                    received_date = parse_email_date(received_date)
                except ValueError as e:
                    print(f"Warning: Could not parse date '{received_date}': {e}")
                    continue

            # Extract and process message body
            if 'data' in msg['payload']['body']:
                data = msg['payload']['body']['data']
            else:
                data = msg['payload']['parts'][0]['body']['data']
            decoded_body = base64.urlsafe_b64decode(data).decode('utf-8')
            
            extracted_data = extract_table_data(decoded_body)
            if extracted_data:
                for record in extracted_data:
                    # Add subject information
                    record['email_subject'] = subject_substring
                    # Generate unique ID with subject
                    record_id = generate_record_id(record['Date'], record['Fund'], subject_substring)
                    record['id'] = record_id
                    existing_data[record_id] = record
        # Save updated data
        if existing_data:
            with open(json_file_path, 'w') as f:
                json.dump(list(existing_data.values()), f, indent=2)
            return json_file_path

        return None

    except HttpError as error:
        print(f"An error occurred: {error}")
        return None

    except HttpError as error:
        print(f"An error occurred: {error}")
        return None
    
def refresh_data(start_date: date = None, end_date: date = None):
    """Refreshes the data only for authorized Gmail accounts."""
    print("Refreshing Gmail/Zerodha data...")
    
    authorized_emails = DATA_SOURCE_MAPPINGS["zerodha"]["emails"]
    subject_substring = DATA_SOURCE_MAPPINGS["zerodha"]["subject_substring"]
    
    for email in authorized_emails:
        try:
            json_file_path = get_emails_by_subject(
                email, 
                subject_substring,
                start_date=start_date,
                end_date=end_date
            )
            if json_file_path:
                print(f"Gmail/Zerodha data refresh complete for {email}")
            else:
                print(f"No new data found for {email}")
        except Exception as e:
            print(f"Error refreshing data for {email}: {e}")