import os
import base64
from typing import Optional, List, Dict, Any
import tempfile
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def authenticate_gmail() -> Credentials:
    """Authenticates with Gmail using OAuth 2.0 and returns credentials."""
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=8070)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds

def extract_table_data(html_content: str) -> Optional[pd.DataFrame]:
    """
    Extracts table data from HTML content using BeautifulSoup and returns it as a Pandas DataFrame.

    Args:
        html_content: The HTML content as a string.

    Returns:
        A Pandas DataFrame containing the table data, or None if no matching table is found.
    """
    soup = BeautifulSoup(html_content, 'html.parser')

    table = soup.find('table', {
        'style': 'cellspacing:0;color:#000000;font-family:Ubuntu, Helvetica, Arial, sans-serif;font-size:13px;line-height:22px;table-layout:auto;width:100%; min-width: 700px;'})

    if table:
        # Extract table headers
        headers = [th.text.strip() for th in table.find_all('th')]

        # Extract table rows
        rows = []
        for tr in table.find_all('tr', {'class': 'fund_list'}):
            row_data = [td.text.strip() for td in tr.find_all('td')]
            rows.append(row_data)

        # Create a Pandas DataFrame
        df = pd.DataFrame(rows, columns=headers)
        return df
    else:
        return None

def get_emails_by_subject(email_id: str, subject_substring: str, max_results: Optional[int] = 10) -> str:
    """
    Retrieves emails from Gmail, extracts table data, and saves it to an Excel file.

    Args:
        email_id: The email address to search within.
        subject_substring: The substring to search for in the email subject.
        max_results: The maximum number of emails to retrieve.

    Returns:
        The path to the generated Excel file.
    """
    creds = authenticate_gmail()

    try:
        service = build('gmail', 'v1', credentials=creds)

        # Search for emails
        query = f"subject:{subject_substring} to:{email_id}"
        results = service.users().messages().list(userId='me', q=query, maxResults=max_results).execute()
        messages = results.get('messages', [])

        # Create an empty DataFrame to store combined table data
        combined_df = pd.DataFrame()

        for message in messages:
            msg = service.users().messages().get(userId='me', id=message['id']).execute()

            # Decode body
            if 'data' in msg['payload']['body']:
                data = msg['payload']['body']['data']
            else:
                data = msg['payload']['parts'][0]['body']['data']
            decoded_body = base64.urlsafe_b64decode(data).decode('utf-8')

            # Extract table data from email body
            table_df = extract_table_data(decoded_body)

            if table_df is not None:
                # Concatenate table data to the combined DataFrame
                combined_df = pd.concat([combined_df, table_df], ignore_index=True)

        # Save combined DataFrame to an Excel file in a temporary directory
        if not combined_df.empty:
            # Create a directory to store the Excel files if it doesn't exist
            output_dir = "excel_files"
            os.makedirs(output_dir, exist_ok=True)

            # Generate a unique filename using a timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            excel_file_name = f"{subject_substring}_tables_{timestamp}.xlsx"
            excel_file_path = os.path.join(output_dir, excel_file_name)

            combined_df.to_excel(excel_file_path, index=False)
            return excel_file_path
        else:
            return "No tables found in emails."

    except HttpError as error:
        print(f"An error occurred: {error}")
        return f"An error occurred: {error}"