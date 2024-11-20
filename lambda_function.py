import boto3
import os
import sys
import uuid
from urllib.parse import unquote_plus
import json
import csv
import pytz
from datetime import datetime, timedelta
import google.auth
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials

s3_client = boto3.client('s3')

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CREDENTIALS_FILE = 'client_secret.json'

def append_data_to_sheet(spreadsheet_id, sheet_name, csv_file):
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    else:
        flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
        creds = flow.run_local_server(port=0)

        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        # Check if the sheet exists, if not, create it
        try:
            result = sheet.values().get(spreadsheetId=spreadsheet_id, range=f"{sheet_name}!A1").execute()
            values = result.get('values', [])
        except HttpError as error:
            if error.resp.status == 400 or error.resp.status == 404:
                # Sheet doesn't exist, create it
                request = {"addSheet": {"properties": {"title": sheet_name}}}
                response = sheet.batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": [request]}).execute()
                sheet_id = response["replies"][0]["addSheet"]["properties"]["sheetId"]
                values = []
            else:
                raise error

        with open(csv_file, 'r') as file:
            csv_reader = csv.reader(file)
            data = list(csv_reader)

        # Check if the sheet is empty
        if not values:
            # Append the header row
            range_name = f"{sheet_name}!A1"
            request = sheet.values().append(spreadsheetId=spreadsheet_id, range=range_name, valueInputOption='USER_ENTERED', body={'values': [data[0]]}).execute()
            print(f"Appended header row to {spreadsheet_id}, sheet {sheet_name}")
        data = data[1:]  # Remove the header row from the data

        # Find the last row with data
        last_row = len(values) + 1

        # Append the data to the sheet starting from the next row
        range_name = f"{sheet_name}!A{last_row}"
        request = sheet.values().append(spreadsheetId=spreadsheet_id, range=range_name, valueInputOption='USER_ENTERED', body={'values': data}).execute()
        print(f"{len(data)} rows appended to {spreadsheet_id}, sheet {sheet_name}")

    except HttpError as error:
        print(f'An error occurred: {error}')

def get_sheet_name(key):
    parts = key.split('/')
    if len(parts) >= 2:
        sheet_name = parts[1]
        sheet_name = sheet_name.replace('.csv', ' ')
        return sheet_name
    else:
        return None

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        tmpkey = key.replace('/', '')
        csv_file_download_path = '/tmp/{}{}'.format(uuid.uuid4(), tmpkey)
        s3_client.download_file(bucket, key, csv_file_download_path)

        spreadsheet_id = '1X7zDOZTxz-HrCuQvF-8pLna4ZGaRjirONdcOZOj0_9Q'
        sheet_name = get_sheet_name(key)
        if sheet_name:
            append_data_to_sheet(spreadsheet_id, sheet_name, csv_file_download_path)
        else:
            print(f"Skipping file: {key}")
