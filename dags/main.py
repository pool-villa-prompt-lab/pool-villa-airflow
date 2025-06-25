import asyncio
import argparse
from datetime import datetime, timedelta
import pandas as pd
from scraper import VillaScraper
import logging
import os
import pickle
import json
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)

# Storage configuration
STORAGE_FILE = 'app_data.json'
INPUT_SHEET_ID = '1v7MlisgWmYbjbMdbE77uupC7EmF0p_qC8FdgbNR22HU'
INPUT_SHEET_RANGE = 'Sheet1!A1:Z1000'  # Changed from 'house_data' to 'Sheet1'

def load_storage():
    """Load data from storage file"""
    try:
        if os.path.exists(STORAGE_FILE):
            with open(STORAGE_FILE, 'r') as f:
                return json.load(f)
        return {
            'scraper_status': 'stopped',
            'current_cycle': 0,
            'last_update': None,
            'next_run': None,
            'spreadsheet_id': None,
            'config': {
                'type': 'both',
                'start_date': datetime.now().strftime('%Y-%m-%d'),
                'output': 'villa_data.csv',
                'last_scraped_date': None
            }
        }
    except Exception as e:
        logging.error(f"Error loading storage: {str(e)}")
        return None

def save_storage(data):
    """Save data to storage file"""
    try:
        with open(STORAGE_FILE, 'w') as f:
            json.dump(data, f, indent=4)
        return True
    except Exception as e:
        logging.error(f"Error saving storage: {str(e)}")
        return False

def update_storage(key, value):
    """Update specific key in storage"""
    try:
        data = load_storage()
        if data is None:
            return False
        
        # Handle nested keys (e.g., 'config.type')
        if '.' in key:
            main_key, sub_key = key.split('.')
            if main_key not in data:
                data[main_key] = {}
            data[main_key][sub_key] = value
        else:
            data[key] = value
            
        return save_storage(data)
    except Exception as e:
        logging.error(f"Error updating storage: {str(e)}")
        return False

# Google API setup
SCOPES = [
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/spreadsheets.readonly'
]

SERVICE_ACCOUNT_PATH = "/opt/airflow/dags/service_account.json"

def get_google_services():
    """Get Google API services using a Service Account"""
    logging.info("Loading credentials from Service Account JSON")
    if not os.path.exists(SERVICE_ACCOUNT_PATH):
        logging.error(f"Service account file not found at {SERVICE_ACCOUNT_PATH}")
        return None, None

    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_PATH, scopes=SCOPES
        )

        logging.info("Building Google Drive service")
        drive_service = build("drive", "v3", credentials=creds)

        logging.info("Building Google Sheets service")
        sheets_service = build("sheets", "v4", credentials=creds)

        logging.info("Successfully built both services")
        return drive_service, sheets_service

    except Exception as e:
        logging.error(f"Failed to initialize Google API services: {e}")
        return None, None

def create_or_get_spreadsheet(drive_service, sheets_service, file_name):
    try:
        # Set parent folder ID
        folder_id = "1v7s9pL4S9CCdS3ijvtDVttPfHbnZl0NE"
        
        # Search for existing spreadsheet with the same name in the folder
        query = f"name='{file_name}' and '{folder_id}' in parents and trashed=false and mimeType='application/vnd.google-apps.spreadsheet'"
        results = drive_service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        existing_files = results.get('files', [])
        
        if existing_files:
            spreadsheet_id = existing_files[0]['id']
            logging.info(f"Found existing spreadsheet with ID: {spreadsheet_id}")
            return spreadsheet_id
        
        # Create new spreadsheet
        spreadsheet = {
            'properties': {
                'title': file_name
            }
        }
        spreadsheet = sheets_service.spreadsheets().create(body=spreadsheet).execute()
        spreadsheet_id = spreadsheet.get('spreadsheetId')
        
        # Move to specified folder
        file = drive_service.files().update(
            fileId=spreadsheet_id,
            addParents=folder_id,
            fields='id, parents'
        ).execute()
        
        logging.info(f"Created new spreadsheet with ID: {spreadsheet_id}")
        return spreadsheet_id
        
    except Exception as e:
        logging.error(f"Error creating/getting spreadsheet: {str(e)}")
        return None

import os
import logging
import pandas as pd
from googleapiclient.errors import HttpError

def save_to_google_sheets(
    file_path: str,
    file_name: str,
    spreadsheet_id: str | None = None,
    sheet_range: str = "A1"
) -> str | None:
    """
    Upload CSV to Google Sheet.
    If `spreadsheet_id` is given → overwrite that sheet.
    Otherwise create/get sheet by `file_name`.

    Returns the spreadsheet_id on success, else None.
    """
    try:
        # ---------- 1. validate local file ----------
        if not os.path.exists(file_path):
            logging.error(f"[SAVE-GS] File not found: {file_path}")
            return None

        drive_service, sheets_service = get_google_services()

        # ---------- 2. resolve spreadsheet ----------
        if spreadsheet_id is None:
            spreadsheet_id = create_or_get_spreadsheet(
                drive_service, sheets_service, file_name
            )
            if not spreadsheet_id:
                return None
        else:
            logging.info(f"[SAVE-GS] Using existing spreadsheet ID: {spreadsheet_id}")

        # ---------- 3. read & clean dataframe ----------
        df = (
            pd.read_csv(file_path)
              .fillna("")
              .pipe(lambda d: d.assign(
                     villa_id=pd.to_numeric(d.get("villa_id", pd.Series()), errors="coerce")
                               .fillna(0)
                               .astype(int)
                   ) if "villa_id" in d.columns else d)
        )
        # ensure all non-numeric columns are str
        for col in df.columns:
            if col != "villa_id":
                df[col] = df[col].astype(str).str.replace(r"[\n\r]", " ", regex=True)

        values = [df.columns.tolist()] + df.values.tolist()

        # ---------- 4. clear existing sheet then write ----------
        try:
            sheets_service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=sheet_range
            ).execute()
            logging.info(f"[SAVE-GS] Cleared previous data in {spreadsheet_id}")
        except HttpError as err:
            logging.warning(f"[SAVE-GS] Could not clear sheet: {err}")

        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=sheet_range,
            valueInputOption="RAW",
            body={"values": values},
        ).execute()

        logging.info(f"[SAVE-GS] Uploaded {len(values)-1} rows to sheet {spreadsheet_id}")
        return spreadsheet_id

    except Exception as e:
        logging.error(f"[SAVE-GS] Error uploading to Google Sheets: {e}")
        return None


def read_from_google_sheet():
    """Read data from Google Sheet"""
    try:
        logging.info("Initializing Google services")
        drive_service, sheets_service = get_google_services()
        if not sheets_service:
            logging.error("Failed to initialize Google Sheets service")
            return None
        
        # First, verify the sheet exists and get its properties
        try:
            logging.info(f"Attempting to access Google Sheet with ID: {INPUT_SHEET_ID}")
            sheet_metadata = sheets_service.spreadsheets().get(
                spreadsheetId=INPUT_SHEET_ID
            ).execute()
            
            # Log all available sheets
            sheets = sheet_metadata.get('sheets', [])
            logging.info(f"Found {len(sheets)} sheets in the spreadsheet")
            for sheet in sheets:
                sheet_name = sheet['properties']['title']
                logging.info(f"Sheet name: {sheet_name}")
        
            # Get the first sheet name
            if not sheets:
                logging.error("No sheets found in the spreadsheet")
                return None
                
            sheet_name = sheets[0]['properties']['title']
            logging.info(f"Using sheet: {sheet_name}")
            
            # Update range with correct sheet name
            range_name = f"{sheet_name}!A1:Z1000"
            logging.info(f"Reading range: {range_name}")
            
        except Exception as e:
            logging.error(f"Error accessing Google Sheet: {str(e)}")
            logging.error("Please check if:")
            logging.error("1. The Sheet ID is correct")
            logging.error("2. You have access to the Sheet")
            logging.error("3. Your credentials have the correct scopes")
            logging.error("4. The Google Sheets API is enabled in your project")
            return None
            
        # Get data from sheet
        try:
            logging.info("Attempting to read data from sheet")
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=INPUT_SHEET_ID,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            if not values:
                logging.error("No data found in Google Sheet")
                return None
                
            # Log the first row (headers)
            if values:
                logging.info(f"Found headers: {values[0]}")
            
            # Convert to DataFrame
            df = pd.DataFrame(values[1:], columns=values[0])
            
            # Log the columns found
            logging.info(f"Found columns: {', '.join(df.columns)}")
            
            # Rename columns to match expected format
            column_mapping = {
                'P ID': 'p_id',
                'D ID': 'd_id',
                'ชื่อบ้าน': 'villa_name'
            }
            
            # Check which columns exist and can be mapped
            existing_columns = {}
            for old_col, new_col in column_mapping.items():
                if old_col in df.columns:
                    existing_columns[old_col] = new_col
                    logging.info(f"Found required column: {old_col}")
                else:
                    logging.warning(f"Column '{old_col}' not found in sheet")
            
            if not existing_columns:
                logging.error("No required columns found in sheet")
                return None
                
            df = df.rename(columns=existing_columns)
            
            # Clean data
            df = df.fillna('')
            
            # Log the number of rows found
            logging.info(f"Found {len(df)} rows of data")
            
            return df
            
        except Exception as e:
            logging.error(f"Error reading data from sheet: {str(e)}")
            return None
        
    except Exception as e:
        logging.error(f"Error in read_from_google_sheet: {str(e)}")
        logging.error("Please check your Google Sheet configuration and permissions")
        return None

def load_villa_data(file_path=None):
    """Load villa data from Google Sheet"""
    try:
        # Read from Google Sheet
        df = read_from_google_sheet()
        if df is None:
            return None
        
        # Check for required columns
        required_columns = ['p_id', 'd_id']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logging.error(f"Google Sheet missing required columns: {', '.join(missing_columns)}")
            return None
        
        return df
    except Exception as e:
        logging.error(f"Error loading villa data: {str(e)}")
        return None

async def run_scraping_cycle(config, villa_df, scraper=None):
    try:
        # Update storage with current status
        update_storage('scraper_status', 'running')
        update_storage('last_update', datetime.now().isoformat())
        
        if scraper is None:
            scraper = VillaScraper()
            should_close = True
        else:
            should_close = False
        
        # Set continuous mode flag on scraper
        scraper.is_continuous = config.get('mode') == 'continuous'
        
        # For continuous mode, check if we need to update dates based on real calendar
        if config.get('mode') == 'continuous':
            current_real_date = datetime.now().strftime('%Y-%m-%d')
            last_run_date = config.get('last_run_date')
            
            # If this is first run or it's a new day
            if not last_run_date or last_run_date < current_real_date:
                # Set start date to yesterday
                start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                # Set end date to 30 days from start date
                end_date = (datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=30)).strftime('%Y-%m-%d')
                
                # Update config with new dates
                config['start_date'] = start_date
                config['end_date'] = end_date
                config['last_run_date'] = current_real_date
                update_storage('config', config)
                logging.info(f"Updated date range for new day: {start_date} to {end_date}")
        
        # Start scraping for the entire date range
        await scraper.start_scraping(
            villa_df,
            config['type'],
            config['start_date'],
            config['end_date'],
            config['output']
        )
        
        # Update last scraped date
        config['last_scraped_date'] = config['end_date']
        update_storage('config', config)
        
        # Construct full output path
        output_file = os.path.join('data', config['output'])
        
        # Upload to Google Sheets if file exists
        if os.path.exists(output_file):
            spreadsheet_id = save_to_google_sheets(output_file, config['output'])
            if spreadsheet_id:
                logging.info(f"Data uploaded to Google Sheets with ID: {spreadsheet_id}")
                update_storage('spreadsheet_id', spreadsheet_id)
            else:
                logging.error("Failed to upload data to Google Sheets")
        else:
            logging.error(f"Output file not found: {output_file}")
            
    except Exception as e:
        logging.error(f"Error in scraping cycle: {str(e)}")
        update_storage('scraper_status', 'error')
    finally:
        # Only close scraper if we created it and we're not in continuous mode
        if should_close and config.get('mode') != 'continuous':
            await scraper.close()
        
        # For continuous mode, ensure we stay in running state
        if config.get('mode') == 'continuous':
            update_storage('scraper_status', 'running')
            # Calculate and update next run time
            next_run = datetime.now() + timedelta(hours=config.get('wait_hours', 0.1))
            update_storage('next_run', next_run.isoformat())
            logging.info(f"Continuous mode: Next run scheduled for {next_run}")
        else:
            # Only update status to stopped if not in continuous mode
            update_storage('scraper_status', 'stopped')

async def main():
  pass

if __name__ == "__main__":
    asyncio.run(main()) 