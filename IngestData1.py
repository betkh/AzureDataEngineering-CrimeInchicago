
import os
import pandas as pd

from src.pull_data import fetch_data_from_api
from datetime import datetime
from dotenv import load_dotenv


def load_config():
    """Load API configuration from environment variables."""
    # Load environment variables from .env file
    load_dotenv()
    
    api_key_id = os.getenv('API_KEY_ID')
    api_secret = os.getenv('API_SECRET')
    
    if not api_key_id or not api_secret:
        raise ValueError("API_KEY_ID and API_SECRET environment variables must be set")
    
    return api_key_id, api_secret


def ingest_crimes_data():
    """
    Ingests 2 months of 'Crimes' dataset and saves to CSV file.
    """

    print("DataSet1 ingestion - 'Crimes - 2001 to Present'")

    # Load API keys from environment variables
    api_key_id, api_secret = load_config()

    # Fetch data via API
    print("Fetching data via API ...")

    # Construct the full URL
    BASE_URL = "https://data.cityofchicago.org/resource"
    END_POINT = "ijzp-q8t2.json"
    url = f"{BASE_URL}/{END_POINT}"

    # Simple 2 months filter (current month and previous month)
    from datetime import datetime, timedelta
    now = datetime.now()
    two_months_ago = now - timedelta(days=60)
    
    start_date = two_months_ago.strftime("%Y-%m-%dT00:00:00")
    end_date = now.strftime("%Y-%m-%dT23:59:59")
    
    row_filter = f"(date>='{start_date}' AND date<='{end_date}')"
    
    print(f"Fetching data from {start_date} to {end_date}")

    # Fetch data from the specified API endpoint
    df = fetch_data_from_api(url,                       # API endpoint
                              api_key_id,                # API key ID for authentication
                              api_secret,                # API secret for authentication
                              columns=None,              # Get all columns
                              row_filter=row_filter,     # Filter to apply on rows
                              max_records=10)            # Get only first 10 records

    print("[Success] - Data fetch from API successful and data stored in df")
    print("\nData insights:")
    print(df.head())
    print(df.shape)
    print(f"Number of columns: {len(df.columns)}")
    print(f"Number of rows: {len(df)}")

    if len(df) == 0:
        print("No New Data, come back later!")
        return None
    else:
        # Generate simple file name with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file_label = f"crimes_data_{timestamp}.csv"

        print("\n[Success] - Generated file label:", csv_file_label)

        # Create directory if it doesn't exist
        local_save_path = 'RawData/DataSet1'
        os.makedirs(local_save_path, exist_ok=True)
        
        # Save data directly to CSV
        csv_file_path = os.path.join(local_save_path, csv_file_label)
        df.to_csv(csv_file_path, index=False)
        
        # Verify the file was created
        if os.path.exists(csv_file_path):
            file_size = os.path.getsize(csv_file_path)
            print(f"[Success] - Data saved to CSV: {csv_file_path}")
            print(f"[Success] - File size: {file_size} bytes")
        else:
            print(f"[Error] - Failed to save CSV file: {csv_file_path}")
            return None
        
        return df


# Allow this script to be run independently or imported
if __name__ == "__main__":
    # Simply ingest 2 months of data
    ingest_crimes_data()
