import os
import pandas as pd
import requests
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from tqdm import tqdm


class Ingestor:
    """Data Ingestor class for handling API data ingestion with DRY principles."""
    
    def __init__(self, data_source_name="Chicago Crimes"):
        """Initialize the Ingestor with API configuration."""
        load_dotenv()
        
        self.data_source_name = data_source_name
        
        # Load API credentials
        self.api_key_id = os.getenv('API_KEY_ID')
        self.api_secret = os.getenv('API_SECRET')
        
        # Validate API credentials
        if not all([self.api_key_id, self.api_secret]):
            raise ValueError("API_KEY_ID and API_SECRET environment variables must be set")
        
        # API configuration
        self.base_url = "https://data.cityofchicago.org/resource"
        self.headers = {
            "X-Api-Key-Id": self.api_key_id,
            "X-Api-Secret": self.api_secret
        }
        
        print(f"Ingestor initialized for: {self.data_source_name}")
        print(f"API credentials loaded successfully")
    
    def fetch_data_from_api(self, endpoint, columns=None, row_filter=None, max_records=None, timeout=10, delay=4):
        """Generic method to fetch data from any API endpoint."""
        url = f"{self.base_url}/{endpoint}"
        
        # Pagination parameters
        pagelimit = 1000
        offset = 0
        all_data = []
        
        # Handle unlimited records
        if max_records is None:
            max_records = float('inf')
        
        print(f"Fetching data from: {url}")
        print(f"Row filter: {row_filter}")
        print(f"Columns: {columns}")
        print(f"Max records: {max_records if max_records != float('inf') else 'Unlimited'}")
        
        # Initialize progress bar
        with tqdm(total=max_records if max_records != float('inf') else None, desc="Fetching records") as pbar:
            while len(all_data) < max_records:
                params = {
                    "$limit": pagelimit,
                    "$offset": offset,
                    "$select": ','.join(columns) if columns else '*',
                    "$where": row_filter
                }
                
                try:
                    response = requests.get(url, headers=self.headers, params=params, timeout=timeout)
                    
                    if response.status_code == 200:
                        data = response.json()
                        if not data:
                            print("No data returned. Exiting loop.")
                            break
                        
                        # Handle the case where max_records is float('inf')
                        if max_records == float('inf'):
                            records_to_add = data
                        else:
                            records_to_add = data[:max_records - len(all_data)]
                        
                        all_data.extend(records_to_add)
                        pbar.update(len(records_to_add))
                        offset += pagelimit
                    else:
                        print(f"Error: {response.status_code} - {response.text}")
                        raise Exception(f"Failed to retrieve data: {response.status_code}")
                    
                    time.sleep(delay)
                    
                except requests.exceptions.Timeout:
                    print(f"Request timed out. Retrying...")
                    time.sleep(delay * 2)
                except requests.exceptions.RequestException as e:
                    print(f"An error occurred: {e}")
                    break
        
        df = pd.DataFrame(all_data)
        print(f"Total records fetched: {len(df)}")
        return df
    
    def ingest_crimes_data(self, max_records=10, date_range_days=60):
        """Ingest crimes data from Chicago API."""
        print(f"\n{'='*50}")
        print(f"Ingesting {self.data_source_name} Data")
        print(f"{'='*50}")
        
        # API endpoint for crimes data
        endpoint = "ijzp-q8t2.json"
        
        # Calculate date range
        now = datetime.now()
        start_date = (now - timedelta(days=date_range_days)).strftime("%Y-%m-%dT00:00:00")
        end_date = now.strftime("%Y-%m-%dT23:59:59")
        
        # Row filter for date range
        row_filter = f"(date>='{start_date}' AND date<='{end_date}')"
        
        print(f"Date range: {start_date} to {end_date}")
        print(f"Max records: {max_records}")
        
        # Fetch data
        df = self.fetch_data_from_api(
            endpoint=endpoint,
            columns=None,  # Get all columns
            row_filter=row_filter,
            max_records=max_records
        )
        
        if len(df) == 0:
            print("No data found for the specified criteria")
            return None
        
        # Display data insights
        print(f"\nData insights:")
        print(f"Shape: {df.shape}")
        print(f"Columns: {len(df.columns)}")
        print(f"Rows: {len(df)}")
        print(f"\nFirst few records:")
        print(df.head())
        
        return df
    
    def save_to_csv(self, df, directory='RawData/DataSet1', filename_prefix='crimes_data'):
        """Save DataFrame to CSV file with timestamp."""
        if df is None or len(df) == 0:
            print("No data to save")
            return None
        
        # Create directory if it doesn't exist
        os.makedirs(directory, exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{filename_prefix}_{timestamp}.csv"
        file_path = os.path.join(directory, filename)
        
        # Save to CSV
        df.to_csv(file_path, index=False)
        
        # Verify file was created
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            print(f"\n[Success] - Data saved to CSV: {file_path}")
            print(f"[Success] - File size: {file_size} bytes")
            return file_path
        else:
            print(f"[Error] - Failed to save CSV file: {file_path}")
            return None
    
    def ingest_and_save_crimes(self, max_records=10, date_range_days=60):
        """Complete workflow: ingest crimes data and save to CSV."""
        try:
            # Ingest data
            df = self.ingest_crimes_data(max_records=max_records, date_range_days=date_range_days)
            
            if df is not None:
                # Save to CSV
                file_path = self.save_to_csv(df)
                return file_path
            else:
                return None
                
        except Exception as e:
            print(f"Error in ingest and save workflow: {e}")
            return None


def main():
    """Main function to demonstrate Ingestor usage."""
    try:
        # Initialize Ingestor
        ingestor = Ingestor("Chicago Crimes")
        
        # Ingest and save crimes data
        csv_file = ingestor.ingest_and_save_crimes(max_records=10, date_range_days=60)
        
        if csv_file:
            print(f"\n{'='*50}")
            print("INGESTION COMPLETED SUCCESSFULLY!")
            print(f"CSV file: {csv_file}")
            print(f"{'='*50}")
        else:
            print(f"\n{'='*50}")
            print("INGESTION FAILED!")
            print(f"{'='*50}")
            
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
