from src.Ingestor import Ingestor
from src.s3_manager import S3Manager
import os


def main():
    """Simple pipeline: Ingest data from API, save to CSV, upload to S3."""
    print("=" * 50)
    print("CHICAGO CRIME DATA PIPELINE")
    print("=" * 50)
    
    try:
        # Step 1: Ingest data from API and save to CSV
        print("\n1. Ingesting data from API...")
        ingestor = Ingestor("Chicago Crimes")
        csv_file_path = ingestor.ingest_and_save_crimes(max_records=10, date_range_days=60)
        
        if not csv_file_path:
            print("❌ Failed to ingest data")
            return False
        
        print(f"✅ Data saved to: {csv_file_path}")
        
        # Step 2: Upload to S3
        print("\n2. Uploading to S3...")
        s3_manager = S3Manager()
        filename = os.path.basename(csv_file_path)
        s3_key = f"ingested-raw/{filename}"
        
        if s3_manager.upload_csv(csv_file_path, s3_key):
            print(f"✅ Uploaded to S3: s3://{s3_manager.bucket_name}/{s3_key}")
        else:
            print("❌ Failed to upload to S3")
            return False
        
        print("\n" + "=" * 50)
        print("PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 50)
        return True
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        return False


if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)
