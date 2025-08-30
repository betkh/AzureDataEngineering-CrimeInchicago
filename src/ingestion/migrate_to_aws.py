#!/usr/bin/env python3
"""
Migration script to help transition from Azure Data Lake Storage to AWS S3.
This script provides utilities to migrate existing data and configurations.
"""

import os
import configparser
from functions.setup import load_config
from functions.upload_s3 import init_s3_client, upload_dataframe_to_s3
import pandas as pd


def migrate_config():
    """Migrate from Azure config to AWS config."""
    print("=== Azure to AWS S3 Migration Helper ===\n")
    
    # Check if aws_config.ini exists
    if os.path.exists('aws_config.ini'):
        print("✓ AWS config file already exists")
        return
    
    # Create aws_config.ini from template
    config_content = """[DEFAULT]
# Chicago Data Portal API Credentials
API_KEY_ID = your_api_key_id_here
API_SECRET = your_api_secret_here

# AWS S3 Configuration
AWS_ACCESS_KEY_ID = your_aws_access_key_id_here
AWS_SECRET_ACCESS_KEY = your_aws_secret_access_key_here
AWS_REGION = us-east-1
S3_BUCKET_NAME = crimeinchicago-data
S3_PREFIX = input-ingested-raw/

# Note: Replace the placeholder values above with your actual credentials
"""
    
    with open('aws_config.ini', 'w') as f:
        f.write(config_content)
    
    print("✓ Created aws_config.ini template")
    print("  Please update the file with your actual AWS credentials")


def test_aws_connection():
    """Test AWS S3 connection."""
    print("\n=== Testing AWS S3 Connection ===")
    
    try:
        # Load config
        config = load_config('aws_config.ini')
        aws_access_key_id = config.get('DEFAULT', 'AWS_ACCESS_KEY_ID')
        aws_secret_access_key = config.get('DEFAULT', 'AWS_SECRET_ACCESS_KEY')
        aws_region = config.get('DEFAULT', 'AWS_REGION')
        s3_bucket_name = config.get('DEFAULT', 'S3_BUCKET_NAME')
        
        # Check if credentials are still placeholders
        if 'your_aws_access_key_id_here' in aws_access_key_id:
            print("⚠  AWS credentials not configured yet")
            print("   Please update aws_config.ini with your actual credentials")
            return False
        
        # Test connection
        s3_client = init_s3_client(aws_access_key_id, aws_secret_access_key, aws_region)
        
        # Test bucket access
        try:
            s3_client.head_bucket(Bucket=s3_bucket_name)
            print(f"✓ Successfully connected to S3 bucket: {s3_bucket_name}")
            return True
        except Exception as e:
            print(f"⚠  Bucket '{s3_bucket_name}' not found or not accessible")
            print(f"   Error: {e}")
            return False
            
    except Exception as e:
        print(f"✗ Failed to test AWS connection: {e}")
        return False


def migrate_sample_data():
    """Migrate sample data to AWS S3."""
    print("\n=== Migrating Sample Data to AWS S3 ===")
    
    # Check if we have sample data
    sample_data_path = 'RawData/DataSet1'
    if not os.path.exists(sample_data_path):
        print("⚠  No sample data found in RawData/DataSet1")
        return
    
    try:
        # Load AWS config
        config = load_config('aws_config.ini')
        aws_access_key_id = config.get('DEFAULT', 'AWS_ACCESS_KEY_ID')
        aws_secret_access_key = config.get('DEFAULT', 'AWS_SECRET_ACCESS_KEY')
        aws_region = config.get('DEFAULT', 'AWS_REGION')
        s3_bucket_name = config.get('DEFAULT', 'S3_BUCKET_NAME')
        s3_prefix = config.get('DEFAULT', 'S3_PREFIX')
        
        # Check if credentials are configured
        if 'your_aws_access_key_id_here' in aws_access_key_id:
            print("⚠  AWS credentials not configured")
            return
        
        # Initialize S3 client
        s3_client = init_s3_client(aws_access_key_id, aws_secret_access_key, aws_region)
        
        # Find CSV files in sample data
        csv_files = [f for f in os.listdir(sample_data_path) if f.endswith('.csv')]
        
        if not csv_files:
            print("⚠  No CSV files found in sample data")
            return
        
        print(f"Found {len(csv_files)} CSV files to migrate")
        
        for csv_file in csv_files:
            file_path = os.path.join(sample_data_path, csv_file)
            s3_key = f"{s3_prefix}Crime2019_to_Present/{csv_file}"
            
            # Read and upload
            df = pd.read_csv(file_path)
            upload_dataframe_to_s3(s3_client, s3_bucket_name, s3_key, df)
            print(f"✓ Migrated: {csv_file}")
        
        print("✓ Sample data migration completed")
        
    except Exception as e:
        print(f"✗ Failed to migrate sample data: {e}")


def main():
    """Main migration function."""
    print("Azure Data Lake Storage to AWS S3 Migration Tool")
    print("=" * 50)
    
    # Step 1: Migrate config
    migrate_config()
    
    # Step 2: Test connection
    if test_aws_connection():
        # Step 3: Migrate sample data
        migrate_sample_data()
    
    print("\n=== Migration Summary ===")
    print("1. ✓ Configuration template created")
    print("2. ⚠  Please update aws_config.ini with your AWS credentials")
    print("3. ⚠  Run this script again after configuring credentials")
    print("4. ✓ Sample data will be migrated automatically")
    
    print("\nNext steps:")
    print("- Update aws_config.ini with your AWS credentials")
    print("- Run: python migrate_to_aws.py")
    print("- Test the new ingestion: python DataSet1_Crimes.py")


if __name__ == "__main__":
    main()
