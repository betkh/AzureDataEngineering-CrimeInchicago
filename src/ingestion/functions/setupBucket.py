import boto3
import configparser
import os
from botocore.exceptions import ClientError


def load_aws_config(config_file='../aws_config.ini'):
    """Load AWS configuration from config file."""
    config = configparser.ConfigParser()
    config.read(config_file)
    
    return {
        'access_key': config.get('DEFAULT', 'AWS_ACCESS_KEY_ID'),
        'secret_key': config.get('DEFAULT', 'AWS_SECRET_ACCESS_KEY'),
        'region': config.get('DEFAULT', 'AWS_REGION'),
        'bucket_name': config.get('DEFAULT', 'S3_BUCKET_NAME'),
        'prefix': config.get('DEFAULT', 'S3_PREFIX')
    }


def create_s3_bucket(bucket_name, region):
    """Create S3 bucket if it doesn't exist."""
    s3_client = boto3.client('s3')
    
    try:
        # Check if bucket exists
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"✓ Bucket '{bucket_name}' already exists")
        return True
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            # Bucket doesn't exist, create it
            try:
                if region == 'us-east-1':
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    s3_client.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': region}
                    )
                print(f"✓ Bucket '{bucket_name}' created successfully")
                return True
            except ClientError as create_error:
                print(f"✗ Error creating bucket: {create_error}")
                return False
        else:
            print(f"✗ Error checking bucket: {e}")
            return False


def setup_bucket_folders(s3_client, bucket_name, prefix):
    """Create folder structure in S3 bucket."""
    folders = [
        f"{prefix}raw/crimes/",
        f"{prefix}processed/crimes/",
        f"{prefix}curated/crimes/",
        f"{prefix}logs/"
    ]
    
    for folder in folders:
        try:
            s3_client.put_object(Bucket=bucket_name, Key=folder)
            print(f"✓ Created folder: {folder}")
        except Exception as e:
            print(f"✗ Error creating folder {folder}: {e}")


def setup_s3_bucket():
    """Main function to setup S3 bucket and folder structure."""
    print("=" * 50)
    print("S3 BUCKET SETUP")
    print("=" * 50)
    
    # Load configuration
    try:
        config = load_aws_config()
        print(f"✓ Configuration loaded")
        print(f"  Region: {config['region']}")
        print(f"  Bucket: {config['bucket_name']}")
        print(f"  Prefix: {config['prefix']}")
    except Exception as e:
        print(f"✗ Error loading configuration: {e}")
        return False
    
    # Initialize S3 client
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=config['access_key'],
            aws_secret_access_key=config['secret_key'],
            region_name=config['region']
        )
        print("✓ S3 client initialized")
    except Exception as e:
        print(f"✗ Error initializing S3 client: {e}")
        return False
    
    # Create bucket
    if not create_s3_bucket(config['bucket_name'], config['region']):
        return False
    
    # Setup folder structure
    setup_bucket_folders(s3_client, config['bucket_name'], config['prefix'])
    
    print("=" * 50)
    print("✓ S3 BUCKET SETUP COMPLETED!")
    print("=" * 50)
    return True


if __name__ == "__main__":
    setup_s3_bucket()
