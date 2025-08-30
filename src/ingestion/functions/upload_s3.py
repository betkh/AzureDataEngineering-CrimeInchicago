import boto3
import pandas as pd
import io
from botocore.exceptions import ClientError


"""
UPLOAD data to AWS S3
"""


def init_s3_client(aws_access_key_id, aws_secret_access_key, aws_region):
    """Initialize the AWS S3 client."""
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )
        return s3_client
    except Exception as e:
        print(f"Error initializing S3 client: {e}")
        raise


def create_s3_bucket_if_not_exists(s3_client, bucket_name, region):
    """Create S3 bucket if it doesn't exist."""
    try:
        # Check if bucket exists
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"[Success] - S3 bucket '{bucket_name}' already exists.")
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
                        CreateBucketConfiguration={
                            'LocationConstraint': region}
                    )
                print(
                    f"[Success] - S3 bucket '{bucket_name}' created successfully.")
            except ClientError as create_error:
                print(f"Error creating bucket: {create_error}")
                raise
        else:
            print(f"Error checking bucket: {e}")
            raise


def upload_dataframe_to_s3(s3_client, bucket_name, key, df):
    """Upload a DataFrame as a CSV file to AWS S3."""
    try:
        # Convert DataFrame to CSV format in memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=csv_buffer.getvalue()
        )
        print(f"[Success] - '{key}' uploaded to S3 successfully.")
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        raise


def upload_geojson_to_s3(s3_client, bucket_name, key, local_file_path):
    """Upload GeoJSON data from a local file to AWS S3."""
    try:
        with open(local_file_path, 'r') as f:
            geojson_content = f.read()

        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=geojson_content,
            ContentType='application/geo+json'
        )
        print(f"[Success] - '{key}' uploaded to S3 successfully.")
    except Exception as e:
        print(f"Error uploading GeoJSON to S3: {e}")
        raise


def list_s3_objects(s3_client, bucket_name, prefix=''):
    """List objects in S3 bucket with optional prefix."""
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix
        )
        return response.get('Contents', [])
    except Exception as e:
        print(f"Error listing S3 objects: {e}")
        return []
