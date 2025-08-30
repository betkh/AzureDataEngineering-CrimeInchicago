import boto3
import os
from dotenv import load_dotenv
from botocore.exceptions import ClientError


class S3Manager:
    """S3 Manager class for handling all S3 operations."""
    
    def __init__(self, bucket_name='etl-chicago4'):
        """Initialize S3 client with credentials from .env file."""
        load_dotenv()
        
        # Get AWS credentials
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.aws_region = os.getenv('AWS_REGION')
        self.bucket_name = bucket_name
        
        # Validate credentials
        if not all([self.aws_access_key_id, self.aws_secret_access_key, self.aws_region]):
            raise ValueError("AWS credentials not found in environment variables")
        
        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region
        )
        
        print(f"S3 Manager initialized for bucket: {self.bucket_name}")
    
    def create_bucket(self):
        """Create S3 bucket if it doesn't exist."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            print(f"Bucket '{self.bucket_name}' already exists")
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                try:
                    if self.aws_region == 'us-east-1':
                        self.s3_client.create_bucket(Bucket=self.bucket_name)
                    else:
                        self.s3_client.create_bucket(
                            Bucket=self.bucket_name,
                            CreateBucketConfiguration={'LocationConstraint': self.aws_region}
                        )
                    print(f"Bucket '{self.bucket_name}' created successfully")
                    return True
                except ClientError as create_error:
                    print(f"Error creating bucket: {create_error}")
                    return False
            else:
                print(f"Error checking bucket: {e}")
                return False
    
    def create_prefix(self, prefix):
        """Create a prefix (folder) in the S3 bucket."""
        try:
            # Create an empty object with the prefix to establish the "folder"
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=f"{prefix}/"
            )
            print(f"Prefix '{prefix}' created in bucket '{self.bucket_name}'")
            return True
        except Exception as e:
            print(f"Error creating prefix: {e}")
            return False
    
    def upload_csv(self, local_file_path, s3_key):
        """Upload a CSV file to S3."""
        try:
            with open(local_file_path, 'rb') as f:
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=f.read(),
                    ContentType='text/csv'
                )
            print(f"Uploaded '{local_file_path}' to S3 as '{s3_key}'")
            return True
        except Exception as e:
            print(f"Error uploading to S3: {e}")
            return False
    
    def list_objects(self, prefix=''):
        """List objects in the bucket with optional prefix."""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            return response.get('Contents', [])
        except Exception as e:
            print(f"Error listing objects: {e}")
            return []


def upload_latest_csv():
    """Upload the latest CSV file from RawData/DataSet1 to S3."""
    try:
        # Initialize S3 Manager
        s3_manager = S3Manager()
        
        # Find the latest CSV file
        local_dir = 'RawData/DataSet1'
        if not os.path.exists(local_dir):
            print(f"Error: Directory '{local_dir}' does not exist")
            return False
        
        csv_files = [f for f in os.listdir(local_dir) if f.endswith('.csv')]
        if not csv_files:
            print(f"No CSV files found in '{local_dir}'")
            return False
        
        # Get the most recent CSV file
        latest_csv = max(csv_files, key=lambda x: os.path.getctime(os.path.join(local_dir, x)))
        local_file_path = os.path.join(local_dir, latest_csv)
        
        # S3 key with prefix
        s3_key = f"ingested-raw/{latest_csv}"
        
        print(f"Uploading: {latest_csv}")
        print(f"Local path: {local_file_path}")
        print(f"S3 location: s3://{s3_manager.bucket_name}/{s3_key}")
        
        # Upload the file
        return s3_manager.upload_csv(local_file_path, s3_key)
        
    except Exception as e:
        print(f"Error: {e}")
        return False


if __name__ == "__main__":
    print("==========================================")
    print("Uploading latest CSV file to S3")
    print("==========================================")
    
    success = upload_latest_csv()
    
    if success:
        print("\n[Success] - CSV file uploaded to S3 successfully!")
    else:
        print("\n[Error] - Failed to upload CSV file to S3")
    
    print("==========================================")
