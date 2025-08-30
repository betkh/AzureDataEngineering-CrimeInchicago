from s3_manager import S3Manager
import os
from dotenv import load_dotenv


def setup_s3_bucket():
    """Main function to setup S3 bucket and folder structure using S3Manager."""
    print("=" * 50)
    print("S3 BUCKET SETUP")
    print("=" * 50)

    try:
        # Load environment variables
        load_dotenv()
        
        # Get bucket name from environment
        bucket_name = os.getenv('S3_BUCKET_NAME', 'etl-chicago4')
        print(f"✓ Configuration loaded successfully")
        print(f"  Bucket: {bucket_name}")

        # Initialize S3 Manager
        s3_manager = S3Manager(bucket_name)
        print("✓ S3 Manager initialized")

        # Create bucket if it doesn't exist
        if not s3_manager.create_bucket():
            print("✗ Failed to create bucket")
            return False

        # Create folder structure
        if not s3_manager.create_prefix('ingested-raw'):
            print("✗ Failed to create folder structure")
            return False

        print("=" * 50)
        print("✓ S3 BUCKET SETUP COMPLETED!")
        print("=" * 50)
        return True
        
    except Exception as e:
        print(f"✗ Setup failed: {e}")
        return False


if __name__ == "__main__":
    setup_s3_bucket()
