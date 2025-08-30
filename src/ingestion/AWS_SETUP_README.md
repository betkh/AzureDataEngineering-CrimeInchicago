# AWS S3 Setup Guide

This guide will help you set up the necessary AWS credentials for the Chicago Crime Data ingestion pipeline using AWS S3 instead of Azure.

## Prerequisites

1. An AWS account
2. AWS CLI installed (optional but recommended)
3. Access to AWS Console

## Step 1: Create AWS S3 Bucket (if not already created)

### Option A: Using AWS Console

1. Go to [AWS Console](https://console.aws.amazon.com)
2. Navigate to S3 service
3. Click "Create bucket"
4. Fill in the required details:
   - **Bucket name**: `crimeinchicago-data` (or your preferred name)
   - **Region**: Choose a region close to you (e.g., us-east-1)
   - **Block Public Access**: Keep all blocks enabled for security
5. Click "Create bucket"

### Option B: Using AWS CLI

```bash
aws s3 mb s3://crimeinchicago-data --region us-east-1
```

## Step 2: Create IAM User and Access Keys

1. Go to AWS Console → IAM
2. Click "Users" → "Add user"
3. Fill in the details:
   - **User name**: `crime-data-ingestion`
   - **Access type**: Programmatic access
4. Click "Next: Permissions"
5. Click "Attach existing policies directly"
6. Search for and select "AmazonS3FullAccess" (or create a custom policy with minimal permissions)
7. Complete the user creation
8. **Important**: Copy the Access Key ID and Secret Access Key

## Step 3: Configure the Application

1. Open `src/ingestion/aws_config.ini`
2. Replace the placeholder values:
   - `your_api_key_id_here` with your Chicago Data Portal API key ID
   - `your_api_secret_here` with your Chicago Data Portal API secret
   - `your_aws_access_key_id_here` with your AWS Access Key ID
   - `your_aws_secret_access_key_here` with your AWS Secret Access Key
   - `us-east-1` with your preferred AWS region
   - `crimeinchicago-data` with your S3 bucket name
3. Save the file

## Step 4: Install Required Dependencies

Add boto3 to your project dependencies:

```bash
pip install boto3
```

Or add to your Pipfile:

```toml
[packages]
boto3 = "*"
```

## Step 5: Test the Configuration

1. Run the ingestion script:
   ```bash
   cd src/ingestion
   python DataSet1_Crimes.py
   ```

2. Check for any authentication errors in the output

## Security Best Practices

1. **Never commit credentials to version control**
   - Add `*.ini` to your `.gitignore` file
   - Use environment variables in production

2. **Use least privilege principle**
   - Create a custom IAM policy with only the required S3 permissions:
   ```json
   {
       "Version": "2012-10-17",
       "Statement": [
           {
               "Effect": "Allow",
               "Action": [
                   "s3:GetObject",
                   "s3:PutObject",
                   "s3:ListBucket",
                   "s3:CreateBucket"
               ],
               "Resource": [
                   "arn:aws:s3:::crimeinchicago-data",
                   "arn:aws:s3:::crimeinchicago-data/*"
               ]
           }
       ]
   }
   ```

3. **Use AWS credentials file (alternative)**
   - Create `~/.aws/credentials` file:
   ```ini
   [default]
   aws_access_key_id = YOUR_ACCESS_KEY
   aws_secret_access_key = YOUR_SECRET_KEY
   ```
   - Then modify the code to use default credentials

## Alternative: Using Environment Variables

Instead of using the config file, you can set environment variables:

```bash
export AWS_ACCESS_KEY_ID=your_access_key_id
export AWS_SECRET_ACCESS_KEY=your_secret_access_key
export AWS_DEFAULT_REGION=us-east-1
```

Then modify the code to use environment variables:

```python
import os
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
aws_region = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
```

## Troubleshooting

### Common Issues:

1. **"Access Denied" error**
   - Check if the IAM user has the required S3 permissions
   - Verify the bucket name and region

2. **"No such file or directory" error**
   - The bucket will be created automatically by the code
   - Ensure the IAM user has "CreateBucket" permission

3. **"Invalid credentials" error**
   - Verify the Access Key ID and Secret Access Key
   - Check if the credentials are expired

4. **"Bucket already exists" error**
   - This is normal if the bucket already exists
   - The code will use the existing bucket

### Getting Help:

- Check AWS S3 documentation: https://docs.aws.amazon.com/s3/
- Review boto3 documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
- AWS IAM documentation: https://docs.aws.amazon.com/iam/

## Cost Considerations

- S3 Standard storage: ~$0.023 per GB per month
- Data transfer: Free for uploads, charges for downloads
- Request pricing: ~$0.0004 per 1,000 PUT requests
- Consider using S3 Intelligent Tiering for cost optimization
