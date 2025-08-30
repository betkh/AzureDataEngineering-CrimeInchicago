
from functions.setup import load_config, init_s3_storage, save_and_load_csv
from functions.pull_data import fetch_data_from_api1
from functions.timeLabels import crimes_fileLabel2
from functions.upload_s3 import upload_dataframe_to_s3


def ingest_crimes_data(END_POINT="ijzp-q8t2.json",
                       MAX_RECORDS=None,
                       COLUMN_FILTER=["case_number", "date", "primary_type", "description", "location_description",
                                      "arrest", "district", "community_area", "latitude", "longitude"],
                       # by default filters this years data only
                       ROW_FILTER="(date>='2024-01-01T00:00:00' AND date<='2024-02-27T00:00:00')",
                       AWS_REGION="us-east-1",
                       S3_BUCKET_NAME="crimeinchicago-data",
                       S3_PREFIX="input-ingested-raw/Crime2019_to_Present/"):
    """
    Ingests the 'Crimes' dataset and uploads to AWS S3.

    """

    print("DataSet1 ingestion - 'Crimes - 2001 to Present'")

    # Load API keys
    api_key_id, api_secret = load_config()

    # Fetch data via API
    print("Fetching data via API ...")

    # Construct the full URL
    BASE_URL = "https://data.cityofchicago.org/resource"
    url = f"{BASE_URL}/{END_POINT}"

    # filter records
    column_filter = COLUMN_FILTER

    # filter rows
    row_filter = ROW_FILTER

    # Fetch data from the specified API endpoint
    df = fetch_data_from_api1(url,                       # API endpoint
                              api_key_id,                # API key ID for authentication
                              api_secret,                # API secret for authentication
                              columns=column_filter,     # List of columns to retrieve
                              row_filter=row_filter,           # Filter to apply on rows
                              max_records=MAX_RECORDS)            # Maximum number of records to fetch in one go)                 # Delay between successive API calls

    print("[Success] - Data fetch from API successful and data stored in df")
    print("\nData insights:")
    print(df.head())
    print(df.shape)
    print(f"Number of columns: {len(df.columns)}")
    print(f"Number of rows: {len(df)}")

    if len(df) == 0:

        print("No New Data, come back later!")
        pass

    else:

        # Generate file label based on the date range
        csv_file_label = crimes_fileLabel2(
            df, date_column="date", dataSource="Crimes")

        print("\n[Success] - Generated file label:", csv_file_label)

        # save data as csv
        loaclSAVE_PATH = 'RawData/DataSet1'
        df_read = save_and_load_csv(df,
                                    loaclSAVE_PATH,
                                    csv_file_label)

        # Load AWS credentials from config
        config = load_config('aws_config.ini')
        aws_access_key_id = config.get('DEFAULT', 'AWS_ACCESS_KEY_ID')
        aws_secret_access_key = config.get('DEFAULT', 'AWS_SECRET_ACCESS_KEY')
        aws_region = config.get('DEFAULT', 'AWS_REGION')
        s3_bucket_name = config.get('DEFAULT', 'S3_BUCKET_NAME')

        # Init AWS S3 client
        s3_client = init_s3_storage(aws_access_key_id,
                                    aws_secret_access_key,
                                    aws_region,
                                    s3_bucket_name)

        # Create S3 key (file path in bucket)
        s3_key = f"{S3_PREFIX}{csv_file_label}"

        # Upload to S3
        upload_dataframe_to_s3(s3_client, s3_bucket_name, s3_key, df_read)


# Allow this script to be run independently or imported
if __name__ == "__main__":

    # ingest data for last 5+ years -  2019, 2020, 2021, 2022, 2023 & current year

    # start Date, End Date, corresponding MAX_RECORDS
    DateFilter = [
        ("2019-01-01T00:00:00", "2019-12-31T23:45:00", 57000),
        ("2020-01-01T00:00:00", "2020-12-31T23:45:00", 35000),
        ("2021-01-01T00:00:00", "2021-12-31T23:45:00", 27000),
        ("2022-01-01T00:00:00", "2022-12-31T23:45:00", 29000),
        ("2023-01-01T00:00:00", "2023-12-31T23:45:00", 32000),
        ("2024-01-01T00:00:00", "2024-11-27T14:00:00", 31000)
    ]

    recentDataFilter = [("2024-11-27T14:00:00", "2024-11-27T14:00:00", 60000),]

    for start_date, end_date, max_records in recentDataFilter:

        # filter by date
        rowFilter = f"(date>='{start_date}' AND date<='{end_date}')"

        # ingest arrests data
        ingest_crimes_data(ROW_FILTER=rowFilter, MAX_RECORDS=max_records)
