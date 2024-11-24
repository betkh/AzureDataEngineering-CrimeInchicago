
from functions.setup import load_config, init_adls_directory, save_and_load_csv
from functions.pull_data import fetch_data_from_api
from functions.timeLabels import crimes_fileLabel
from functions.upload_ADLs import upload_dataframe_to_adls


def ingest_crimes_data(END_POINT="ijzp-q8t2.json",
                       BASE_URL="https://data.cityofchicago.org/resource",
                       MAX_RECORDS=100,
                       TIME_OUT=10,
                       DELAY=1.5,
                       COLUMN_FILTER=["date", "primary_type", "description", "location_description",
                                      "arrest", "beat", "district", "ward", "community_area", "latitude", "longitude"],
                       ROW_FILTER="arrest=true AND date>='2019-01-01T00:00:00' AND date<='2024-10-22T00:00:00'",
                       SAVE_PATH='RawData/DataSet1',
                       STORAGE_ACCT_NAME="crimeinchicago",
                       FILE_SYSTEM_NAME="data-engineering-project",
                       DIR_NAME="Crime2019_to_Present"):
    """
    Ingests the 'Crimes' dataset and uploads to Azure Data Lake Storage (ADLS).

    About Data:
    Source #1: "Crimes - 2001 to Present" - (DYNAMIC source)

        - About Data:	https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2/about_data
        - API Endpoint:	https://data.cityofchicago.org/resource/ijzp-q8t2.json
        - API Doc:	https://dev.socrata.com/foundry/data.cityofchicago.org/ijzp-q8t2
        - Data Owner: 	Chicago Police Department
        - Date Created:	September 30, 2011
        - Data Update:  Frequency	Daily
        - Rows: 8.19M (each row represents a reported crime, anonymized to the block level)
        - Columns: 22
        - data is too big 8.1M records X 22 columns, pulled only subset of rows and columns (year after 2020 and other filters)
    """

    print("DataSet1 ingestion - 'Crimes - 2001 to Present'")

    # Load API keys
    api_key_id, api_secret = load_config()

    # Fetch data via API
    print("Fetching data via API ...")

    # Construct the full URL
    url = f"{BASE_URL}/{END_POINT}"

    # filter records
    column_filter = COLUMN_FILTER

    # filter rows
    row_filter = ROW_FILTER

    # Fetch data from the specified API endpoint
    df = fetch_data_from_api(url,                       # API endpoint
                             api_key_id,                # API key ID for authentication
                             api_secret,                # API secret for authentication
                             columns=column_filter,     # List of columns to retrieve
                             row_filter=row_filter,     # Filter to apply on rows
                             max_records=MAX_RECORDS,            # Maximum number of records to fetch in one go
                             timeout=TIME_OUT,                # Request timeout in seconds
                             delay=DELAY)                 # Delay between successive API calls

    print("[Success] - Data fetch from API successful and data stored in df")
    print("\nData insights:")
    print(df.head())
    print(df.shape)
    print(f"Number of columns: {len(df.columns)}")
    print(f"Number of rows: {len(df)}")

    # Generate file label based on the date range
    csv_file_label = crimes_fileLabel(
        df, date_column="date", dataSource="Crimes")

    print("\n[Success] - Generated file label:", csv_file_label)

    # save data as csv
    df_read = save_and_load_csv(df,
                                SAVE_PATH,
                                csv_file_label)

    # Init Azure Data Lake storage client
    with open("sas.config") as f:
        sas_key = f.readline().strip()

    storageAcctName = STORAGE_ACCT_NAME
    fileSysName = FILE_SYSTEM_NAME
    dirName = DIR_NAME

    directory = init_adls_directory(storageAcctName,
                                    sas_key,
                                    fileSysName,
                                    dirName)

    # Upload to ADLS
    upload_dataframe_to_adls(directory, df_read, csv_file_label)


# Allow this script to be run independently or imported
if __name__ == "__main__":
    ingest_crimes_data(MAX_RECORDS=205000,
                       DELAY=2,
                       TIME_OUT=20)
