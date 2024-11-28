
from functions.setup import load_config, init_adls_directory, save_and_load_csv
from functions.pull_data import fetch_data_from_api1
from functions.timeLabels import crimes_fileLabel2
from functions.upload_ADLs import upload_dataframe_to_adls


def ingest_crimes_data(END_POINT="ijzp-q8t2.json",
                       MAX_RECORDS=None,
                       COLUMN_FILTER=["case_number", "date", "primary_type", "description", "location_description",
                                      "arrest", "district", "community_area", "latitude", "longitude"],
                       # by default filters this years data only
                       ROW_FILTER="(date>='2024-01-01T00:00:00' AND date<='2024-11-27T00:00:00')",
                       STORAGE_ACCT="crimeinchicago",
                       FILE_SYSTEM_NAME="input-ingested-raw",
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

        # Init Azure Data Lake storage client
        with open("sas.config") as f:
            sas_key = f.readline().strip()

        # storage account name
        storageAcctName = STORAGE_ACCT
        # name of continer for input data - names rules apply
        Input_fileSysName = FILE_SYSTEM_NAME
        # name of directory within a container
        dirName = DIR_NAME

        directory = init_adls_directory(storageAcctName,
                                        sas_key,
                                        Input_fileSysName,
                                        dirName)

        # Upload to ADLS
        upload_dataframe_to_adls(directory, df_read, csv_file_label)


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
