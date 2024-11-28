
from functions.setup import load_config, init_adls_directory, save_and_load_csv
from functions.pull_data import fetch_data_from_api2
from functions.timeLabels import crimes_fileLabel2
from functions.upload_ADLs import upload_dataframe_to_adls


def ingest_Arrests_Data(END_POINT="dpt3-jri9.json",
                        BASE_URL="https://data.cityofchicago.org/resource",
                        MAX_RECORDS=None,
                        COLUMN_FILTER=["case_number", "arrest_date", "race",
                                       "charge_1_description", "charge_1_type", "charge_1_class"],
                        ROW_FILTER="(arrest_date>='2024-01-01T00:00:00' AND arrest_date<='2024-11-27T00:00:00') ",
                        STORAGE_ACCT="crimeinchicago",
                        FILE_SYSTEM_NAME="input-ingested-raw",
                        DIR_NAME="Arrests"):
    """
    Ingests the 'Arrests' dataset and uploads to Azure Data Lake Storage (ADLS).

    About Data:
    Source #2: "Arrests " - (DYNAMIC source)

    - About Data	https://data.cityofchicago.org/Public-Safety/Arrests/dpt3-jri9/about_data
    - API Endpoint	https://data.cityofchicago.org/resource/dpt3-jri9.json
    - API Documentation	https://dev.socrata.com/foundry/data.cityofchicago.org/dpt3-jri9
    - Owner: Chicago Police Department
    - Created:	June 22, 2020
    - Update Frequency:	Daily
    - Rows: 660K (each row represents an arrest, anonymized to the block level)
    - Columns: 24
    - 660K rows x 24 columns
    """

    print("DataSet2 ingestion - 'Arrests'")

    # Load API keys
    api_key_id, api_secret = load_config()

    # Fetch data via API
    print("Fetching data via API ...")

    # Construct the full URL
    url = f"{BASE_URL}/{END_POINT}"

    # filter data before pull
    column_filter = COLUMN_FILTER

    # filter all recent arrests
    row_filter = ROW_FILTER

    # fetch data
    df = fetch_data_from_api2(url,
                              api_key_id,
                              api_secret,
                              columns=column_filter,
                              row_filter=row_filter,
                              max_records=MAX_RECORDS)

    print("[Success] - Data fetched successfully and stored in df")
    print("\nData insights:")
    print("\ndf head: ")
    print(df.head())
    print("\ndf tail: ")
    print(df.tail())
    print(df.shape)
    print(f"Number of columns: {len(df.columns)}")
    print(f"Number of rows: {len(df)}")

    if len(df) == 0:

        print("No New Data, come back later!")
        pass

    else:

        # Generate file label based on the date range
        csv_file_label = crimes_fileLabel2(
            df, date_column="arrest_date", dataSource="Arrests")
        print("\n[Success] - Generated file label:", csv_file_label)

        # save data as csv
        loaclSAVE_PATH = 'RawData/DataSet2'
        df_read = save_and_load_csv(df, loaclSAVE_PATH,
                                    csv_file_label)

        # Initialize ADLS and upload data
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

        upload_dataframe_to_adls(directory, df_read, csv_file_label)


# Allow this script to be run independently or imported
if __name__ == "__main__":

    # ingest data for last 5+ years -  2019, 2020, 2021, 2022, 2023 & current year

    # start Date, End Date, corresponding MAX_RECORDS
    DateFilter = [
        ("2019-01-01T00:00:00", "2019-12-31T23:45:00", 51000),
        ("2020-01-01T00:00:00", "2020-12-31T23:45:00", 32000),
        ("2021-01-01T00:00:00", "2021-12-31T23:45:00", 25000),
        ("2022-01-01T00:00:00", "2022-12-31T23:45:00", 25500),
        ("2023-01-01T00:00:00", "2023-12-31T23:45:00", 29000),
        ("2024-01-01T00:00:00", "2024-11-27T14:00:00", 30000)
    ]

    recentDataFilter = [("2024-11-27T14:00:00", "2024-12-31T14:23:40", 60000),]

    for start_date, end_date, max_records in recentDataFilter:

        # filter by date
        rowFilter = f"(arrest_date>='{start_date}' AND arrest_date<='{end_date}')"

        # ingest arrests data
        ingest_Arrests_Data(ROW_FILTER=rowFilter, MAX_RECORDS=max_records)
