
from functions.setup import load_config, init_adls_directory, save_and_load_csv
from functions.pull_data import fetch_data_from_api
from functions.timeLabels import crimes_fileLabel
from functions.upload_ADLs import upload_dataframe_to_adls


def ingest_Arrests_Data(END_POINT="dpt3-jri9.json",
                        BASE_URL="https://data.cityofchicago.org/resource",
                        MAX_RECORDS=100,
                        TIME_OUT=10,
                        DELAY=1.5,
                        COLUMN_FILTER=["arrest_date", "race", "charge_1_description", "charge_1_type", "charge_1_class",
                                       "charge_2_description", "charge_2_type", "charge_2_class",
                                       "charge_3_description", "charge_3_type", "charge_3_class",
                                       "charge_4_description", "charge_4_type", "charge_4_class"],
                        SAVE_PATH='RawData/DataSet2',
                        STORAGE_ACCT_NAME="crimeinchicago",
                        FILE_SYSTEM_NAME="data-engineering-project",
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
    row_filter = "arrest_date<='2024-10-26T00:00:00'"

    # fetch data
    df = fetch_data_from_api(url,
                             api_key_id,
                             api_secret,
                             columns=column_filter,
                             row_filter=row_filter,
                             max_records=MAX_RECORDS,
                             timeout=TIME_OUT,
                             delay=DELAY)

    print("[Success] - Data fetched successfully and stored in df")
    print("\nData insights:")
    print("\ndf head: ")
    print(df.head())
    print("\ndf tail: ")
    print(df.tail())
    print(df.shape)
    print(f"Number of columns: {len(df.columns)}")
    print(f"Number of rows: {len(df)}")

    # Generate file label based on the date range
    csv_file_label = crimes_fileLabel(
        df, date_column="arrest_date", dataSource="Arrests")
    print("\n[Success] - Generated file label:", csv_file_label)

    # save data as csv
    df_read = save_and_load_csv(df, SAVE_PATH,
                                csv_file_label)

    # Initialize ADLS and upload data
    with open("sas.config") as f:
        sas_key = f.readline().strip()

    storageAcctName = STORAGE_ACCT_NAME
    fileSysName = FILE_SYSTEM_NAME
    dirName = DIR_NAME

    directory = init_adls_directory(storageAcctName,
                                    sas_key,
                                    fileSysName,
                                    dirName)

    upload_dataframe_to_adls(directory, df_read, csv_file_label)


# Allow this script to be run independently or imported
if __name__ == "__main__":
    ingest_Arrests_Data(MAX_RECORDS=205000,
                        DELAY=2,
                        TIME_OUT=20)
