import configparser
from functions.pull_data import fetch_data_from_api
from functions.timeLabels import create_file_label_from_dates
from ingestion.functions.upload_ADLs import init_storage_acct, upload_dataframe_to_adls

# DataSet1.py - script to extract data from its source and load into ADLS.

"""
Ingest dynamic data sets to ADLS

Source #1: "Crimes - 2001 to Present"

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

if __name__ == "__main__":

    print("DataSet1 ingestion - 'Crimes - 2001 to Present'")

    # init config parser
    print("Parsing 'keys.config' to get access keys ...")
    config = configparser.ConfigParser()
    config.read('keys.config')

    # get  API key id & secret from config file
    api_key_id = config.get('DEFAULT', 'API_KEY_ID')
    api_secret = config.get('DEFAULT', 'API_SECRET')
    print("[Success] - Accessed API keys")

    # Fetch data via API
    print("Fetching data via API ...")

    url = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
    columns = ["date", "primary_type", "description", "location_description",
               "arrest", "beat", "district", "ward", "community_area", "latitude", "longitude"]
    row_filter = "arrest=true AND date>='2019-01-01T00:00:00' AND date<='2024-10-22T00:00:00'"

    df = fetch_data_from_api(url, api_key_id, api_secret,
                             columns=columns, row_filter=row_filter, max_records=500000)

    print("[Success] - Data fetch from API successful and data stored in df")
    print("\nData insights:")
    print(df.head())
    print(df.shape)
    print(f"Number of columns: {len(df.columns)}")
    print(f"Number of rows: {len(df)}")

    # Generate label based on the date range
    csv_file_label = create_file_label_from_dates(df, "date", "Crimes")
    print("\n[Success] - Generated file label:", csv_file_label)

    # Initialize Azure Data Lake storage client
    with open("sas.config") as f:
        sas_key = f.readline().strip()
    dLake_serv_client = init_storage_acct("assign1storebekalue", sas_key)

    # Define the file system and directory names
    fsName = "data-engineering-project"
    dirName = "Crime2019_to_Present"

    # create the file system if it does not exist
    if not dLake_serv_client.get_file_system_client(file_system=fsName).exists():
        file_system = dLake_serv_client.create_file_system(file_system=fsName)
        print(f"[Success] - File System '{fsName}' created successfully.")
    else:
        file_system = dLake_serv_client.get_file_system_client(
            file_system=fsName)

    # create the directory if it does not exist
    if not file_system.get_directory_client(dirName).exists():
        directory = file_system.create_directory(dirName)
        print(f"[Success] - Directory '{dirName}' created successfully.")
    else:
        directory = file_system.get_directory_client(dirName)

    # Upload the DataFrame to ADLS
    upload_dataframe_to_adls(directory, df, csv_file_label)
    print("[Success] - Data uploaded to ADLS successfully.")
