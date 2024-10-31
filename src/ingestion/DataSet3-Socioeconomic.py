import configparser
from functions.pull_data import fetch_data_from_api
from functions.timeLabels import create_file_label_geo
from functions.upload_ADLs import init_storage_acct, upload_dataframe_to_adls

# DataSet1.py - script to extract data from its source and load into ADLS.

"""
Ingest dynamic data sets to ADLS

Source #3: Socioeconomically Disadvantaged Areas 

    - API Endpoint	https://data.cityofchicago.org/resource/2ui7-wiq8.json
    - API Documentation	https://dev.socrata.com/foundry/data.cityofchicago.org/2ui7-wiq8
    - Data Owner	Department of Planning and Development
    - Date Created	October 13, 2022
    - Last Update	July 12, 2024
    - Data Update Frequency	N/A
    - Rows:	254K
    - Columns: 1
    - 254K rows x 1 column
"""

if __name__ == "__main__":

    print("DataSet3 ingestion - 'Socioeconomic ...'")

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

    url = "https://data.cityofchicago.org/resource/2ui7-wiq8.json"

    df = fetch_data_from_api(url, api_key_id, api_secret, max_records=50)

    print("[Success] - Data fetch from API successful and data stored in df")
    print("\nData insights:")
    print(df.head())
    print(df.shape)
    print(f"Number of columns: {len(df.columns)}")
    print(f"Number of rows: {len(df)}")

    # Generate label based on the date range
    csv_file_label = create_file_label_geo(df)
    print("\n[Success] - Generated file label:", csv_file_label)

    # Initialize Azure Data Lake storage client
    with open("sas.config") as f:
        sas_key = f.readline().strip()

    # storageAcctName = "crimeinchicago"
    dLake_serv_client = init_storage_acct("crimeinchicago", sas_key)

    # Define the file system and directory names
    fsName = "data-engineering-project"
    dirName = "Socioeconomic Areas"

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
