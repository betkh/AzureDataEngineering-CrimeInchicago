import configparser
from functions.pull_data import fetch_data_from_api
from functions.timeLabels import create_file_label_from_dates
from functions.upload_ADLs import init_storage_acct, upload_dataframe_to_adls

# DataSet1.py - script to extract data from its source and load into ADLS.

"""
Ingest dynamic data sets to ADLS

Source #2: "Arrests "

    - Dataset URL	Arrests
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

if __name__ == "__main__":

    print("DataSet2 ingestion - 'Arrests'")

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

    url = "https://data.cityofchicago.org/resource/dpt3-jri9.json"
    columns = ["arrest_date", "race", "charge_1_description", "charge_1_type", "charge_1_class",
               "charge_2_description", "charge_2_type", "charge_2_class",
               "charge_3_description", "charge_3_type", "charge_3_class",
               "charge_4_description", "charge_4_type", "charge_4_class",]

    # fetch till recent data
    row_filter = "arrest_date<='2024-10-26T00:00:00'"

    df = fetch_data_from_api(url, api_key_id, api_secret,
                             columns=columns, row_filter=row_filter, max_records=50)

    print("[Success] - Data fetch from API successful and data stored in df")
    print("\nData insights:")
    print(df.head())
    print(df.shape)
    print(f"Number of columns: {len(df.columns)}")
    print(f"Number of rows: {len(df)}")

    # Generate label based on the date range
    csv_file_label = create_file_label_from_dates(df, "arrest_date", "Arrests")
    print("\n[Success] - Generated file label:", csv_file_label)

    # Initialize Azure Data Lake storage client
    with open("sas.config") as f:
        sas_key = f.readline().strip()

    # storageAcctName = "crimeinchicago"
    dLake_serv_client = init_storage_acct("crimeinchicago", sas_key)

    # Define the file system and directory names
    fsName = "data-engineering-project"
    dirName = "Arrests"

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
