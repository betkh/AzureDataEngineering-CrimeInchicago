
from functions.setup import load_config, init_adls_directory, save_and_load_csv
from functions.pull_data import fetch_data_from_api
from functions.timeLabels import crimes_fileLabel, socio_fileLabel
from functions.upload_ADLs import upload_dataframe_to_adls


def ingest_socioecon_indicators(END_POINT="kn9c-c2s2.json",
                                BASE_URL="https://data.cityofchicago.org/resource",
                                STORAGE_ACCT="crimeinchicago",
                                FILE_SYSTEM_NAME="input-ingested-raw",
                                DIR_NAME="Socioeconomic_Indicators"):
    """
    Ingests the 'Socioeconomic indicators' dataset and uploads to Azure Data Lake Storage (ADLS).

    About Data:
    Source #4: socioeconomic indicators in Chicago, 2008 – 2012

        - API Endpoint:	https://data.cityofchicago.org/resource/kn9c-c2s2.json
        - API Documentation:	https://dev.socrata.com/foundry/data.cityofchicago.org/kn9c-c2s2
        - Data Owner:	Public Health
        - Date Created:	January 5, 2012
        - Last Update:	September 12, 2014
        - Data Update Frequency:	Updated as new data becomes available
        - Rows:	78
        - Columns: 9
        - 78 rows x 9 columns
    """

    print("DataSet4 ingestion - 'Socioeconomic Indicators...'")

    # Load API keys
    api_key_id, api_secret = load_config()

    # Fetch data via API
    print("Fetching data via API ...")

    # Construct the full URL
    url = f"{BASE_URL}/{END_POINT}"

    # no need to specify max records here because we have only 78 records
    df = fetch_data_from_api(url,
                             api_key_id,
                             api_secret,
                             max_records=100)

    print("[Success] - Data fetch from API successful and data stored in df")
    print("\nData insights:")
    print(df.head())
    print(df.shape)
    print(f"Number of columns: {len(df.columns)}")
    print(f"Number of rows: {len(df)}")

    # Generate file label based on the date range
    csv_fileLabel = socio_fileLabel(df, label_="socio_econ_indicators")

    print("\n[Success] - Generated file label:", csv_fileLabel)

    # save data as cvs
    loaclSAVE_PATH = 'RawData/DataSet4'
    df_read = save_and_load_csv(df,
                                loaclSAVE_PATH,
                                csv_fileLabel)

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
    upload_dataframe_to_adls(directory, df_read, csv_fileLabel)


# Allow this script to be run independently or imported
if __name__ == "__main__":

    ingest_socioecon_indicators()
