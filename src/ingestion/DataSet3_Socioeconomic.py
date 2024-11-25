
from functions.setup import load_config, init_adls_directory, save_and_load_csv
from functions.pull_data import fetch_data_from_api
from functions.timeLabels import crimes_fileLabel, socio_fileLabel
from functions.upload_ADLs import upload_dataframe_to_adls


def ingest_socioecon_areas_data(END_POINT="2ui7-wiq8.json",
                                BASE_URL="https://data.cityofchicago.org/resource",
                                MAX_RECORDS=300000,
                                TIME_OUT=10,
                                DELAY=1.5,
                                SAVE_PATH='RawData/DataSet3'):
    """
    Ingests the 'Socioeconomic areas' dataset and uploads to Azure Data Lake Storage (ADLS).

    About Data:
    Source #3: Socioeconomically Disadvantaged Areas - (STATIC source)

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

    print("DataSet3 ingestion - 'Socioeconomic Areas...'")

    # Load API keys
    api_key_id, api_secret = load_config()

    # Fetch data via API
    print("Fetching data via API ...")

    # Construct the full URL
    url = f"{BASE_URL}/{END_POINT}"

    # fetch_data_from_api(url, api_key_id, api_secret, columns=None, row_filter=None, max_records=100000, timeout=10, delay=1)

    df = fetch_data_from_api(url, api_key_id, api_secret,
                             max_records=MAX_RECORDS,
                             timeout=TIME_OUT,
                             delay=DELAY)

    print("[Success] - Data fetch from API successful and data stored in df")
    print("\nData insights:")
    print(df.head())
    print(df.shape)
    print(f"Number of columns: {len(df.columns)}")
    print(f"Number of rows: {len(df)}")

    # Generate file label based on the date range
    csv_fileLabel = socio_fileLabel(df, label_="socio_econ_areas")

    print("\n[Success] - Generated file label:", csv_fileLabel)

    # save data as cvs
    df_read = save_and_load_csv(df,
                                SAVE_PATH,
                                csv_fileLabel)

    # Init Azure Data Lake storage client
    with open("sas.config") as f:
        sas_key = f.readline().strip()

    storageAcctName = "crimeinchicago"
    fileSysName = "data-engineering-project"
    dirName = "Socioeconomic Areas"

    directory = init_adls_directory(storageAcctName,
                                    sas_key,
                                    fileSysName,
                                    dirName)

    # Upload to ADLS
    upload_dataframe_to_adls(directory, df_read, csv_fileLabel)


# Allow this script to be run independently or imported
if __name__ == "__main__":

    ingest_socioecon_areas_data(MAX_RECORDS=300000,
                                DELAY=3.6,
                                TIME_OUT=20)
