
from functions.setup import load_config, init_adls_directory
from functions.pull_data import fetch_geojson_from_api
from functions.upload_ADLs import upload_geojson_to_adls
import json


def ingest_socioecon_areas_data(END_POINT="2ui7-wiq8.geojson",
                                BASE_URL="https://data.cityofchicago.org/resource"):
    """
    Ingests the 'Socioeconomic areas' dataset and uploads to Azure Data Lake Storage (ADLS).

    About Data:
    Source #3: Socioeconomically Disadvantaged Areas - (STATIC source)

        - API Endpoint	https://data.cityofchicago.org/resource/2ui7-wiq8.geojson
        - API Documentation	https://dev.socrata.com/foundry/data.cityofchicago.org/2ui7-wiq8
        - Data Owner	Department of Planning and Development
        - Date Created	October 13, 2022
        - Last Update	July 12, 2024
        - Data Update Frequency	N/A
        - Rows:	254K
        - Columns: 1
        - 254K rows x 1 column
    """

    print("DataSet3 ingestion - 'Socioeconomically disadvantaged Areas...'")

    # Load API keys
    api_key_id, api_secret = load_config()

    # Fetch data via API
    print("Fetching data via API ...")

    # Construct the full URL
    url = f"{BASE_URL}/{END_POINT}"

    geojson_data = fetch_geojson_from_api(url, api_key_id, api_secret)

    # Check if the response contains data
    if geojson_data:
        print("[Success] - GeoJSON data fetched successfully.")
        print("GeoJSON data preview:")
        # Pretty print the GeoJSON data
        # print(json.dumps(geojson_data, indent=2))
    else:
        print("[Error] - GeoJSON data is empty or null.")

        print("[Success] - Data fetch from API successful")

    #  file label
    geojson_file_label = "socioecon_disadvantaged_Areas.geojson"
    SAVE_PATH = 'RawData/DataSet3'
    geojson_file_path = f"{SAVE_PATH}/{geojson_file_label}"

    with open(geojson_file_path, "w") as f:
        json.dump(geojson_data, f)  # Save GeoJSON data as a JSON file

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
    upload_geojson_to_adls(directory, geojson_file_path, geojson_file_label)


# Allow this script to be run independently or imported
if __name__ == "__main__":

    ingest_socioecon_areas_data()
