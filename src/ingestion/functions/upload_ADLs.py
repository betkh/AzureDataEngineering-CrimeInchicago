from azure.storage.filedatalake import DataLakeServiceClient
import json


def init_storage_acct(storage_account_name, storage_account_key):
    """Initialize the Azure Data Lake service client."""
    service_client = DataLakeServiceClient(account_url=f"https://{storage_account_name}.dfs.core.windows.net",
                                           credential=storage_account_key)
    return service_client


def upload_dataframe_to_adls(directory_client, df, remote_file_name):
    """Upload a DataFrame as a CSV file to Azure Data Lake."""

    if not remote_file_name.endswith('.csv'):
        remote_file_name += '.csv'

    # Convert DataFrame to CSV format
    csv_data = df.to_csv(index=False)

    # Create a file client in the specified directory
    file_client = directory_client.create_file(remote_file_name)

    # Upload the CSV data
    file_client.upload_data(csv_data, overwrite=True)
    print(f"[Success] - '{remote_file_name}' uploaded to ADLS successfully.")


def upload_geojson_to_adls(directory_client, local_file_path, remote_file_name):
    """
    Upload GeoJSON data from a local file to Azure Data Lake.

    """
    if not remote_file_name.endswith('.geojson'):
        remote_file_name += '.geojson'

    # Read the content of the GeoJSON file
    with open(local_file_path, "r") as f:
        geojson_content = f.read()

    # Create a file client in the specified directory
    file_client = directory_client.create_file(remote_file_name)

    # Upload the GeoJSON content
    file_client.upload_data(geojson_content, overwrite=True)
    print(f"[Success] - '{remote_file_name}' uploaded to ADLS successfully.")
