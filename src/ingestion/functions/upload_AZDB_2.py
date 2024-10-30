from azure.storage.filedatalake import DataLakeServiceClient


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
    print(f"DataFrame uploaded as '{remote_file_name}' successfully.")
