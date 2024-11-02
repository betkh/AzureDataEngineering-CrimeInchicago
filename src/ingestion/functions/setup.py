import os
import pandas as pd
import configparser
from .upload_ADLs import init_storage_acct


# Utility function to load configuration values
def load_config(file_path='keys.config'):
    config = configparser.ConfigParser()
    config.read(file_path)
    api_key_id = config.get('DEFAULT', 'API_KEY_ID')
    api_secret = config.get('DEFAULT', 'API_SECRET')
    print("[Success] - Accessed API keys")
    return api_key_id, api_secret


# Utility function to initialize ADLS file system and directory
def init_adls_directory(storage_acct_name,
                        sas_key,
                        fs_name,
                        dir_name):
    """
    Initializes and returns a directory in Azure Data Lake Storage (ADLS).

    This function creates the specified file system and directory if they don't already exist.

    Parameters:
    - storage_acct_name (str): Name of the Azure storage account.
    - sas_key (str): Shared Access Signature (SAS) key for authenticating with ADLS.
    - fs_name (str): Name of the file system to create or access within ADLS.
    - dir_name (str): Name of the directory within the file system to create or access.

    Returns:
    - directory: The ADLS directory client, which can be used for further file operations.
    """

    client = init_storage_acct(storage_acct_name, sas_key)

    if not client.get_file_system_client(file_system=fs_name).exists():
        file_sys = client.create_file_system(file_system=fs_name)
        print(f"[Success] - File System '{fs_name}' created successfully.")
    else:
        file_sys = client.get_file_system_client(file_system=fs_name)

    if not file_sys.get_directory_client(dir_name).exists():
        directory = file_sys.create_directory(dir_name)
        print(f"[Success] - Directory '{dir_name}' created successfully.")
    else:
        directory = file_sys.get_directory_client(dir_name)

    return directory


# Utility function to save DataFrame to CSV and load it for verification
def save_and_load_csv(df, save_dir, file_label):
    file_path = os.path.join(save_dir, file_label)
    df.to_csv(file_path, index=False)
    print("[Success] - Data saved to CSV.")

    df_read = pd.read_csv(file_path)
    print("[Success] - CSV file read successfully!")
    return df_read
