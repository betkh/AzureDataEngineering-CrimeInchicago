import time
import requests
import traceback
import pandas as pd
from tqdm import tqdm


# how to query more than 1000 rows ? https://support.socrata.com/hc/en-us/articles/202949268-How-to-query-more-than-1000-rows-of-a-dataset
# API doc https://dev.socrata.com/consumers/getting-started


def fetch_data_from_api(url, api_key_id, api_secret, columns=None, row_filter=None, timeout=10, delay=4, max_records=None):
    """Fetch data from the specified API with selected columns and row filter."""
    headers = {
        "X-Api-Key-Id": api_key_id,
        "X-Api-Secret": api_secret
    }

    # Pagination parameters
    pagelimit = 1000
    offset = 0
    all_data = []

    # Initialize tqdm progress bar
    with tqdm(total=max_records, desc="Fetching records") as pbar:
        while len(all_data) < max_records:
            # Request parameters, including filters and selected columns
            params = {
                "$limit": pagelimit,
                "$offset": offset,
                "$select": ','.join(columns) if columns else '*',
                "$where": row_filter
            }

            try:
                response = requests.get(
                    url, headers=headers, params=params, timeout=timeout)
                if response.status_code == 200:
                    data = response.json()
                    if not data:
                        break  # Stop if no data is returned
                    records_to_add = data[:max_records - len(all_data)]
                    all_data.extend(records_to_add)
                    pbar.update(len(records_to_add))
                    offset += pagelimit
                else:
                    raise Exception(
                        f"Failed to retrieve data: {response.status_code}")

                # Delay between requests to prevent overwhelming the server
                time.sleep(delay)

            except requests.exceptions.Timeout:
                print(
                    f"Request timed out after {timeout} seconds. Retrying with backoff...")
                time.sleep(delay * 2)  # Exponential backoff
            except requests.exceptions.RequestException as e:
                print(f"An error occurred: {e}")
                break

    df = pd.DataFrame(all_data)
    print(f"Total records fetched: {len(df)}")
    return df


def fetch_data_from_api1(url, api_key_id, api_secret, columns=None, row_filter=None, max_records=None, timeout=10, delay=4):
    """Fetch data from the specified API with selected columns and row filter."""
    headers = {
        "X-Api-Key-Id": api_key_id,
        "X-Api-Secret": api_secret
    }

    # Pagination parameters
    pagelimit = 1000
    offset = 0
    all_data = []

    # Handle unlimited records
    if max_records is None:
        max_records = float('inf')

    not_null_filter = "(case_number IS NOT NULL) AND (primary_type IS NOT NULL) AND (district IS NOT NULL) AND (community_area IS NOT NULL) AND (latitude IS NOT NULL) AND (longitude IS NOT NULL) AND (arrest=true) "
    combined_filter = f"({row_filter}) AND ({not_null_filter})"

    print(f"Requesting data with URL: {url}")
    print(f"Row filter: {row_filter}")
    print(f"Columns: {columns}")

    # Initialize tqdm progress bar
    with tqdm(total=max_records if max_records != float('inf') else None, desc="Fetching records") as pbar:
        while len(all_data) < max_records:
            params = {
                "$limit": pagelimit,
                "$offset": offset,
                "$select": ','.join(columns) if columns else '*',
                "$where": combined_filter
            }

            try:
                response = requests.get(
                    url, headers=headers, params=params, timeout=timeout)
                # print(f"Response status: {response.status_code}")

                if response.status_code == 200:
                    data = response.json()
                    if not data:
                        print("No data returned. Exiting loop.")
                        break
                    records_to_add = data[:max_records - len(all_data)]
                    all_data.extend(records_to_add)
                    pbar.update(len(records_to_add))
                    offset += pagelimit
                else:
                    print(f"Error: {response.status_code} - {response.text}")
                    raise Exception(
                        f"Failed to retrieve data: {response.status_code}")

                time.sleep(delay)

            except requests.exceptions.Timeout:
                print(f"Request timed out. Retrying...")
                time.sleep(delay * 2)
            except requests.exceptions.RequestException as e:
                print(f"An error occurred: {e}")
                traceback.print_exc()
                break

    df = pd.DataFrame(all_data)
    print(f"Total records fetched: {len(df)}")
    return df


def fetch_data_from_api2(url, api_key_id, api_secret, columns=None, row_filter=None, max_records=None, timeout=10, delay=4):
    """Fetch data from the specified API with selected columns and row filter."""
    headers = {
        "X-Api-Key-Id": api_key_id,
        "X-Api-Secret": api_secret
    }

    # Pagination parameters
    pagelimit = 1000
    offset = 0
    all_data = []

    # Handle unlimited records
    if max_records is None:
        max_records = float('inf')

    not_null_filter = "(case_number IS NOT NULL) AND (arrest_date IS NOT NULL) AND ( (race IS NOT NULL) AND (charge_1_description IS NOT NULL) AND (charge_1_type IS NOT NULL) AND (charge_1_class IS NOT NULL) )"
    combined_filter = f"({row_filter}) AND ({not_null_filter})"

    print(f"Requesting data with URL: {url}")
    print(f"Row filter: {row_filter}")
    print(f"Columns: {columns}")

    # Initialize tqdm progress bar
    with tqdm(total=max_records if max_records != float('inf') else None, desc="Fetching records") as pbar:
        while len(all_data) < max_records:
            params = {
                "$limit": pagelimit,
                "$offset": offset,
                "$select": ','.join(columns) if columns else '*',
                "$where": combined_filter
            }

            try:
                response = requests.get(
                    url, headers=headers, params=params, timeout=timeout)
                print(f"Response status: {response.status_code}")

                if response.status_code == 200:
                    data = response.json()
                    if not data:
                        print("No data returned. Exiting loop.")
                        break
                    records_to_add = data[:max_records - len(all_data)]
                    all_data.extend(records_to_add)
                    pbar.update(len(records_to_add))
                    offset += pagelimit
                else:
                    print(f"Error: {response.status_code} - {response.text}")
                    raise Exception(
                        f"Failed to retrieve data: {response.status_code}")

                time.sleep(delay)

            except requests.exceptions.Timeout:
                print(f"Request timed out. Retrying...")
                time.sleep(delay * 2)
            except requests.exceptions.RequestException as e:
                print(f"An error occurred: {e}")
                traceback.print_exc()
                break

    df = pd.DataFrame(all_data)
    print(f"Total records fetched: {len(df)}")
    return df


def fetch_geojson_from_api(url, api_key_id, api_secret, row_filter=None, timeout=10, delay=4):
    """
    Fetch GeoJSON data from the specified API with optional row filters.
    """
    headers = {
        "X-Api-Key-Id": api_key_id,
        "X-Api-Secret": api_secret
    }

    # Query parameters for filtering
    params = {
        "$where": row_filter
    }

    try:
        response = requests.get(url, headers=headers,
                                params=params, timeout=timeout)
        if response.status_code == 200:
            geojson_data = response.json()
            print("GeoJSON data fetched successfully.")
            return geojson_data
        else:
            raise Exception(f"Failed to retrieve data: {response.status_code}")
    except requests.exceptions.Timeout:
        print(
            f"Request timed out after {timeout} seconds. Retrying with backoff...")
        time.sleep(delay * 2)  # Exponential backoff
        return None
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None
