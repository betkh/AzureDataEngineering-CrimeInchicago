import requests
import pandas as pd
from tqdm import tqdm


# how to query more than 1000 rows ? https://support.socrata.com/hc/en-us/articles/202949268-How-to-query-more-than-1000-rows-of-a-dataset
# API doc https://dev.socrata.com/consumers/getting-started


def fetch_data_from_api(url, api_key_id, api_secret, columns=None, row_filter=None, max_records=100000):
    """Fetch data from the specified API with selected columns and row filter."""
    headers = {
        "X-Api-Key-Id": api_key_id,
        "X-Api-Secret": api_secret
    }

    # Pagination parameters
    limit = 1000
    offset = 0
    all_data = []

    # Initialize tqdm progress bar
    with tqdm(total=max_records, desc="Fetching records") as pbar:
        while len(all_data) < max_records:
            # Request parameters, including filters and selected columns
            params = {
                "$limit": limit,
                "$offset": offset,
                "$select": ','.join(columns) if columns else '*',
                "$where": row_filter
            }

            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                if not data:
                    break  # Stop if no data is returned
                records_to_add = data[:max_records - len(all_data)]
                all_data.extend(records_to_add)
                pbar.update(len(records_to_add))
                offset += limit
            else:
                raise Exception(
                    f"Failed to retrieve data: {response.status_code}")

    df = pd.DataFrame(all_data)
    print(f"Total records fetched: {len(df)}")
    return df
