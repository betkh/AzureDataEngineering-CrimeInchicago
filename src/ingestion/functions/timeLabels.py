from datetime import datetime
import pandas as pd


def createTimestamp():
    now = datetime.now()
    # Format the timestamp in a readable format
    timeStamp = now.strftime("%Y-%m-%d-%H:%M")
    return timeStamp


def createDayLabel():
    dayLabel = datetime.now().strftime("%A")
    return dayLabel


def create_file_label_from_dates(df, date_column="date", dataSource="Data"):
    """Generate a file label based on the date range in the data."""
    df[date_column] = pd.to_datetime(df[date_column])
    min_date = df[date_column].min().strftime("%Y-%m-%d")
    max_date = df[date_column].max().strftime("%Y-%m-%d")
    max_rows = len(df)
    label = f"{dataSource}_{min_date}_to_{max_date}_{max_rows}_rows.csv"
    return label


def create_file_label_geo(df):

    now = datetime.now()
    timeStamp = now.strftime("%Y-%m-%d-%H:%M")
    max_rows = len(df)

    label = f"socio-econ-areas-{timeStamp}_{max_rows}_rows.csv"
    return label


if __name__ == "__main__":

    #  usage
    print(createTimestamp())  # Example output: "2024-10-30-15:45:23"
    print(createDayLabel())   # Example output: "Wednesday"

    dayLabl = createDayLabel()
    timeStamp = createTimestamp()

    csv_file_name = f"crimes-ingest-Date-{dayLabl}-{timeStamp}"
    print(csv_file_name)
