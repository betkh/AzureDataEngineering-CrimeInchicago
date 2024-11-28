from DataSet1_Crimes import ingest_crimes_data
from DataSet2_Arrests import ingest_Arrests_Data


"""
    run main.py:
        - to ingest from both data sources - crimes and arrests in one go
        - to only get the recent data for daynamic data sets

    run DataSet1_Crimes.py & DataSet2_Arrests.py:
        - to ingest from each data source, separately 

"""

if __name__ == "__main__":

    data1_rowFilter = [("2024-11-27T14:00:00", "2024-11-27T14:00:00", 60000),]

    #  --------- ingest Dataset-1 (crimes) - Dynamic  -------------------------------------------
    for start_date, end_date, max_records in data1_rowFilter:

        # apply filter
        rowFilter = f"(date>='{start_date}' AND date<='{end_date}')"
        # ingest
        ingest_crimes_data(ROW_FILTER=rowFilter, MAX_RECORDS=max_records)

    #  --------- ingest Dataset-2 (arrests) - Dynamic -------------------------------------------
    data2_rowFilter = [("2024-11-27T14:00:00", "2024-12-31T14:23:40", 60000),]
    for start_date, end_date, max_records in data2_rowFilter:

        # apply filter
        rowFilter = f"(arrest_date>='{start_date}' AND arrest_date<='{end_date}')"
        # ingest
        ingest_Arrests_Data(ROW_FILTER=rowFilter, MAX_RECORDS=max_records)
