from DataSet1_Crimes import ingest_crimes_data
from DataSet2_Arrests import ingest_Arrests_Data
from DataSet3_Socioeconomic import ingest_socioecon_areas_data
from DataSet4_socioecon_Indicators import ingest_socioecon_indicators_data


def start(max_rec_=1000):

    ingest_crimes_data(END_POINT="ijzp-q8t2.json",
                       MAX_RECORDS=max_rec_,
                       SAVE_PATH='RawData/DataSet1')

    ingest_Arrests_Data(END_POINT="dpt3-jri9.json",
                        MAX_RECORDS=max_rec_,
                        SAVE_PATH='RawData/DataSet2')

    ingest_socioecon_areas_data(END_POINT="2ui7-wiq8.json",
                                MAX_RECORDS=max_rec_,
                                SAVE_PATH='RawData/DataSet3')

    ingest_socioecon_indicators_data(END_POINT="kn9c-c2s2.json",
                                     MAX_RECORDS=max_rec_,
                                     SAVE_PATH='RawData/DataSet3'
                                     )


if __name__ == "__main__":

    start(max_rec_=100)
