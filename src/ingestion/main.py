from DataSet3_Socioeconomic import ingest_socioecon_areas_data
from DataSet2_Arrests import ingest_Arrests_Data
from DataSet1_Crimes import ingest_crimes_data


def start(delay_=1.5,
          time_Out_=10,
          max_rec_=100):

    ingest_crimes_data(END_POINT="ijzp-q8t2.json",
                       MAX_RECORDS=max_rec_,
                       DELAY=delay_,
                       TIME_OUT=time_Out_,
                       SAVE_PATH='RawData/DataSet1')

    ingest_Arrests_Data(END_POINT="dpt3-jri9.json",
                        MAX_RECORDS=max_rec_,
                        DELAY=delay_,
                        TIME_OUT=time_Out_,
                        SAVE_PATH='RawData/DataSet2')

    ingest_socioecon_areas_data(END_POINT="2ui7-wiq8.json",
                                MAX_RECORDS=max_rec_,
                                TIME_OUT=time_Out_,
                                SAVE_PATH='RawData/DataSet3')


if __name__ == "__main__":

    start(delay_=1.5, time_Out_=15, max_rec_=2000)
