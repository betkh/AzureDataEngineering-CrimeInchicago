# Databricks notebook source
# MAGIC %md
# MAGIC #### Step-1: Setup and import data from azure blob storage

# COMMAND ----------

# storage endpoint 
# https://crimeinchicago.dfs.core.windows.net/       (- remove https://)


container_name = "input-ingested-raw"  
storage_end_point = "crimeinchicago.dfs.core.windows.net" 
my_scope = "Data-eng-chg-crime"
my_key = "secret-key-for-crimeinchicago-storage-acct"


# Set Spark context for the storage account and the base URI.
spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key))


# construct the URI using:
uri = f"abfss://{container_name}@{storage_end_point}/"

# uri = "abfss://data-engineering-project@crimeinchicago.dfs.core.windows.net/"



# DataSource1 - Crime Incidents 
crimes_2019 = spark.read.csv(uri+"Crime2019_to_Present/Crimes_2019-01-01_to_2019-12-31_56106_rows.csv", header=True)
crimes_2020 = spark.read.csv(uri+"Crime2019_to_Present/Crimes_2020-01-01_to_2020-12-31_33808_rows.csv", header=True)
crimes_2021 = spark.read.csv(uri+"Crime2019_to_Present/Crimes_2021-01-01_to_2021-12-31_25095_rows.csv", header=True)
crimes_2022 = spark.read.csv(uri+"Crime2019_to_Present/Crimes_2022-01-01_to_2022-12-31_27248_rows.csv", header=True)
crimes_2023 = spark.read.csv(uri+"Crime2019_to_Present/Crimes_2023-01-01_to_2023-12-31_31560_rows.csv", header=True)
crimes_2024 = spark.read.csv(uri+"Crime2019_to_Present/Crimes_2024-01-01_to_2024-11-19_30397_rows.csv", header=True)

# DataSource2 - Arrests 
arrests_2019 = spark.read.csv(uri+"Arrests/Arrests_2019-01-01_to_2019-12-31_50753_rows.csv", header=True)
arrests_2020 = spark.read.csv(uri+"Arrests/Arrests_2020-01-01_to_2020-12-31_31354_rows.csv", header=True)
arrests_2021 = spark.read.csv(uri+"Arrests/Arrests_2021-01-01_to_2021-12-31_24422_rows.csv", header=True)
arrests_2022 = spark.read.csv(uri+"Arrests/Arrests_2022-01-01_to_2022-12-31_25177_rows.csv", header=True)
arrests_2023 = spark.read.csv(uri+"Arrests/Arrests_2023-01-01_to_2023-12-31_28990_rows.csv", header=True)
arrests_2024 = spark.read.csv(uri+"Arrests/Arrests_2024-01-01_to_2024-11-23_29945_rows.csv", header=True)

# # DataSource3 - Socio economic Indicators 
indicators_DF = spark.read.csv(uri+"Socioeconomic_Indicators/socio_econ_indicators-2024-11-27-15:22_78_rows.csv", header=True)

# # DataSource4 - economially disadvantaged areas
# disadvantagedAreas_DF = spark.read.format("geojson").load(uri+"Disadvanatged_Areas/socioecon_disadvantaged_Areas.geojson")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Transformation-1:  Unify Data 

# COMMAND ----------

from pyspark.sql import DataFrame
def merge_dataframes(dataframes: list) -> DataFrame:
    
    # Start with the first df
    merged_df = dataframes[0]
    
    # Loop through the rest of the DataFrames and perform union
    for df in dataframes[1:]:
        merged_df = merged_df.union(df)
    
    return merged_df



# List of all DataFrames
crime_dataframes = [crimes_2019, crimes_2020, crimes_2021, crimes_2022, crimes_2023, crimes_2024]
arrest_dataframes = [arrests_2019, arrests_2020, arrests_2021, arrests_2022, arrests_2023, arrests_2024]

# Merge all crime data into a single DataFrame
merged_crime_DF = merge_dataframes(crime_dataframes)
merged_arrest_DF = merge_dataframes(arrest_dataframes)


# rename column in indicators_DF so that it will be merged with merged_crime_DF later
indicators_DF = indicators_DF.withColumnRenamed("ca", "community_area")


display(merged_crime_DF.limit(3))
display(merged_arrest_DF.limit(3))
display(indicators_DF.limit(3))

# COMMAND ----------


# Count the total number of rows in the DataFrame
total_rows_crimes = merged_crime_DF.count()
total_rows_arrests = merged_arrest_DF.count()
total_rows_indicators = indicators_DF.count()


# Print the result
print(f"Total number of rows in crimes: {total_rows_crimes}")
print(f"Total number of rows in arrests: {total_rows_arrests}")
print(f"Total number of rows in indicators: {total_rows_indicators}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save unified files into a new container
# MAGIC
# MAGIC - create a container for transformed data - `transformed-data`
# MAGIC - created _unifiedData_Crimes and `_unifiedData_Arrests` dircetories manually
# MAGIC - Save files after unifying them 
# MAGIC - finally rename auto generated file names
# MAGIC - comment out saving code to avoid re-writing the data

# COMMAND ----------

# create a container for transformed data - transformed-data
# created _unifiedData_Crimes and _unifiedData_Arrests dircetories manually
# Save files after unifying them - then rename files 

container_name = "transformed-data"
storage_end_point = "crimeinchicago.dfs.core.windows.net" 

uri = f"abfss://{container_name}@{storage_end_point}/"

# merged_crime_DF.coalesce(1).write.option('header', True).mode('overwrite').csv(uri + "_unifiedData_Crimes/")
# merged_arrest_DF.coalesce(1).write.option('header', True).mode('overwrite').csv(uri + "_unifiedData_Arrests/")
# indicators_DF.coalesce(1).write.option('header', True).mode('overwrite').csv(uri + "_renamed_Indicators/")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation-2: Merge Data from different sources (Enrichement)
# MAGIC
# MAGIC - read unified data for crimes and arrests
# MAGIC - merge crimes with arrests
# MAGIC - merge the result with socio eocnomic indicators

# COMMAND ----------

container_name = "transformed-data"
storage_end_point = "crimeinchicago.dfs.core.windows.net" 

uri = f"abfss://{container_name}@{storage_end_point}/"

# read transformed data from azure  
crimes_DF = spark.read.csv(uri+"_unifiedData_Crimes/merged_crime_DF.csv", header=True)
arrests_DF = spark.read.csv(uri+"_unifiedData_Arrests/merged_arrest_DF.csv", header=True)
indicators_DF = spark.read.csv(uri+"_renamed_Indicators/indicators_renamed_DF.csv", header=True)


# merge DataSet1 and DataSet2 - (crimes with arrests)
Crimes_Arrests_DF = crimes_DF.join(arrests_DF, on="case_number", how="inner")


# merge Crimes_Arrests_DF with socio_economicIndicators 
from pyspark.sql.functions import col


# Cast community_area to Integer in both DataFrames
Crimes_Arrests_DF = Crimes_Arrests_DF.withColumn("community_area", col("community_area").cast("int"))
indicators_DF = indicators_DF.withColumn("community_area", col("community_area").cast("int"))

#join on 'community_area' 
merged_DF = Crimes_Arrests_DF.join(indicators_DF, on="community_area", how="left")

# COMMAND ----------

# Print the number of columns in the DataFrame
merged_columns_count = len(merged_DF.columns)
crime_columns_count = len(merged_crime_DF.columns)
arrests_columns_count = len(merged_arrest_DF.columns)
indicators_columns_count = len(indicators_DF.columns)


print(f"Number of columns in crimes data: {crime_columns_count}")
print(f"Number of columns in arrests data: {arrests_columns_count}")
print(f"Number of columns in indicators data: {indicators_columns_count}")
print(f"Number of columns in merged data: {merged_columns_count}")

count_rows_crimeArrests = Crimes_Arrests_DF.count()
count_rows_MergedDF = merged_DF.count()

print(f"\ncount_rows_crimeArrests : {count_rows_crimeArrests}")
print(f"count_rows_MergedDF : {count_rows_MergedDF}")
display(merged_DF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### save the merged data 
# MAGIC
# MAGIC - save merged data into `transofrmed datasets` in a dir `_MERGED_ALL`
# MAGIC - then rename it as `merged_DF.csv`
# MAGIC - then comment out to avoid oevrwriting

# COMMAND ----------

# save merged file to azure blob
# merged_DF.coalesce(1).write.option('header', True).mode('overwrite').csv(uri + "_MERGED_ALL/")

# COMMAND ----------

display(merged_DF.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation - II - aggregations 
# MAGIC
# MAGIC - null values avoided at api level
# MAGIC - column data types wil be handled within power bi

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregations
# MAGIC
# MAGIC
# MAGIC ##### crimes 
# MAGIC - crime counts by district: count rows and aggregate by district
# MAGIC
# MAGIC
# MAGIC ##### arrests
# MAGIC - arrest counts by district: count rows with arrest = true and aggregate by district
# MAGIC - top felony districts: count all cases of felony and aggregete by district 
# MAGIC
# MAGIC ##### indicators 
# MAGIC - avg hardship idex by district: avg rows for hardship index and aggregate by district
# MAGIC - count of population with age >25 without highschool diploma by district 
# MAGIC - avg percapita income by district: avg percapita and aggregate by district 
# MAGIC - avg percentage of poverty by district: avg percentage of poverty and aggregate by distcrict 
# MAGIC
# MAGIC ##### categorical
# MAGIC
# MAGIC - most convicted by race: count all crime incidents and aggregate by race
# MAGIC - most common crime type: count crime types and aggregete by type
# MAGIC - most common charge type: count all crimes and ggregate by charge type (F/M)
# MAGIC
# MAGIC ##### temporal
# MAGIC - years with most crime: count all unique cases for each year

# COMMAND ----------

display(merged_DF.limit(4))

# COMMAND ----------

print(merged_DF.dtypes)

# COMMAND ----------

from pyspark.sql import functions as F

# Group by 'district' and calculate felony, misdemeanor, and total counts
cases_by_district = Crimes_Arrests_DF.groupBy("district").agg(
    F.sum(F.when(F.col("charge_1_type") == "F", 1).otherwise(0)).alias("felony_cases"),  # Felony cases
    F.sum(F.when(F.col("charge_1_type") == "M", 1).otherwise(0)).alias("misdemeanor_cases"),  # Misdemeanor cases
    F.count("*").alias("total_cases")  # Total cases per district
)


sorted_cases_by_district = cases_by_district.orderBy(F.col("total_cases").desc())
display(sorted_cases_by_district)

# COMMAND ----------


from pyspark.sql import functions as F

# Cast 'arrest' to boolean type (if it's stored as a string)
Crimes_Arrests_DF = Crimes_Arrests_DF.withColumn("arrest", F.col("arrest").cast("boolean"))

# Group by 'district' and count total crimes and arrests
crime_arrest_counts = Crimes_Arrests_DF.groupBy("district").agg(
    F.count("*").alias("total_crimes"),  # Total crimes per district
    F.sum(F.when(F.col("arrest") == True, 1).otherwise(0)).alias("arrest_count"),  # Arrests per district
    F.sum(F.when(F.col("arrest") == False, 1).otherwise(0)).alias("non_arrest_count")  # Non-arrests per district
)


sorted_crime_arrest_counts = crime_arrest_counts.orderBy(F.col("total_crimes").desc())


display(sorted_crime_arrest_counts.select("district", "arrest_count"))

# COMMAND ----------

# max number of incidents by case number

crime_incident_counts = Crimes_Arrests_DF.groupBy("case_number").count().withColumnRenamed("count", "crime_incident_counts").orderBy("crime_incident_counts", ascending=False)
display(crime_incident_counts.limit(15))

# COMMAND ----------


# top felony districts
felony_data = Crimes_Arrests_DF.filter(Crimes_Arrests_DF["charge_1_type"] == "F")


top_felony_districts = felony_data.groupBy("district").count().withColumnRenamed("count", "felony_count").orderBy("felony_count", ascending=False)
display(top_felony_districts)


# COMMAND ----------

# top misdemenor districts
misdemenor_data = Crimes_Arrests_DF.filter(Crimes_Arrests_DF["charge_1_type"] == "M")


top_misdemenor_districts = misdemenor_data.groupBy("district").count().withColumnRenamed("count", "midemenor_count").orderBy("midemenor_count", ascending=False)
display(top_misdemenor_districts)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Socio economic Indicators

# COMMAND ----------

# MAGIC %md
# MAGIC ##### hardships in districts

# COMMAND ----------

# averge hardship index by district


from pyspark.sql.functions import col, avg

# cast 
merged_DF = merged_DF.withColumn("hardship_index", col("hardship_index").cast("double"))

#Group by district 
avg_hardship_by_district = merged_DF.groupBy("district").agg(
    avg("hardship_index").alias("avg_hardship_index")
)

display(avg_hardship_by_district)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### top hardship districts

# COMMAND ----------

from pyspark.sql.functions import col, max

# Group by district and calculate the max hardship_index
top_10_hardship_districts = merged_DF.groupBy("district").agg(
    max("hardship_index").alias("max_hardship_index")
)


top_10_hardship_districts = top_10_hardship_districts.orderBy(col("max_hardship_index").desc()).limit(10)
display(top_10_hardship_districts)

# COMMAND ----------

print(merged_DF.dtypes)

# COMMAND ----------

# MAGIC %md
# MAGIC #### avg percent of adults with highschood delpoma in a district

# COMMAND ----------

from pyspark.sql.functions import col

# cast 'percent_aged_25_without_high_school_diploma' as float type 
merged_DF = merged_DF.withColumn("percent_aged_25_without_high_school_diploma", col("percent_aged_25_without_high_school_diploma").cast("float"))

# Aggregate by district and calculate average
avg_percent_by_district = merged_DF.groupBy("district").agg(
    {"percent_aged_25_without_high_school_diploma": "avg"}
)

# Rename the column 
avg_percent_by_district = avg_percent_by_district.withColumnRenamed("avg(percent_aged_25_without_high_school_diploma)", "avg_percent_aged_25_without_high_school_diploma")


display(avg_percent_by_district)


# COMMAND ----------

# MAGIC %md
# MAGIC #### avergae percapita in a district

# COMMAND ----------

from pyspark.sql.functions import col

# cast 'per_capita_income_' to float
merged_DF = merged_DF.withColumn("per_capita_income_", col("per_capita_income_").cast("float"))

# Aggregate by district and calculate average
avg_percapita_by_district = merged_DF.groupBy("district").agg(
    {"per_capita_income_": "avg"}
)


avg_percapita_by_district = avg_percapita_by_district.withColumnRenamed("avg(per_capita_income_)", "avg_per_capita_income_")


display(avg_percapita_by_district)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Categorical Analysis 
# MAGIC
# MAGIC #### most common crime 

# COMMAND ----------

# agg by primary type

from pyspark.sql import functions as F

# Aggregate by primary_type to count occurrences
count_by_primary_type = Crimes_Arrests_DF.groupBy("primary_type").agg(
    F.count("case_number").alias("crime_count")  # Counting the occurrences of case_number (or any other column)
)

sorted_count_by_primary_type = count_by_primary_type.orderBy(F.col("crime_count").desc())
display(sorted_count_by_primary_type.limit(15))


# COMMAND ----------

# MAGIC %md
# MAGIC #### most common crime place

# COMMAND ----------

# agg by location type

from pyspark.sql import functions as F

# Aggregate by location_description to count occurrences
count_by_location = Crimes_Arrests_DF.groupBy("location_description").agg(
    F.count("case_number").alias("crime_count")  # Counting the occurrences of case_number (or any other column)
)


sorted_count_by_location = count_by_location.orderBy(F.col("crime_count").desc())
display(sorted_count_by_location.limit(15))

# COMMAND ----------

# MAGIC %md
# MAGIC ### crime and racial group

# COMMAND ----------

from pyspark.sql.functions import col, count

# Group by race and count the number of incidents for each race
crime_count_by_race = merged_DF.groupBy("race").agg(
    count("case_number").alias("crime_count")
)

# Sort 
crime_count_by_race_sorted = crime_count_by_race.orderBy(col("crime_count").desc())
display(crime_count_by_race_sorted)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Save all of these Aggregated datasets
# MAGIC
# MAGIC - save all aggregated datasets in `_AGGREGATED_DATA` dir within `transformed-data`
# MAGIC - two ways to save:
# MAGIC   - manually download and upload ( preferred for this project)
# MAGIC   - save programatically ( generates lots of files)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregations
# MAGIC
# MAGIC
# MAGIC ##### crimes & arrests
# MAGIC - felony count by district `[done]`
# MAGIC - misdemenr count by district `[done]`
# MAGIC - crime/arrrest/non-arrest counts by district: count rows and aggregate by district `[done]`
# MAGIC - frlony/midemenor/total - `[done]`
# MAGIC
# MAGIC
# MAGIC ##### indicators 
# MAGIC - avg hardship idex by district: avg rows for hardship index and aggregate by district `[done]`
# MAGIC - top 10 hardship districts (max). `[done]`
# MAGIC - count of population with age >25 without highschool diploma by district `[done]`
# MAGIC - avg percapita income by district: avg percapita and aggregate by district `[done]`
# MAGIC
# MAGIC
# MAGIC ##### categorical
# MAGIC
# MAGIC - most convicted by race: count all crime incidents and aggregate by race `[done]`
# MAGIC - most common crime type: count crime types and aggregete by type `[done]`
# MAGIC - most common crime place `[done]`
# MAGIC
# MAGIC
# MAGIC ##### temporal
# MAGIC - years with most crime: count all unique cases for each year
