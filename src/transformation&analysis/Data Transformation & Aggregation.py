# Databricks notebook source
# MAGIC %md
# MAGIC #### Step-1: Setup and import data from azure blob storage

# COMMAND ----------

# storage endpoint 
# https://crimeinchicago.dfs.core.windows.net/       (- remove https://)


container_name = "data-engineering-project"  
storage_end_point = "crimeinchicago.dfs.core.windows.net" 
my_scope = "Data-eng-chg-crime"
my_key = "secret-key-for-crimeinchicago-storage-acct"


# Set Spark context for the storage account and the base URI.
spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key))


# construct the URI using:
#uri = f"abfss://{container_name}@{storage_end_point}/"

uri = "abfss://data-engineering-project@crimeinchicago.dfs.core.windows.net/"

crimes_DF = spark.read.csv(uri+"Crime2019_to_Present/Crimes_2014_present_filtered_new.csv", header=True)
arrests_DF = spark.read.csv(uri+"Arrests/Arrests_2014_present_filtered.csv", header=True)
indicators_DF = spark.read.csv(uri+"Socioeconomic Indicators/socio_econ_indicators-2024-11-24-16:53_78_rows.csv", header=True)


arrests_DF = arrests_DF.select("CASE NUMBER", "ARREST DATE", "RACE", "CHARGE 1 DESCRIPTION", "CHARGE 1 TYPE", "CHARGE 1 CLASS")

# filter columns
indicators_DF = indicators_DF.select("ca",
    "community_area_name",
    "percent_households_below_poverty",
    "percent_aged_25_without_high_school_diploma",
    "per_capita_income_",
    "hardship_index"
)


display(crimes_DF.limit(3))
display(arrests_DF.limit(3))
display(indicators_DF.limit(3))

# COMMAND ----------


# Count the total number of rows in the DataFrame
total_rows_crimes = crimes_DF.count()
total_rows_arrests = arrests_DF.count()
total_rows_indicators = indicators_DF.count()


# Print the result
print(f"Total number of rows in crimes: {total_rows_crimes}")
print(f"Total number of rows in arrests: {total_rows_arrests}")
print(f"Total number of rows in indicators: {total_rows_indicators}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation-1: Rename Columns, Merge DFs

# COMMAND ----------

# MAGIC %md
# MAGIC ### re-name columns 

# COMMAND ----------


# re name columns 
crimes_DF = crimes_DF.withColumnRenamed("Case Number", "case_number")
crimes_DF = crimes_DF.withColumnRenamed("Date", "date")
crimes_DF = crimes_DF.withColumnRenamed("Primary Type", "primary_type")
crimes_DF = crimes_DF.withColumnRenamed("Description", "description")
crimes_DF = crimes_DF.withColumnRenamed("Location Description", "location_description")
crimes_DF = crimes_DF.withColumnRenamed("Arrest", "arrest")
crimes_DF = crimes_DF.withColumnRenamed("District", "district")
crimes_DF = crimes_DF.withColumnRenamed("Community Area", "community_area")



arrests_DF = arrests_DF.withColumnRenamed("CASE NUMBER", "case_number")
arrests_DF = arrests_DF.withColumnRenamed("ARREST DATE", "arrest_date")
arrests_DF = arrests_DF.withColumnRenamed("RACE", "race")
arrests_DF = arrests_DF.withColumnRenamed("CHARGE 1 DESCRIPTION", "charge_description")
arrests_DF = arrests_DF.withColumnRenamed("CHARGE 1 TYPE", "charge_type")
arrests_DF = arrests_DF.withColumnRenamed("CHARGE 1 CLASS", "charge_class")

indicators_DF = indicators_DF.withColumnRenamed("ca", "community_area")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save renamed columns Data to Azure Blob

# COMMAND ----------

# Save files after renaming columns

# uri = "abfss://data-engineering-project@crimeinchicago.dfs.core.windows.net/"
# crimes_DF.coalesce(1).write.option('header', True).mode('overwrite').csv(uri + "Crime2019_to_Present/")
# arrests_DF.coalesce(1).write.option('header', True).mode('overwrite').csv(uri + "Arrests/")
# indicators_DF.coalesce(1).write.option('header', True).mode('overwrite').csv(uri + "Socioeconomic Indicators/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrich / Merge data - from 3 DFs 

# COMMAND ----------

from pyspark.sql.functions import col

# merge crimes with arrests
Crimes_Arrests_DF = crimes_DF.join(arrests_DF, on="case_number", how="inner")


# merge Crimes_Arrests_DF with socio_economicIndicators 

# Cast community_area to Integer in both DataFrames
Crimes_Arrests_DF = Crimes_Arrests_DF.withColumn("community_area", col("community_area").cast("int"))
indicators_DF = indicators_DF.withColumn("community_area", col("community_area").cast("int"))

#join on 'community_area' 
merged_DF = Crimes_Arrests_DF.join(indicators_DF, on="community_area", how="inner")

display(Crimes_Arrests_DF.limit(2))
display(Crimes_Arrests_DF.count())

# COMMAND ----------

print(Crimes_Arrests_DF.dtypes)
print("\n")

print(indicators_DF.dtypes)

# COMMAND ----------

# save merged file to azure blob
# merged_DF.coalesce(1).write.option('header', True).mode('overwrite').csv(uri + "_merged_crimes_arrests/")

# COMMAND ----------

crimes_count = crimes_DF.count()
arrests_count = arrests_DF.count()
merged_crime_arrests_count = Crimes_Arrests_DF.count()
merged_DF_count = merged_DF.count()



print(f"crimes_count: {crimes_count}")
print(f"arrests_count: {arrests_count}")
print(f"merged_crime_arrests: {merged_crime_arrests_count}")
print(f"merged_DF_count: {merged_DF_count}")

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


unique_community_areas = Crimes_Arrests_DF.select("community_area").distinct()

display(unique_community_areas)


# COMMAND ----------

from pyspark.sql import functions as F

# Group by 'district' and calculate felony, misdemeanor, and total counts
cases_by_district = Crimes_Arrests_DF.groupBy("district").agg(
    F.sum(F.when(F.col("charge_type") == "F", 1).otherwise(0)).alias("felony_cases"),  # Felony cases
    F.sum(F.when(F.col("charge_type") == "M", 1).otherwise(0)).alias("misdemeanor_cases"),  # Misdemeanor cases
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
display(sorted_crime_arrest_counts)

# COMMAND ----------

# max number of incidents by case number

crime_incident_counts = Crimes_Arrests_DF.groupBy("case_number").count().withColumnRenamed("count", "crime_incident_counts").orderBy("crime_incident_counts", ascending=False)
display(crime_incident_counts.limit(15))

# COMMAND ----------


# top felony districts
felony_data = Crimes_Arrests_DF.filter(Crimes_Arrests_DF["charge_type"] == "F")


top_felony_districts = felony_data.groupBy("district").count().withColumnRenamed("count", "felony_count").orderBy("felony_count", ascending=False)
display(top_felony_districts)


# COMMAND ----------

# top misdemenor districts
misdemenor_data = Crimes_Arrests_DF.filter(Crimes_Arrests_DF["charge_type"] == "M")


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
