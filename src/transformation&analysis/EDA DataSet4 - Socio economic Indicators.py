# Databricks notebook source
# MAGIC %md
# MAGIC ## EDA DataSet4 - Socio economic Indicators

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


# Read the Grades file using defaults and use the top row as header (not the default behavior)
indicators_df = spark.read.csv(uri+"Socioeconomic Indicators/socio_econ_indicators-2024-11-24-16:53_78_rows.csv", header=True)
 
display(indicators_df)
