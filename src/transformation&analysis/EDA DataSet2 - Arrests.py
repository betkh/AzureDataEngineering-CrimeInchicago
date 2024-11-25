# Databricks notebook source
# MAGIC %md
# MAGIC ## DataSet-2: Arrests

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
arrests_df = spark.read.csv(uri+"Arrests/Arrests_2019-11-11_to_2024-10-25_205000_rows.csv", header=True)
 
display(arrests_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### insights about data

# COMMAND ----------

# Get the number of columns
num_columns = len(arrests_df.columns)
print(f"Number of columns: {num_columns}")

# COMMAND ----------


# null values
from pyspark.sql import functions as F

null_counts_df = arrests_df.select(
    [F.count(F.when(F.col(column).isNull(), column)).alias(column) for column in arrests_df.columns]
)

# Collect null counts 
null_counts = null_counts_df.first()

for column, null_count in zip(arrests_df.columns, null_counts):
    print(f"Null values in Column {column:<25} -> {null_count}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# create a df for null counts
null_counts = [(col, arrests_df.filter(F.col(col).isNull()).count()) for col in arrests_df.columns]


null_counts_df = spark.createDataFrame(null_counts, ["columnLabel", "NullCounts"])

sorted_null_counts_df = null_counts_df.orderBy("NullCounts", ascending=False)


display(sorted_null_counts_df)

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import numpy as np


sns.set_style("darkgrid")


sorted_null_counts_pd = sorted_null_counts_df.toPandas()


plt.figure(figsize=(20, 6))

# colormap
cmap = plt.get_cmap("viridis", len(sorted_null_counts_pd))


bars = plt.bar(sorted_null_counts_pd['columnLabel'], sorted_null_counts_pd['NullCounts'], color=cmap(np.arange(len(sorted_null_counts_pd))))

# Add titles and labels
plt.title('Null Counts by Column', fontsize=16)
plt.xlabel('Column Labels', fontsize=14)
plt.ylabel('Null Counts', fontsize=14)
plt.xticks(rotation=90, ha='right')

# Add count labels on top of each bar
for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, yval, int(yval), va='bottom', ha='center')


plt.grid(axis='y', linestyle='--')
plt.tight_layout()
plt.show()


# COMMAND ----------

# Aggregate by 'race'
race_counts = arrests_df.groupBy("race").count()
display(race_counts.select)

# COMMAND ----------

from pyspark.sql.functions import col

top_4_races_df = race_counts.orderBy(col("count").desc()).limit(4)

display(top_4_races_df)

# COMMAND ----------

from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import matplotlib.cm as cm

race_counts_pd = top_4_races_df.toPandas()

sns.set(style="darkgrid") 


color_map = cm.get_cmap("viridis", len(race_counts_pd))


plt.figure(figsize=(15, 5))

colors = [color_map(i) for i in range(len(race_counts_pd))]  # Get colors from the color map


plt.bar(race_counts_pd['race'], race_counts_pd['count'], color=colors)
plt.xlabel('Race')
plt.ylabel('Count')
plt.title('Count of Arrests by Race')
plt.xticks(rotation=0)  
plt.tight_layout() 

plt.show()

# COMMAND ----------

from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import matplotlib.cm as cm

charge_counts = arrests_df.groupBy("charge_1_type").count()
charge_counts_pd = charge_counts.toPandas()

top_3_chargeCounts = charge_counts_pd.sort_values(by='count', ascending=False).head(3)
display(top_3_chargeCounts)

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.cm as cm


top_3_chargeCounts['charge_1_type'] = top_3_chargeCounts['charge_1_type'].replace({'F': 'Felony', 'M': 'Misdemeanor', None: 'Uncategorized'})


sns.set(style="darkgrid")
color_map = cm.get_cmap("viridis", len(top_3_chargeCounts))

# Create figure
plt.figure(figsize=(12, 7))
colors = [color_map(i) for i in range(len(top_3_chargeCounts))]
bars = plt.bar(top_3_chargeCounts['charge_1_type'], top_3_chargeCounts['count'], color=colors)

# Add labels 
for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width() / 2, height, f'{int(height)}', ha='center', va='bottom')



# Label axes and title
plt.xlabel('Charge Type')
plt.ylabel('Count')
plt.title('Count of Arrests by Charge Type')
plt.xticks(rotation=0, ha='right')
plt.tight_layout()
plt.show()


# COMMAND ----------

charge_class_counts = arrests_df.groupBy("charge_1_class").count()
charge_class_counts_pd = charge_class_counts.toPandas()


top_chargeClassCount = charge_class_counts_pd.sort_values(by='count', ascending=False).head(10)

display(top_chargeClassCount)

# COMMAND ----------



# replace labels
charge_counts = arrests_df.groupBy("charge_1_class").count()
charge_counts_pd = charge_counts.toPandas()

label_mapping = {
    'Z': 'Warrant Arrests',
    'U': 'Business Offense',
    'P': 'Petty Offense',
    'L': 'Local Ordinance',
    'M': 'Misdemeanor',
    'F': 'Felony',
    'A': 'Charge Class A',
    'B': 'Charge Class B',
    'C': 'Charge Class C',
    'X': 'Charge Class X',
    '1': 'Charge Class 1',
    '2': 'Charge Class 2',
    '3': 'Charge Class 3',
    '4': 'Charge Class 4'
}

top_chargeClassCount['charge_1_class'] = top_chargeClassCount['charge_1_class'].replace(label_mapping)

display(top_chargeClassCount)


# COMMAND ----------

sns.set(style="darkgrid")
color_map = cm.get_cmap("viridis", len(top_chargeClassCount))

plt.figure(figsize=(12, 7))
colors = [color_map(i) for i in range(len(charge_counts_pd))]
bars = plt.bar(top_chargeClassCount['charge_1_class'], top_chargeClassCount['count'], color=colors)

plt.xlabel('Charge Class')
plt.ylabel('Count')
plt.title('Count of Arrests by Charge Class')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()


# COMMAND ----------

from pyspark.sql.functions import month, year

arrests_df_month_year = arrests_df.withColumn("arrest_month", month("arrest_date"))
arrests_df_month_year = arrests_df_month_year.withColumn("arrest_year", year("arrest_date"))

arrests_df_month_year

display(arrests_df_month_year.select("arrest_date", "arrest_month", "arrest_year", "charge_1_type"))


# COMMAND ----------

from pyspark.sql.functions import date_format

# Add a new column 'year_month' with the format 'YYYY-MM'
arrests_df = arrests_df.withColumn("year_month", date_format("arrest_date", "yyyy-MM"))

# Aggregate and count the number of arrests by 'year_month'
monthly_arrest_counts = arrests_df.groupBy("year_month").count().orderBy("year_month")

display(monthly_arrest_counts)

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np


monthly_arrest_counts_pd = monthly_arrest_counts.toPandas()

# Apply log transformation to smooth the plot
monthly_arrest_counts_pd['log_count'] = np.log1p(monthly_arrest_counts_pd['count'])

# figure size
plt.figure(figsize=(20, 6))  
plt.plot(monthly_arrest_counts_pd['year_month'], monthly_arrest_counts_pd['log_count'], marker='o', color='blue', linewidth=2)

# Labels and title
plt.title('Temporal Analysis - Monthly Arrest Patterns Over 5 Years (Log-scaled)', fontsize=16)
plt.xlabel('Year-Month', fontsize=14)
plt.ylabel('Log of Number of Arrests', fontsize=14)

# 
plt.xticks(rotation=90)
plt.grid(True)

plt.tight_layout()
plt.show()

