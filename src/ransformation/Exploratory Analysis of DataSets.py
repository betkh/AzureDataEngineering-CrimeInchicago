# Databricks notebook source
# MAGIC %md
# MAGIC ## DataSet-1: Crime incidents in Chicago

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
crimes_df = spark.read.csv(uri+"Crime2019_to_Present/Crimes_2019-01-01_to_2024-11-16_206941_rows.csv", header=True)

# Read the Grades file using defaults and use the top row as header (not the default behavior)
arrests_df = spark.read.csv(uri+"Arrests/Arrests_2019-12-25_to_2024-11-20_200000_rows.csv", header=True)
display(crimes_df)

# COMMAND ----------

display(arrests_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### I. Explore Data Attributes

# COMMAND ----------

num_rows = crimes_df.count()
num_columns = len(crimes_df.columns)

print(f"crimes_df rows: {num_rows} \ncrimes_df columns: {num_columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### define Schema

# COMMAND ----------

from pyspark.sql.types import (StructType, StructField, StringType, 
                               BooleanType, IntegerType, DoubleType, 
                               TimestampType)


print(f"data types before schema definition: {crimes_df.dtypes}")


schema = StructType([
    StructField("case_number", StringType(), True),
    StructField("date", TimestampType(), True),           # Assuming this is a datetime field
    StructField("primary_type", StringType(), True),      # Type of crime
    StructField("description", StringType(), True),       # Description of crime
    StructField("location_description", StringType(), True), # Crime location type
    StructField("arrest", BooleanType(), True),           # Whether an arrest was made
    StructField("district", IntegerType(), True),         # Police district
    StructField("community_area", StringType(), True),   # Community area code
    StructField("latitude", DoubleType(), True),          # Latitude
    StructField("longitude", DoubleType(), True)          # Longitude
])


# Load the CSV file with the schema
df = spark.read.options(delimiter=',', header=True).schema(schema).csv(uri+"Crime2019_to_Present/Crimes_2019-01-01_to_2024-11-16_206941_rows.csv")


print(f"\n\n\ndata types after schema definition: {df.dtypes}")


# Display the DataFrame
display(df)

# COMMAND ----------

# Display schema and count of records
df.printSchema()
print(f"Number of records: {df.count()}")


# COMMAND ----------

# MAGIC %md
# MAGIC #### top crime types

# COMMAND ----------


crime_counts = df.groupBy("primary_type").count().orderBy("count", ascending=False)
display(crime_counts)

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# Set style
sns.set(style="darkgrid")

# Bar Chart Visualization
plt.figure(figsize=(12, 6))
bar_plot = sns.barplot(x='primary_type', y='count', data=crime_counts.limit(10).toPandas(), palette='viridis')

# Add labels on top of the bars
for p in bar_plot.patches:
    bar_plot.annotate(format(p.get_height(), '.0f'),  # Format the count as integer
                      (p.get_x() + p.get_width() / 2., p.get_height()),  # Position at the top of the bar
                      ha='center', va='bottom', fontsize=12, color='black', 
                      xytext=(0, 5),  # Offset the text slightly above the bar
                      textcoords='offset points')

plt.title('Top 10 Crime Types', fontsize=16, pad=30)
plt.xlabel('Crime Type', fontsize=14)
plt.ylabel('Count', fontsize=14)
plt.xticks(rotation=35, ha='right', fontsize=12)
plt.yticks(fontsize=10)
plt.tight_layout()
plt.show()


# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Set the style for the plot
sns.set(style="darkgrid")
plt.figure(figsize=(8, 6))

# Convert the DataFrame to Pandas
crime_data = crime_counts.limit(10).toPandas()

# Explode all wedges
explode = [0.1] * len(crime_data)  # Use the length of the crime_data

# Create a donut chart (pie chart with a hole in the middle)
wedges, texts, autotexts = plt.pie(crime_data['count'],  
                                    explode=explode,  # Apply explode effect
                                    labels=crime_data['primary_type'],  # Use the primary_type column
                                    autopct='%1.1f%%', 
                                    startangle=140, 
                                    colors=sns.color_palette('viridis', 10),
                                    wedgeprops=dict(width=0.3))  # Set width for donut

# Draw a circle at the center of the pie to make it a donut
centre_circle = plt.Circle((0, 0), 0.75, fc='white')
fig = plt.gcf()
fig.gca().add_artist(centre_circle)

# Equal aspect ratio ensures that the pie chart is circular
plt.axis('equal')  

plt.tight_layout()  
plt.title('Top 10 Crime Types Distribution', fontsize=20, pad=40)
plt.show()


# COMMAND ----------

# Count crimes by community area
display(df.groupBy("community_area").count().orderBy("count", ascending=False))

# You might also consider using a map visualization if you have access to geographic libraries.


# COMMAND ----------

# MAGIC %md
# MAGIC ### II. Temporal Analysis of Crime incidents - High Crime Year, Months, Days
# MAGIC
# MAGIC - bar charts
# MAGIC - stacked bar chart
# MAGIC - line graphs ( time series)

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofweek, when

# Extract year, month, and day of the week
df = df.withColumn("year", year(df.date))
df = df.withColumn("month", month(df.date))
df = df.withColumn("day", dayofweek(df.date))  # 1 = Sunday, 2 = Monday, ..., 7 = Saturday


# Map numeric day to day names
df = df.withColumn("day_name", when(df.day == 1, "Sunday")
                                    .when(df.day == 2, "Monday")
                                    .when(df.day == 3, "Tuesday")
                                    .when(df.day == 4, "Wednesday")
                                    .when(df.day == 5, "Thursday")
                                    .when(df.day == 6, "Friday")
                                    .when(df.day == 7, "Saturday")
                                    .otherwise("Unknown"))


display(df.select("date", "year", "month", "day", "day_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Crime incidents over the years

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, count
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder.appName("Crime Data Analysis").getOrCreate()

# Extract year, month, and day
df_ = df.withColumn("year", year(df.date)).withColumn("month", month(df.date)).withColumn("dayNum", dayofmonth(df.date))

# Aggregate counts by year and month for Plot 1
crime_counts = df_.groupBy("year", "month").agg(count("*").alias("incident_count"))
crime_counts_pd = crime_counts.toPandas()
crime_counts_pd['date'] = pd.to_datetime(crime_counts_pd[['year', 'month']].assign(day=1))
crime_counts_pd = crime_counts_pd.sort_values('date')

# Aggregate daily counts for Plot 2 and resample for monthly smoothing
crime_counts_daily = df_.groupBy("year", "month", "day").agg(count("*").alias("incident_count"))
crime_counts_daily_pd = crime_counts_daily.toPandas()
crime_counts_daily_pd['date'] = pd.to_datetime(crime_counts_daily_pd[['year', 'month', 'day']])
crime_counts_daily_pd = crime_counts_daily_pd.sort_values('date')
crime_counts_monthly = (crime_counts_daily_pd.set_index('date').resample('M').sum().reset_index())
crime_counts_monthly['smoothed'] = crime_counts_monthly['incident_count'].rolling(window=3, center=True).mean()

# Set up figure with two stacked subplots
sns.set(style="darkgrid")
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 12))

# Plot 1: Number of Crime Incidents Over the Years
sns.lineplot(x='date', y='incident_count', data=crime_counts_pd, marker='o', color='blue', linewidth=1.5, ax=ax1)
ax1.set_title('Number of Crime Incidents Over the Years', fontsize=20, pad=20)
ax1.set_xlabel('Time in Years', fontsize=14)
ax1.set_ylabel('Number of Incidents', fontsize=14)
ax1.tick_params(axis='x', rotation=45)
ax1.grid(True)

# Plot 2: Monthly Crime Incidents with Log Scale
sns.lineplot(x='date', y='smoothed', data=crime_counts_monthly, marker='o', color='red', linewidth=1.5, ax=ax2)
ax2.set_yscale('log')
ax2.set_title('Monthly Number of Crime Incidents (Log Scale)', fontsize=20, pad=20)
ax2.set_xlabel('Time in Years', fontsize=14)
ax2.set_ylabel('Number of Incidents (Log Scale)', fontsize=14)
ax2.tick_params(axis='x', rotation=45)
ax2.grid(True, which="both", linestyle='--', linewidth=0.5)

plt.tight_layout()

plt.savefig('temporal_analysis_crime_incidents.png', dpi=300, bbox_inches='tight')
plt.show()


# COMMAND ----------

high_crime_days_pd = df_.groupBy("day_name").count().toPandas()

day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

high_crime_days_pd['day_name'] = pd.Categorical(high_crime_days_pd['day_name'], categories=day_order, ordered=True)

high_crime_days_pd2 = high_crime_days_pd.sort_values('day_name')


display(high_crime_days_pd2)

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import matplotlib.cm as cm
import pandas as pd

import warnings

# Suppress warnings
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)


# First subplot: Crimes by Day of the Week
plt.figure(figsize=(12, 12))




norm_counts_days = (high_crime_days_pd2['count'] - high_crime_days_pd2['count'].min()) / \
                   (high_crime_days_pd2['count'].max() - high_crime_days_pd2['count'].min())

cmap = cm.get_cmap('viridis')
colors_days = cmap(norm_counts_days)

# Plot the first bar chart
ax1 = plt.subplot(2, 1, 1)
bars_days = ax1.bar(high_crime_days_pd2['day_name'], high_crime_days_pd2['count'], color=colors_days)
ax1.set_title('5 year Aggregate Crime incidents by Day of the Week', fontsize=18, pad=15)
ax1.set_ylabel('Number of Crimes', fontsize=14)
ax1.grid(axis='y', linestyle='--', alpha=0.7)

for bar in bars_days:
    count_k = f'{int(bar.get_height() / 1000)}K' if bar.get_height() >= 1000 else str(bar.get_height())
    ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 5, count_k, ha='center', va='bottom', fontsize=10)

sm_days = plt.cm.ScalarMappable(cmap=cmap, norm=plt.Normalize(vmin=high_crime_days_pd2['count'].min(), vmax=high_crime_days_pd2['count'].max()))
sm_days.set_array([])
cbar_days = plt.colorbar(sm_days, ax=ax1)
cbar_days.set_label('Magnitude of Crimes', fontsize=14, labelpad=-80)

# Second subplot: Crimes by Month
high_crime_months = df.groupBy("month").count().orderBy("month")
high_crime_months_pd = high_crime_months.toPandas()

month_labels = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
high_crime_months_pd['month'] = high_crime_months_pd['month'].map(month_labels)

norm_counts_months = (high_crime_months_pd['count'] - high_crime_months_pd['count'].min()) / \
                     (high_crime_months_pd['count'].max() - high_crime_months_pd['count'].min())

colors_months = cmap(norm_counts_months)

# Plot the second bar chart
ax2 = plt.subplot(2, 1, 2)
bars_months = ax2.bar(high_crime_months_pd['month'], high_crime_months_pd['count'], color=colors_months)
ax2.set_title('5 year aggragate Crime incidents by Month', fontsize=18, pad=15)

ax2.set_ylabel('Number of Crimes', fontsize=14)
ax2.set_xticklabels(high_crime_months_pd['month'], rotation=45)
ax2.grid(axis='y', linestyle='--', alpha=0.7)

for bar in bars_months:
    count_k = f'{int(bar.get_height() / 1000)}K'
    ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 500, count_k, ha='center', va='bottom', fontsize=11, color='black')

sm_months = plt.cm.ScalarMappable(cmap=cmap, norm=plt.Normalize(vmin=high_crime_months_pd['count'].min(), vmax=high_crime_months_pd['count'].max()))
sm_months.set_array([])
cbar_months = plt.colorbar(sm_months, ax=ax2)
cbar_months.set_label('Magnitude of Crimes', rotation=90, fontsize=14, labelpad=-80)


plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### III. Spatial Analysis of Crime incidents - Community Area, 
# MAGIC
# MAGIC - heatmaps using latituide, longutuide
# MAGIC - bubble plots

# COMMAND ----------

# Count unique values in each of the specified columns
unique_community_areas = df.select("community_area").distinct().count()

print(f"Unique Community Areas: {unique_community_areas}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Aggregate crime incident by District

# COMMAND ----------

# Aggregate crime incidents by district and community area
community_area_counts = df.groupBy("district").count().orderBy("count", ascending=False)

# Convert to Pandas DataFrame for visualization
community_area_counts_pd = community_area_counts.toPandas()

display(community_area_counts_pd)


# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder.appName("Crime Data Analysis").getOrCreate()

# Aggregate crime incidents by district
district_counts = df.groupBy("district").count().orderBy("count", ascending=False)

# Convert to Pandas DataFrame for visualization
district_counts_pd = district_counts.toPandas()

# Display the DataFrame (optional, for notebook environments)
display(district_counts_pd)


# COMMAND ----------

district_counts_pd.to_csv('district_crime_inicdent_count.csv', index=False)

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.cm as cm
import pandas as pd
import warnings

# Suppress warnings
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)


norm_counts = (district_counts_pd['count'] - district_counts_pd['count'].min()) / \
              (district_counts_pd['count'].max() - district_counts_pd['count'].min())

# Set up color map
cmap = cm.get_cmap('viridis')
colors = cmap(norm_counts)

# Plot setup
plt.figure(figsize=(20, 6))
ax = plt.subplot(1, 1, 1)

# Plot vertical bar chart with colors based on normalized counts
bars = ax.bar(district_counts_pd['district'].astype(str), district_counts_pd['count'], color=colors)
ax.set_title('Crime Incidents by District', fontsize=18, pad=15)
ax.set_xlabel('District', fontsize=14)
ax.set_ylabel('Number of Crimes', fontsize=14)
ax.grid(axis='y', linestyle='--', alpha=0.7)

# Label each bar with count in "K" format
for bar in bars:
    count_k = f"{int(bar.get_height() / 1000)}K" if bar.get_height() >= 1000 else str(bar.get_height())
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 500, count_k, ha='center', va='bottom', fontsize=10, color='black')

# Color spectrum legend at the side
sm = plt.cm.ScalarMappable(cmap=cmap, norm=plt.Normalize(vmin=district_counts_pd['count'].min(), vmax=district_counts_pd['count'].max()))
sm.set_array([])
cbar = plt.colorbar(sm, ax=ax, orientation='vertical', pad=0.02)
cbar.set_label('Magnitude of Crimes', fontsize=14)

plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC **Insight**: 
# MAGIC - district 11 is the most dangerous area
# MAGIC - district 30 is the safest with least crime incidents
# MAGIC - district `13`, `21`, `23`, `30` doesn't exist on the cpd WEBSITE
# MAGIC - here are the chicago police districts [website](https://www.chicagopolice.org/police-districts/)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Heat Map of crime incidents in chocago districts
# MAGIC
# MAGIC **Steps to Visualize Crime Heat Map by Police District**
# MAGIC - Load the Crime Data in PySpark and aggregate crime incidents by district.
# MAGIC - Convert the Aggregated Data to a Pandas DataFrame.
# MAGIC - Load the GeoJSON File using GeoPandas.
# MAGIC - Merge Crime Data with GeoJSON data.
# MAGIC - Create a Choropleth Map with Folium to visualize the heat map.
# MAGIC - Save the Map as an HTML file.

# COMMAND ----------

# MAGIC %md
# MAGIC ## DataSet-2: Arrests

# COMMAND ----------

# Read the Grades file using defaults and use the top row as header (not the default behavior)
arrests_df = spark.read.csv(uri+"Arrests/Arrests_2019-12-25_to_2024-11-20_200000_rows.csv", header=True)
 
display(arrests_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### insights about data

# COMMAND ----------

# Get the number of columns
num_columns = len(arrests_df.columns)
print(f"Number of columns: {num_columns}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# create a df for null counts
null_counts = [(col, arrests_df.filter(F.col(col).isNull()).count()) for col in arrests_df.columns]


null_counts_df = spark.createDataFrame(null_counts, ["columnLabel", "NullCounts"])

sorted_null_counts_df = null_counts_df.orderBy("NullCounts", ascending=False)


display(sorted_null_counts_df)

# COMMAND ----------

from pyspark.sql.functions import col

# Aggregate by 'race'
race_counts = arrests_df.groupBy("race").count()


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


# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA DataSet4 - Socio economic Indicators

# COMMAND ----------

indicators_df = spark.read.csv(uri+"Socioeconomic Indicators/socio_econ_indicators-2024-11-24-16:53_78_rows.csv", header=True)
 
display(indicators_df.limit(10))

# COMMAND ----------

# Select only the desired columns
indicators_df = indicators_df.select(
    "community_area_name",
    "percent_households_below_poverty",
    "percent_aged_25_without_high_school_diploma",
    "per_capita_income_",
    "hardship_index"
)

# Display the filtered DataFrame
display(indicators_df)

# COMMAND ----------

# Number of rows and columns
num_rows = indicators_df.count()
num_cols = len(indicators_df.columns)

print(f"Number of rows: {num_rows}")
print(f"Number of columns: {num_cols}")


# COMMAND ----------

# Display the structure and data types of the DataFrame
indicators_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum

# Count null values for each column
null_counts = indicators_df.select([(indicators_df[c].isNull().cast("int")).alias(c) for c in indicators_df.columns]) \
                           .agg(*[_sum(c).alias(c) for c in indicators_df.columns])
display(null_counts)


# COMMAND ----------

numeric_cols = [field.name for field in indicators_df.schema.fields if str(field.dataType) in ('IntegerType', 'DoubleType')]
categorical_cols = [field.name for field in indicators_df.schema.fields if str(field.dataType) == 'StringType']

print(f"Numeric Columns: {numeric_cols}")
print(f"Categorical Columns: {categorical_cols}")

