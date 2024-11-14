# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## IDS706-11-Databricks
# MAGIC
# MAGIC Author: Haobo Yuan
# MAGIC
# MAGIC This is IDS-706 week 11 project which involves Creating a data pipeline using Databricks, and Include at least one data source and one data sink.
# MAGIC
# MAGIC This notebook show how to create and query a table or DataFrame that uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows user to store data for querying inside of Databricks. 
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/NASDAQ_100_Data_From_2010.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = "\t"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

# permanent_table_name = "NASDAQ_100_Data_From_2010_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

from pyspark.sql.functions import col, year, mean, stddev, expr
from datetime import datetime

# Step 2: Load Data (Data Source)
def preprocess_data(file_path):
  # stock = spark.read.csv(file_path, sep="\t", header=True, inferSchema=True)
  stock_AAPL = df.filter(col("Name") == "AAPL")
  stock_AAPL = stock_AAPL.withColumn("Date", expr("to_date(Date, 'yyyy-MM-dd')"))
  stock_AAPL = stock_AAPL.withColumn("Year", year("Date"))
  return stock_AAPL

# COMMAND ----------

# Plotting function for statistics
def generate_plot(yearly_stats):
  # Convert Spark DataFrame to Pandas DataFrame for plotting
  yearly_stats_pd = yearly_stats.toPandas()
  
  # Extract the data for plotting
  years = yearly_stats_pd["Year"].values
  means = yearly_stats_pd["mean"].values
  medians = yearly_stats_pd["median"].values
  stds = yearly_stats_pd["std"].values
  
  # Plot the statistics
  plt.figure(figsize=(15, 6))
  plt.plot(years, means, label="Mean", marker="o")
  plt.plot(years, medians, label="Median", marker="x")
  plt.plot(years, stds, label="Standard Deviation", marker="s")
  plt.grid(True)
  plt.title("AAPL Close Price Statistics (2010-2021)")
  plt.xlabel("Year")
  plt.ylabel("Price")
  plt.legend()

  # Add timestamp
  timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  plt.text(0.95, 0.01, f"Generated on: {timestamp}", 
            verticalalignment='bottom', horizontalalignment='right', 
            transform=plt.gca().transAxes, 
            color='gray', fontsize=8)
  
  # plt.savefig("pictures/plot.png")


# COMMAND ----------

from pyspark.sql.functions import mean, stddev, expr

# Load and preprocess data
file_path = "/FileStore/tables/NASDAQ_100_Data_From_2010.csv"
stock_AAPL = preprocess_data(file_path)

# Calculate descriptive statistics by grouping by year
yearly_stats = stock_AAPL.groupBy("Year").agg(
    mean("Close").alias("mean"),                   # Mean of 'Close' price per year
    expr("percentile_approx(Close, 0.5)").alias("median"),  # Approximate median of 'Close' price
    stddev("Close").alias("std")                   # Standard deviation of 'Close' price
).orderBy("Year")                                  # Ensure data is sorted by 'Year'

# Show the results
yearly_stats.show()
# Data Sink: Save yearly statistics to a table
yearly_stats.write.mode("overwrite").saveAsTable("AAPL_Yearly_Stats")
# Save as CSV in the DBFS (Databricks File System)
yearly_stats.write.csv("/FileStore/tables/AAPL_Yearly_Stats.csv", header=True, mode="overwrite")


# COMMAND ----------

import matplotlib.pyplot as plt

# Generate visualization
generate_plot(yearly_stats)
