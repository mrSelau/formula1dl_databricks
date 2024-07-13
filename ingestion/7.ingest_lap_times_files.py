# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest lap_times.csv files

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the csv files using the spark dataframe reader

# COMMAND ----------

# Storage description
storage_account_name = "formula1dlld"
# Origen
origen_blob = "raw"
# Destiny
destiny_blob = "processed"
# Files
file_name = "lap_times"
folder_name = "lap_times"
file_type = "csv"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

# Define Schema
lap_times_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time",  StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

# Read data
lap_times_df = spark.read.csv(
    f"/mnt/{storage_account_name}/{origen_blob}/{folder_name}",
    schema = lap_times_schema
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename and add columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

lap_times_final_df = lap_times_df \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write data to datalake as parquet

# COMMAND ----------

lap_times_final_df.write.parquet(f"/mnt/{storage_account_name}/{destiny_blob}/{file_name}", mode="overwrite")
