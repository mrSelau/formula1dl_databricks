# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest lap_times.csv files

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/commom_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the csv files using the spark dataframe reader

# COMMAND ----------

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
    f"{raw_folder_path}/{folder_name}",
    schema = lap_times_schema
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename and add columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

lap_times_renamed_df = lap_times_df \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

pit_stops_final_df = add_ingestion_date(lap_times_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write data to datalake as parquet

# COMMAND ----------

#lap_times_final_df.write.parquet(f"{processed_folder_path}/{file_name}", mode="overwrite")
pit_stops_final_df.write.mode("overwrite").format("delta").saveAsTable(f"f1_processed.{file_name}")

# COMMAND ----------

dbutils.notebook.exit("success")
