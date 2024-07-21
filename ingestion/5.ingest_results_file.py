# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest results.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/commom_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the json file using the spark dataframe reader

# COMMAND ----------

# File
file_name = "results"
file_type = "json"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

# COMMAND ----------

# Define Schema
results_schema = StructType(fields=[
    StructField("constructorId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("fastestLapSpeed", StringType(), True),
    StructField("fastestLapTime",  StringType(), True),
    StructField("grid", IntegerType(), True),
    StructField("laps", IntegerType(), True),
    StructField("milliseconds", IntegerType(), True), 
    StructField("number", IntegerType(), True),
    StructField("points", FloatType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("raceId", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("resultId", IntegerType(), True),
    StructField("statusId", IntegerType(), True),
    StructField("time", StringType(), True)
])

# COMMAND ----------

# Read data
results_df = spark.read.json(
    f"{raw_folder_path}/{file_name}.{file_type}",
    schema = results_schema
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename and add columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

results_with_collumns_df = results_df \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("resultId", "result_id") \
    .withColumnRenamed("statusId", "status_id") \
    .withColumnRenamed("positionText", "position_text") \
    .withColumnRenamed("positionOrder", "position_order") \
    .withColumnRenamed("fastestLap", "fastest_lap") \
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
    .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

results_with_timestamp_df = add_ingestion_date(results_with_collumns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Drop unwanted columns from the dataframe

# COMMAND ----------

results_final_df = results_with_timestamp_df.drop(results_with_timestamp_df.status_id)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write data to datalake as parquet

# COMMAND ----------

#results_final_df.write.parquet(f"{processed_folder_path}/{file_name}", mode="overwrite", partitionBy="race_id")
results_final_df.write.mode("overwrite").partitionBy('race_id').format("delta").saveAsTable(f"f1_processed.{file_name}")

# COMMAND ----------

dbutils.notebook.exit("success")
