# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest pit_stops.json file

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
file_name = "pit_stops"
file_type = "json"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

# COMMAND ----------

# Define Schema
pit_stops_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time",  StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

# Read data
pit_stops_df = spark.read.option("multiline", True).json(
    f"{raw_folder_path}/{file_name}.{file_type}",
    schema = pit_stops_schema
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename and add columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

pit_stops_renamed_df = pit_stops_df \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

pit_stops_final_df = add_ingestion_date(pit_stops_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write data to datalake as parquet

# COMMAND ----------

#pit_stops_final_df.write.parquet(f"{processed_folder_path}/{file_name}", mode="overwrite")
pit_stops_final_df.write.mode("overwrite").format("delta").saveAsTable(f"f1_processed.{file_name}")

# COMMAND ----------

dbutils.notebook.exit("success")
