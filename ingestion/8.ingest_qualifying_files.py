# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest qualifying.json files

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/commom_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the json files using the spark dataframe reader

# COMMAND ----------

# Files
file_name = "qualifying"
folder_name = "qualifying"
file_type = "json"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

# Define Schema
qualifying_schema = StructType(fields=[
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1",  StringType(), True),
    StructField("q2",  StringType(), True),
    StructField("q3",  StringType(), True),
])

# COMMAND ----------

# Read data
qualifying_df = spark.read.option("multiline", True).json(
    f"{raw_folder_path}/{folder_name}",
    schema = qualifying_schema
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename and add columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

qualifying_renamed_df = qualifying_df \
    .withColumnRenamed("qualifyId", "qualify_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumn("data_source", lit(v_data_source))


# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write data to datalake as parquet

# COMMAND ----------

qualifying_final_df.write.parquet(f"{processed_folder_path}/{file_name}", mode="overwrite")

# COMMAND ----------

dbutils.notebook.exit("success")
