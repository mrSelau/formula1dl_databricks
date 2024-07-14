# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest drivers.json file

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

# Define Schema
name_schema = StructType(fields=[
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

drivers_schema = StructType(fields=[
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

# Read data
drivers_df = spark.read.json(
    f"{raw_folder_path}/{file_name}.{file_type}",
    schema = drivers_schema
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename and add columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, concat

# COMMAND ----------

drivers_with_collumns_df = drivers_df \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef", "driver_ref") \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("name",
                concat(
                    "name.forename",
                    lit(" "),
                    "name.surname",
                ))

# COMMAND ----------

constructors_with_timestamp_df = add_ingestion_date(drivers_with_collumns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Drop unwanted columns from the dataframe

# COMMAND ----------

drivers_final_df = constructors_with_timestamp_df.drop(constructors_with_timestamp_df.url)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write data to datalake as parquet

# COMMAND ----------

drivers_final_df.write.parquet(f"{processed_folder_path}/{file_name}", mode="overwrite")

# COMMAND ----------

dbutils.notebook.exit("success")
