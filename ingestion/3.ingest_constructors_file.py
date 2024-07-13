# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the json file using the spark dataframe reader

# COMMAND ----------

# Storage description
storage_account_name = "formula1dlld"
# Origen
origen_blob = "raw"
# Destiny
destiny_blob = "processed"
# File
file_name = "constructors"
file_type = "json"

# COMMAND ----------

# Define Schema
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

# Read data
constructors_df = spark.read.json(
    f"/mnt/{storage_account_name}/{origen_blob}/{file_name}.{file_type}",
    schema = constructors_schema
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Drop unwanted columns from the dataframe

# COMMAND ----------

constructors_dropped_df = constructors_df.drop(constructors_df.url)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename columns and Add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, lit, concat

# COMMAND ----------

constructors_final_df = constructors_dropped_df \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("constructorRef", "constructor_ref") \
    .withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to datalake as parquet

# COMMAND ----------

constructors_final_df.write.parquet(f"/mnt/{storage_account_name}/{destiny_blob}/{file_name}", mode="overwrite")
