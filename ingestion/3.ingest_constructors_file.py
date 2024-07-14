# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest constructors.json file

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
file_name = "constructors"
file_type = "json"

# COMMAND ----------

# Define Schema
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

# Read data
constructors_df = spark.read.json(
    f"{raw_folder_path}/{file_name}.{file_type}",
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

constructors_renamed_df = constructors_dropped_df \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("constructorRef", "constructor_ref") \
    .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

constructors_final_df = add_ingestion_date(constructors_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to datalake as parquet

# COMMAND ----------

constructors_final_df.write.parquet(f"{processed_folder_path}/{file_name}", mode="overwrite")

# COMMAND ----------

dbutils.notebook.exit("success")
