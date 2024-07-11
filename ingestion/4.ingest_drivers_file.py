# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the csv file using the spark dataframe reader

# COMMAND ----------

# Storage description
storage_account_name = "formula1dlld"
# Origen
origen_blob = "raw"
# Destiny
destiny_blob = "processed"
# File
file_name = "drivers"
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
    f"/mnt/{storage_account_name}/{origen_blob}/{file_name}.{file_type}",
    #inferSchema = True     #Enable inferSchema
    schema = drivers_schema
    )
display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Drop unwanted columns from the dataframe

# COMMAND ----------

drivers_dropped_df = drivers_df.drop(drivers_df.url)

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
