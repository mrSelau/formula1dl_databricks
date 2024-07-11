# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest circuits.csv file

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
file_name = "circuits"
file_type = "csv"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), False),
    StructField("name", StringType(), False),
    StructField("location", StringType(), False),
    StructField("country", StringType(), False),
    StructField("lat", DoubleType(), False),
    StructField("lng", DoubleType(), False),
    StructField("alt", IntegerType(), False),
    StructField("url", StringType(), False)
])

# COMMAND ----------

# Read data
circuits_df = spark.read.csv(
    f"/mnt/{storage_account_name}/{origen_blob}/{file_name}.{file_type}",
    header = True,           #Enable line 1 as header,
    #inferSchema = True     #Enable inferSchema
    schema = circuits_schema
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Select only the required columns

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    circuits_df.circuitId, 
    circuits_df.circuitRef,
    circuits_df.name,
    circuits_df.location,
    circuits_df.country,
    circuits_df.lat,
    circuits_df.lng,
    circuits_df.alt
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename the columns as required

# COMMAND ----------

circuits_rename_df = circuits_selected_df \
    .withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude") \
    

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = circuits_rename_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.parquet(f"/mnt/{storage_account_name}/{destiny_blob}/circuits", mode="overwrite")
