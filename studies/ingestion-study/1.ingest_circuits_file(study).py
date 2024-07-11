# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the csv file using the spark dataframe reader

# COMMAND ----------

# MAGIC %md
# MAGIC Search the mount path

# COMMAND ----------

dbutils.fs.mounts()
display(dbutils.fs.ls("/mnt/formula1dlld/raw"))

# COMMAND ----------

# MAGIC %md
# MAGIC Importing types

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a schema

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

# MAGIC %md
# MAGIC Read

# COMMAND ----------

# Storage description
storage_account_name = "formula1dlld"
# Origen
origen_blob = "raw"
# Destiny
destiny_blob = "processed"

# COMMAND ----------

# Read data
circuits_df = spark.read.csv(
    f"/mnt/{storage_account_name}/{origen_blob}/circuits.csv",
    header = True,           #Enable line 1 as header,
    #inferSchema = True     #Enable inferSchema
    schema = circuits_schema
    )
display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Print column types 

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Information of collumn data

# COMMAND ----------

display(circuits_df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - [Select](url) only the required columns

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    circuits_df.circuitId, 
    circuits_df.circuitRef,
    circuits_df.name,
    circuits_df.location,
    circuits_df.country,
    circuits_df.lat.alias("latitude"),
    circuits_df.lng,
    circuits_df.alt
    )

# COMMAND ----------

# Outras formas de usar
# 1 - "column_name"
# 2 - df["column_name"]
circuits_selected_df = circuits_df.select(
    "circuitId", 
    "circuitRef",
    "name",
    "location",
    circuits_df["country"],
    circuits_df["lat"],
    circuits_df["lng"],
    circuits_df["alt"]
    )

# COMMAND ----------

display(circuits_selected_df)

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

display(circuits_rename_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

circuits_final_df = circuits_rename_df \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("env", lit("Production"))

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.parquet(f"/mnt/{storage_account_name}/{destiny_blob}/circuits", mode="overwrite")


# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/{storage_account_name}/{destiny_blob}/circuits"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Test - Reading data 

# COMMAND ----------

df = spark.read.parquet(f"/mnt/{storage_account_name}/{destiny_blob}/circuits")
display(df)
