# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest races.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/commom_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the csv file using the spark dataframe reader

# COMMAND ----------

# File
file_name = "races"
file_type = "csv"

# COMMAND ----------

# Import types
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

# Define Schema
races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), False),
    StructField("round", IntegerType(), False),
    StructField("circuitId", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("date", DateType(), False),
    StructField("time", StringType(), False),
    StructField("url", StringType(), False)
])

# COMMAND ----------

# Read data
races_df = spark.read.csv(
    f"{raw_folder_path}/{file_name}.{file_type}",
    header = True,           #Enable line 1 as header,
    #inferSchema = True     #Enable inferSchema
    schema = races_schema
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Add ingestion date and race_timestamp to df

# COMMAND ----------

# Import functions
from pyspark.sql.functions import current_timestamp, to_timestamp, lit, concat

# COMMAND ----------

races_with_race_timestamp_df = races_df \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn(
        'race_timestamp', 
        to_timestamp(
            concat(
                races_df.date,
                lit(' '),
                races_df.time,
            ),
            "yyyy-MM-dd HH:mm:ss"
        )
    )

# COMMAND ----------

races_with_timestamp_df = add_ingestion_date(races_with_race_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Select only the required columns

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(
    races_with_timestamp_df.raceId.alias("race_id"),
    races_with_timestamp_df.year.alias("race_year"),
    races_with_timestamp_df.round,
    races_with_timestamp_df.circuitId.alias("circuit_id"),
    races_with_timestamp_df.name,
    races_with_timestamp_df.race_timestamp,
    races_with_timestamp_df.ingestion_date
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to datalake as parquet

# COMMAND ----------

#races_selected_df.write.parquet(f"{processed_folder_path}/{file_name}", mode="overwrite")
races_selected_df.write.mode("overwrite").format("delta").saveAsTable(f"f1_processed.{file_name}")

# COMMAND ----------

dbutils.notebook.exit("success")
