# Databricks notebook source
# MAGIC %run "../../includes/configuration"

# COMMAND ----------

# Files
file_name = "races"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/{file_name}")

# COMMAND ----------

races_filtered_df = races_df.filter((races_df.race_year == 2019) & (races_df.round <= 5))
display(races_filtered_df)
