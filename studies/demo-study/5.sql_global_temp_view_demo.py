# Databricks notebook source
# MAGIC %md
# MAGIC ## Access dataframes using sql
# MAGIC It's possible access a temporary view on other notebook
# MAGIC
# MAGIC #### Objectives
# MAGIC 1. Create global temporary views on dataframe
# MAGIC 1. Access the view from SQL cell
# MAGIC 1. Access the view from Python cell

# COMMAND ----------

# MAGIC %run "../../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Create temporary views on dataframe

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Access the view from SQL cell

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM global_temp.gv_race_results

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Access the view from Python cell

# COMMAND ----------

race_results_2019_df = spark.sql("SELECT * FROM global_temp.gv_race_results WHERE race_year = 2019")
