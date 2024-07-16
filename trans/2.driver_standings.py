# Databricks notebook source
# MAGIC %md
# MAGIC ## Driver Standings
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/commom_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read processed files

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Group By

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col

# COMMAND ----------

driver_standings_df = race_results_df \
    .groupBy("race_year", "driver_name") \
    .agg(
        sum("points").alias("total_points"),
        count(when(race_results_df.position == 1, True)).alias("wins")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Order by , Window Function

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

# COMMAND ----------

final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write

# COMMAND ----------

final_df.write.parquet(f"{presentation_folder_path}/driver_standings", mode="overwrite")
