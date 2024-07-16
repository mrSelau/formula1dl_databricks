# Databricks notebook source
# MAGIC %run "../../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Prepare dataframe

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")


# COMMAND ----------

demo_df = race_results_df.filter(race_results_df.race_year == 2020)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Examples

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

demo_df.select(count("*")).show()

# COMMAND ----------

demo_df.select(countDistinct(demo_df.race_name)).show()

# COMMAND ----------

demo_df.select(sum(demo_df.points)).show()

# COMMAND ----------

demo_df.filter(demo_df.driver_name == 'Lewis Hamilton') \
    .select(sum(demo_df.points), countDistinct(demo_df.race_name)) \
    .withColumnRenamed('sum(points)', 'total_points') \
    .withColumnRenamed('count(DISTINCT race_name)', 'number_of_races') \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Group by

# COMMAND ----------

# Use agg to agregate multiple columns
demo_df \
    .groupBy("driver_name") \
    .sum("points") \
    .show()

# COMMAND ----------

# Use agg to agregate multiple columns
demo_df \
    .groupBy("driver_name") \
    .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races")) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Window Functions

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

demo_df = race_results_df.filter("race_year in (2019, 2020)")

# COMMAND ----------

demo_grouped_df = demo_df \
    .groupBy("race_year", "driver_name") \
    .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races"))

# COMMAND ----------

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))
demo_grouped_df.withColumn("rank", rank().over(driverRankSpec)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Full Join

# COMMAND ----------

# MAGIC %md
# MAGIC #### Semi Join

# COMMAND ----------

# MAGIC %md
# MAGIC #### Anti Join

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cross Join
