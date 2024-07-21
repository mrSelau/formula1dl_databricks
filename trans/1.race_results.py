# Databricks notebook source
# MAGIC %md
# MAGIC ## Race Results
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/commom_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read processed files

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races") \
    .withColumnRenamed("name", "race_name") \
    .withColumnRenamed("race_timestamp", "race_date")


circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits") \
    .withColumnRenamed("name", "circuit_name") \
    .withColumnRenamed("location", "circuit_location")

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("nationality", "driver_nationality")

constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors") \
    .withColumnRenamed("name", "team")

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
    .withColumnRenamed("time", "race_time")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Join, Filter, Select, Add collumn

# COMMAND ----------

race_results_df = results_df \
    .join(races_df, results_df.race_id == races_df.race_id, "left") \
    .join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "left") \
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "left") \
    .select(
        races_df.race_year, 
        races_df.race_name, 
        races_df.race_date,
        circuits_df.circuit_location,
        drivers_df.driver_name,
        drivers_df.driver_number,
        drivers_df.driver_nationality,
        constructors_df.team,
        results_df.grid,
        results_df.fastest_lap,
        results_df.race_time,
        results_df.points,
        results_df.position
    )


# COMMAND ----------

final_df = add_ingestion_date(race_results_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write

# COMMAND ----------

#final_df.write.parquet(f"{presentation_folder_path}/race_results", mode="overwrite")
final_df.write.mode("overwrite").format("delta").saveAsTable("f1_presentation.race_results")
