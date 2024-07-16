# Databricks notebook source
# MAGIC %run "../../includes/configuration"

# COMMAND ----------

# Files
circuits_file = "circuits"
races_file = "races"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/{circuits_file}") \
    .filter("circuit_id < 70") \
    .withColumnRenamed("name", "circuit_name")

races_df = spark.read.parquet(f"{processed_folder_path}/{races_file}") \
    .filter("race_year = 2019")\
    .withColumnRenamed("name", "race_name")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Inner Join

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Left Join

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Right Join

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Full Join

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Semi Join

# COMMAND ----------

# Get only columns from the circuits_df(Left)
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Anti Join

# COMMAND ----------

# Get information from circuits_df(Left) that is not in races_df(Right)
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cross Join

# COMMAND ----------

# Combine the two dataframes
race_circuits_df = circuits_df.crossJoin(races_df)
