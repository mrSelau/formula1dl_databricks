-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Lesson Objectives
-- MAGIC 1. Create external table using Python
-- MAGIC 1. Create external table using SQL
-- MAGIC 1. Effect of dropping a external table

-- COMMAND ----------

-- MAGIC %run "../../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 1. Create managed table using Python

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Here, we created a managed table in the Hive metastore

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py", mode="overwrite")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Select database and show tables

-- COMMAND ----------

USE demo;
SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Describe table

-- COMMAND ----------

desc extended demo.race_results_ext_py

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2. Create managed table using SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Here, we created a managed table in the Hive metastore

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(
  race_year INT,
  race_name STRING,
  race_date TIMESTAMP,
  circuit_location STRING,
  driver_name STRING,
  driver_number INT,
  driver_nationality STRING,
  team STRING,
  grid INT,
  fastest_lap INT,
  race_time STRING,
  points FLOAT,
  position INT,
  created_date TIMESTAMP
)
LOCATION "/mnt/formula1dlld/presentation/race_results_ext_sql"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Describe table

-- COMMAND ----------

desc extended race_results_ext_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Insert data

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * 
FROM demo.race_results_ext_py
WHERE race_year = 2020;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3. Effect of dropping a external table

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql
