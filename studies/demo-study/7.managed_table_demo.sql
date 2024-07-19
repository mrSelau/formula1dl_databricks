-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Lesson Objectives
-- MAGIC 1. Create managed table using Python
-- MAGIC 1. Create managed table using SQL
-- MAGIC 1. Effect of dropping a managed table

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

-- MAGIC %python
-- MAGIC race_results_df.write.saveAsTable("demo.race_results_python", mode="overwrite")

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

desc extended race_results_python

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Table Query

-- COMMAND ----------

SELECT *
FROM demo.race_results_python

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2. Create managed table using SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Here, we created a managed table in the Hive metastore

-- COMMAND ----------

CREATE TABLE demo.race_results_sql
AS
SELECT *
FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Describe table

-- COMMAND ----------

desc extended race_results_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3. Effect of dropping a managed table

-- COMMAND ----------

DROP TABLE demo.race_results_sql
