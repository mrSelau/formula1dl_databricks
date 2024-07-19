-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Lesson Objectives
-- MAGIC 1. Create Temp View
-- MAGIC 1. Create Global Temp View
-- MAGIC 1. Create Permanent View

-- COMMAND ----------

-- MAGIC %run "../../includes/configuration"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 1. Create Temp View
-- MAGIC It's not possible access a temporary view on other notebook

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS 
(
  SELECT * 
  FROM demo.race_results_python
  WHERE race_year = 2018
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Query

-- COMMAND ----------

SELECT * FROM v_race_results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2. Create Global Temp View
-- MAGIC It's possible access a global temporary view on other notebook

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS 
(
  SELECT * 
  FROM demo.race_results_python
  WHERE race_year = 2012
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Query

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Diference

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SHOW TABLES IN global_temp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3. Create Permanent View
-- MAGIC It's possible access this view after the cluster is restarted

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results
AS 
(
  SELECT * 
  FROM demo.race_results_python
  WHERE race_year = 2000
)
