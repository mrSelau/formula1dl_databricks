-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Lesson Objectives
-- MAGIC 1. Spark SQL documentation
-- MAGIC 1. Create Database demo
-- MAGIC 1. Data tab in the UI
-- MAGIC 1. SHOW command
-- MAGIC 1. DESCRIBE command
-- MAGIC 1. Find the current database

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 1. Create database demo

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 4. Show command

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 5. DESCRIBE command

-- COMMAND ----------

DESC DATABASE demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 6. Find the current database

-- COMMAND ----------

USE demo

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES
