# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using cluster scoped credentials
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from demo cluster
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

storage_account_name = "formula1dlld"
container = "demo"
display(dbutils.fs.ls("abfss://" + container + "@" + storage_account_name + ".dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://" + container + "@" + storage_account_name + ".dfs.core.windows.net/circuits.csv"))
