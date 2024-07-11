# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access key
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

storage_account_name = "formula1dlld"
container = "demo"
key = "<key>"

spark.conf.set(
    "fs.azure.account.key." + storage_account_name + ".dfs.core.windows.net",
    key
    )

# COMMAND ----------


display(dbutils.fs.ls("abfss://" + container + "@" + storage_account_name + ".dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlld.dfs.core.windows.net/circuits.csv"))
