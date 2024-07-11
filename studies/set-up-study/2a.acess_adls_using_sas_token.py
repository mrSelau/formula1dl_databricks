# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC 1. Set the spark config for SAS Token
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

storage_account_name = "formula1dlld"
token = "<token>"

spark.conf.set("fs.azure.account.auth.type." + storage_account_name + ".dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type." + storage_account_name + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token." + storage_account_name + ".dfs.core.windows.net", token)

# COMMAND ----------

container = "demo"
display(dbutils.fs.ls("abfss://" + container + "@" + storage_account_name + ".dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://" + container + "@" + storage_account_name + ".dfs.core.windows.net/circuits.csv"))
