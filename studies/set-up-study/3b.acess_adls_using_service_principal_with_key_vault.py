# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using service principal
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 1. Generate a secret/ password for the application
# MAGIC 1. Set spark Config with App/ Client Id, Directory/ Tenant Is & Secret
# MAGIC 1. Assign Role 'Storage Blob Data Contributor' to the Data Lake

# COMMAND ----------

dbutils.secrets.list('formula1-scope')

# COMMAND ----------

storage_account_name = "formula1dlld"
# Service Principal
client_id = dbutils.secrets.get(scope='formula1-scope', key='formula1dl-client-id')
tenant_id = dbutils.secrets.get(scope='formula1-scope', key='formula1dl-tenant-id')
# Secret
client_secret = dbutils.secrets.get(scope='formula1-scope', key='formula1dl-client-secret')

spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

container = "demo"
display(dbutils.fs.ls("abfss://" + container + "@" + storage_account_name + ".dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://" + container + "@" + storage_account_name + ".dfs.core.windows.net/circuits.csv"))
