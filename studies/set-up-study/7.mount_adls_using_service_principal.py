# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake using Service Principal
# MAGIC 1. Get client_id, tenant_id, client_secret using key vault
# MAGIC 1. Set Spark config with App/ Client Id,Directory, Tenant Id, Secret
# MAGIC 1. Call file system utilities mount to mount the storage
# MAGIC 1. Explore other file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

# Storage
storage_account_name = "formula1dlld"
blob = "demo"
# Service Principal
client_id = dbutils.secrets.get(scope='formula1-scope', key='formula1dl-client-id')
tenant_id = dbutils.secrets.get(scope='formula1-scope', key='formula1dl-tenant-id')
# Secret
client_secret = dbutils.secrets.get(scope='formula1-scope', key='formula1dl-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = f"abfss://{blob}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{blob}",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/{storage_account_name}/{blob}"))

# COMMAND ----------

display(spark.read.csv(f"/mnt/{storage_account_name}/{blob}/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/formula1dlld/demo')
