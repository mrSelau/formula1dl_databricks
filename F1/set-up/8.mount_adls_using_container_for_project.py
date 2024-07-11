# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake Container for the Project

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Definy Information

# COMMAND ----------

# Storage
storage_account_name = "formula1dlld"
# Get secrets from Key Vault
client_id = dbutils.secrets.get(scope='formula1-scope', key='formula1dl-client-id')
tenant_id = dbutils.secrets.get(scope='formula1-scope', key='formula1dl-tenant-id')
client_secret = dbutils.secrets.get(scope='formula1-scope', key='formula1dl-client-secret')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Configure access

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Functio to Create Mount

# COMMAND ----------

def mount_adls(storage_account_name, container_name, configs):
    # Verify if mount already created
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        # Unmount
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    # Create
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Mount

# COMMAND ----------

mount_adls(storage_account_name, 'raw', configs)
mount_adls(storage_account_name, 'processed', configs)
mount_adls(storage_account_name, 'presentation', configs)
