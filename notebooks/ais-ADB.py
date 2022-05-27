# Databricks notebook source
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}
# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(source = "adl://ais-data@istatdatalake.dfs.core.windows.net/", 
                 mount_point = "/mnt/istatdatalake/ais-data",extra_configs=configs)
              

# COMMAND ----------

display(dbutils.fs.ls("/mnt/istatdatalake/ais-data"))

# COMMAND ----------


