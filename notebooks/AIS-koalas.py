# Databricks notebook source
mount_point = "/mnt/istatdatalake/ais-data"

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

try:  
  print(f"Unmounting {mount_point}")
  dbutils.fs.unmount(mount_point)
except:
  print(f"{mount_point} not mounted")

# Optionally, you can add <directory-name> to the source URI of your mount point.

dbutils.fs.mount(source = "abfss://ais-data@istatdatalake.dfs.core.windows.net/", 
                 mount_point = mount_point,extra_configs=configs)

display(dbutils.fs.ls("/mnt/istatdatalake/ais-data"))

# COMMAND ----------

import configparser

# COMMAND ----------

config = configparser.ConfigParser()
config.read('ais_config.ini')

# read values from a section
ais_dataset_name = config.get('INIT', 'ais_dataset_name')
rotte_dataset_name = config.get('INIT', 'rotte_dataset_name')
#rottesort_filename = config.get('PREPROCESS', 'rottesort_filename')

world_porti_filename = config.get('INIT', 'world_porti_filename')
arrivi_nei_porti_fname = config.get('ELABORATION','arrivi_nei_porti_fname')
rotte_arrivi_porti_world_fname = config.get('ELABORATION','rotte_arrivi_porti_world_fname')
rotte_arrivi_porti_all_fname = config.get('ELABORATION','rotte_arrivi_porti_all_fname')

#print(ais_dataset_name)
#print(rotte_dataset_name)

# COMMAND ----------

ais_dataset=spark.read.format("csv").option("inferSchema",True).option("header",True).option("multiline",False).option("delimiter",",").load(ais_dataset_name)

# COMMAND ----------

vessels_type = [x.type_summary for x in ais_dataset.select('type_summary').distinct().collect()]
print(vessels_type)

vessels_flag = [x.flag for x in ais_dataset.select('flag').distinct().collect()]
print(vessels_flag)

vessels_mmsi = [x.mmsi for x in ais_dataset.select('mmsi').distinct().collect()]
vessels_imo = [x.imo for x in ais_dataset.select('imo').distinct().collect()]

# COMMAND ----------

print(len(vessels_imo))
print(len(vessels_mmsi))

# COMMAND ----------

rotte_dataset=spark.read.format("csv").option("header",True).option("multiline",False).option("delimiter",",").load(rotte_dataset_name)

# COMMAND ----------

#Drop NA values
rotte_dataset=rotte_dataset.na.drop()

# COMMAND ----------

import databricks.koalas as ks

# COMMAND ----------

ais_dataset_ks = ks.DataFrame(ais_dataset)
display(ais_dataset_ks)

# COMMAND ----------

rotte_dataset_ks = ks.DataFrame(rotte_dataset)
display(rotte_dataset_ks)

# COMMAND ----------

header= rotte_dataset_ks.head(1)
header
