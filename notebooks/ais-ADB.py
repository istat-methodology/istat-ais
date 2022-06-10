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


             

# COMMAND ----------

display(dbutils.fs.ls("/mnt/istatdatalake/ais-data"))

# COMMAND ----------

import configparser

# COMMAND ----------

config = configparser.ConfigParser()
config.read('ais_config.ini')

# COMMAND ----------

# read values from a section
ais_dataset_name = config.get('INIT', 'ais_dataset_name')
rotte_dataset_name = config.get('INIT', 'rotte_dataset_name')
#rottesort_filename = config.get('PREPROCESS', 'rottesort_filename')

world_porti_filename = config.get('INIT', 'world_porti_filename')
arrivi_nei_porti_fname = config.get('ELABORATION','arrivi_nei_porti_fname')
rotte_arrivi_porti_world_fname = config.get('ELABORATION','rotte_arrivi_porti_world_fname')
rotte_arrivi_porti_all_fname = config.get('ELABORATION','rotte_arrivi_porti_all_fname')

# COMMAND ----------

print(ais_dataset_name)

# COMMAND ----------

df=spark.read.format("csv").option("header",True).option("multiline",False).option("delimiter",",").load(ais_dataset_name)

# COMMAND ----------

from haversine import haversine, Unit

# COMMAND ----------

display(df)

# COMMAND ----------

#display(df[df['type_summary'] == 'NULL'])
#display(df[df['type_summary'] == 'Tug'])

#import pandas as pd

temp_table = "temp_table"

df.createOrReplaceTempView(temp_table)
    

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(distinct imo) from temp_table where type_summary == 'NULL' 

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct imo) from temp_table

# COMMAND ----------

import numpy as np

# COMMAND ----------


a_df=df.drop_duplicates(['type_summary','shiptype'])[['type_summary','shiptype']]

# COMMAND ----------

display(a_df)
