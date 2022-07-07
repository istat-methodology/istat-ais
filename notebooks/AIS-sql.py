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
print(rotte_dataset_name)

# COMMAND ----------

ais_dataset=spark.read.format("csv").option("header",True).option("multiline",False).option("delimiter",",").load(ais_dataset_name)

# COMMAND ----------

vessels_type = [x.type_summary for x in ais_dataset.select('type_summary').distinct().collect()]
print(vessels_type)

vessels_flag = [x.flag for x in ais_dataset.select('flag').distinct().collect()]
print(vessels_flag)

vessels_mmsi = [x.mmsi for x in ais_dataset.select('mmsi').distinct().collect()]
vessels_imo = [x.imo for x in ais_dataset.select('imo').distinct().collect()]

# COMMAND ----------

rotte_dataset=spark.read.format("csv").option("header",True).option("multiline",False).option("delimiter",",").load(rotte_dataset_name)

# COMMAND ----------

#Drop NA values
rotte_dataset=rotte_dataset.na.drop()

# COMMAND ----------


rotte_dataset = rotte_dataset.filter(rotte_dataset.stamp != "stamp")

# COMMAND ----------

from pyspark.sql.types import FloatType

stamp = rotte_dataset.select(
            (rotte_dataset.stamp.cast(FloatType())).alias('stamp'),
)

stamp.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col,udf
from datetime import datetime

def f(a):
    tmp = datetime.fromtimestamp(a)
    print(tmp)
    return tmp

# COMMAND ----------

datetime.fromtimestamp(1630488290)

# COMMAND ----------

from pyspark.sql.types import FloatType
reg_f = udf(lambda x : f(x), FloatType())

# COMMAND ----------

rotte_dataset.withColumn("timestamp",reg_f(col('stamp').cast(FloatType()))).show()
