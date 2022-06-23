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

ais_dataset=spark.read.format("csv").option("header",True).option("multiline",False).option("delimiter",",").load(ais_dataset_name)

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

#Keep only rows with no wrong values
rotte_dataset_f = rotte_dataset.where(rotte_dataset.stamp != 'stamp')
#display(pandas_rotte_dataset_f)

# COMMAND ----------

#build timestamp column to be added to rotte dataframe
stamp = rotte_dataset_f['stamp'].astype('float32')
from datetime import datetime
timestamp_column = stamp.apply(lambda x: datetime.fromtimestamp(x)) 
#print(timestamp_column)
rotte_dataset_f['timestamp']=timestamp_column

# COMMAND ----------

import pyspark.pandas as ps
pandas_rotte_dataset = ps.DataFrame(rotte_dataset)

# COMMAND ----------

#Keep only rows with no wrong values
pandas_rotte_dataset_f = pandas_rotte_dataset.where(pandas_rotte_dataset.stamp != 'stamp')
#display(pandas_rotte_dataset_f)

# COMMAND ----------

#build timestamp column to be added to rotte dataframe
stamp = pandas_rotte_dataset_f['stamp'].astype('float32')
from datetime import datetime
timestamp_column = stamp.apply(lambda x: datetime.fromtimestamp(x)) 
#print(timestamp_column)
pandas_rotte_dataset_f['timestamp']=timestamp_column
#display(pandas_rotte_dataset_f)

# COMMAND ----------

pandas_rotte_dataset_f=pandas_rotte_dataset_f.sort_values(by=['mmsi','stamp','lng','lat'],ascending=True)

# COMMAND ----------

class Utils(object):
   def intersect(self, nums1, nums2):
      """
      :type nums1: List[int]
      :type nums2: List[int]
      :rtype: List[int]
      """
      m = {}
      print(len(nums1))
      print(len(nums2))
      if len(nums1)<len(nums2):
         nums1,nums2 = nums2,nums1
      for i in nums1:
         if i not in m:
            m[i] = 1
         else:
            m[i]+=1
      result = []
      for i in nums2:
         if i in m and m[i]:
            m[i]-=1
            result.append(i)
      return result

# COMMAND ----------

rotte_mmsi = [x.mmsi for x in rotte_dataset.select('mmsi').distinct().collect()]
print(len(rotte_mmsi))

# COMMAND ----------

utils = Utils()
mmsi_intersection = utils.intersect(vessels_mmsi, rotte_mmsi)
print(f'result intersection: {mmsi_intersection}')

# COMMAND ----------

#Drop rows whose mmsi is not in rotte_092021_simplified(rotte_092021_mmsi,vessels_mmsi) from rotte_092021,
# i.e. get only rows whos mmsi is in mmsi_intersection

#rotte_dataset_simplified = rotte_dataset[rotte_dataset['mmsi'].astype(int).isin(mmsi_intersection)]
#rotte_dataset_simplified.head()

rotte_dataset_simplified = pandas_rotte_dataset_f[pandas_rotte_dataset_f['mmsi'].astype(int).isin(mmsi_intersection)]
#len(rotte_dataset_simplified)

# COMMAND ----------

print(len(pandas_rotte_dataset_f))
print(len(rotte_dataset_simplified))

# COMMAND ----------

ELABORAZIONE DELLE ROTTE


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
