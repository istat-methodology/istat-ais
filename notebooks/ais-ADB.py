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

print(len(vessels_imo))
print(len(vessels_mmsi))

# COMMAND ----------

rotte_dataset=spark.read.format("csv").option("header",True).option("multiline",False).option("delimiter",",").load(rotte_dataset_name)

# COMMAND ----------

#Drop NA values
rotte_dataset=rotte_dataset.na.drop()

# COMMAND ----------

import pyspark.pandas as ps
pandas_rotte_dataset = ps.DataFrame(rotte_dataset)

# COMMAND ----------

pandas_ais_dataset = ps.DataFrame(ais_dataset)

# COMMAND ----------

#Keep only rows with no wrong values
pandas_rotte_dataset = pandas_rotte_dataset.where(pandas_rotte_dataset.stamp != 'stamp')
#display(pandas_rotte_dataset_f)

# COMMAND ----------

#build timestamp column to be added to rotte dataframe
stamp = pandas_rotte_dataset['stamp'].astype('float32')
from datetime import datetime
timestamp_column = stamp.apply(lambda x: datetime.fromtimestamp(x)) 
#print(timestamp_column)
pandas_rotte_dataset['timestamp']=timestamp_column
#display(pandas_rotte_dataset_f)

# COMMAND ----------

pandas_rotte_dataset=pandas_rotte_dataset.sort_values(by=['mmsi','stamp','lng','lat'],ascending=True)

# COMMAND ----------

print(len(pandas_rotte_dataset))
print(len(pandas_ais_dataset))

# COMMAND ----------

rotte_dataset_simplified = pandas_rotte_dataset[pandas_rotte_dataset['mmsi'].isin(vessels_mmsi)]

# COMMAND ----------

rotte_dataset_simplified = rotte_dataset_simplified.reset_index()

# COMMAND ----------

print(len(rotte_dataset_simplified))

# COMMAND ----------

display(rotte_dataset_simplified)

# COMMAND ----------

# MAGIC %md
# MAGIC ELABORAZIONE DELLE ROTTE

# COMMAND ----------

from datetime import datetime
import haversine as hs
from haversine import Unit

# COMMAND ----------

#elaborazione degli arrivi (df)
def arrival_elaboration(df_rotte):
    #df_rotte = rottesort

    dim=len(df_rotte)
    print("df_rotte len before: ",dim)

    df_arrival = ps.DataFrame(columns = ['row','mmsi','arrival','departure','lng','lat','lng_orig','lat_orig','speed','status'])

    oldmmsi=0
    sumrec=0
    start=0
    lat_orig=0
    lng_orig=0
    oldlng=0
    oldlat=0
    start='unknown'
    i=0
    status=0 ###0-nuovo 1-arrivato 2-partito

    start_time = datetime.now()

    df_rotte_pandas = df_rotte.to_pandas_on_spark()
    for item in df_rotte_pandas.itertuples():
        
        try:
            #item = df_rotte.iloc[i]
            mmsi,time_voyage,lng,lat,speed = item.mmsi,item.timestamp,float(item.lng),float(item.lat),int(item.speed)

            if(mmsi!=oldmmsi):
                if (status==1):#si riferisce alla old ship
                    df_arrival=df_arrival.append({'row':i,'mmsi':oldmmsi,'arrival':start,
                           'departure':end,'lng':oldlng,'lat':oldlat,'lng_orig':lng_orig,'lat_orig':lat_orig,
                            'speed':speed,'status':status},ignore_index=True)
                start,status,lng_orig,lat_orig,oldlng,oldlat,oldmmsi='unknown',0,0,0,0,0,mmsi
                oldmmsi = mmsi
    
            if(speed==0):          
                if (status==0):
                    start,oldlng,oldlat = time_voyage,lng,lat
                
                #To calculate distance in meters
                if(status<2):
                    loc1=(lat,lng)
                    loc2=(oldlat,oldlng)
            
                    distance = hs.haversine(loc1,loc2,unit=Unit.METERS)
            
                    if(distance > 3000.0):
                        #print(distance)
                        #if ((abs(oldlng-lng)+abs(oldlat-lat))>0.3):
                        df_arrival=df_arrival.append({'row':i,'mmsi':oldmmsi,'arrival':start,
                           'departure':end,'lng':oldlng,'lat':oldlat,
                                       'lng_orig':lng_orig,'lat_orig':lat_orig,'speed':speed,'status':status},ignore_index=True)
                        start,lng_orig,lat_orig,oldlng,oldlat = time_voyage,oldlng,oldlat,lng,lat
                    
                if(status==2):
                    start,lng_orig,lat_orig,oldlng,oldlat = time_voyage,oldlng,oldlat,lng,lat
                    
                end=time_voyage
                status=1
                
            if (speed>0):
                if(status==1):
                    loc1=(lat,lng)
                    loc2=(oldlat,oldlng)
                    distance = hs.haversine(loc1,loc2,unit=Unit.METERS)
                    if(distance > 3000.0):
                        df_arrival=df_arrival.append({'row':i,'mmsi':oldmmsi,'arrival':start,
                           'departure':end,'lng':oldlng,'lat':oldlat,
                            'lng_orig':lng_orig,'lat_orig':lat_orig,'speed':speed,'status':2},ignore_index=True)
                        
                        status,lng_orig,lat_orig = 2,oldlng,oldlat
                if(status==0):
                    status=2
                   
            #stampa di controllo
            if(i%1000000 == 0):
               print(i)
        except Exception as e:
            print(i," ",speed)
            print(e)
        i+=1
    #end for
    
    #scrive l'ultimo record
    if(status==1):
            df_arrival=df_arrival.append({'row':i,'mmsi':oldmmsi,'arrival':start,
                        'departure':end,'lng':oldlng,'lat':oldlat,
                        'lng_orig':lng_orig,'lat_orig':lat_orig,'speed':speed,'status':status},ignore_index=True)

 
    end_time = datetime.now()
    print('Duration: {}'.format(end_time - start_time))
    print("df_arrival len after: ",len(df_arrival))
    
    return(df_arrival)

# COMMAND ----------

df_rotte = rotte_dataset_simplified[['mmsi','stamp','timestamp','lng','lat','speed']]
df_rotte = df_rotte.sort_values(by=['mmsi','stamp'],ascending=[True,True])
df_rotte = df_rotte.reset_index()

# COMMAND ----------

df_arrival= arrival_elaboration(df_rotte)

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
