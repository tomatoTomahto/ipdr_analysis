# # Shaw IPDR Data Analysis
# This script explores and analyzes IPDR network data. We start by plotting the data downloaded and uploaded each day
# to understand a daily aggregate trend. We then visualize the aggregate usage by weekday, to see if there are any hot spots on
# a particular day of the week. We then dig into the top data usage first by area code, then by MAC address. 

# ## Import libraries and initialize Spark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
get_ipython().magic(u'matplotlib inline')
import seaborn as sb
import pandas
from datetime import datetime

KB=1000
MB=KB*1000
GB=MB*1000

spark = SparkSession\
  .builder\
  .appName("IPDRAnalysis")\
  .getOrCreate()

# ## Import IPDR data from AWS S3 storage
# All data is stored in csv files in AWS S3. This data can be loaded into Spark and transformed for analysis. The schema
# for the CSV files is shown below:
ipdrFields = [StructField('cmtsdevice', StringType(), True),
              StructField('address_resolution', StringType(), True),
              StructField('creation_date', DateType(), True),
              StructField('cmmacaddress', StringType(), True),
              StructField('hub_code', StringType(), True),
              StructField('area_code', StringType(), True),
              StructField('bytes_upstream', IntegerType(), True),
              StructField('bytes_downstream', IntegerType(), True),
              StructField('dxservicelevel', StringType(), True),
              StructField('optical_rx', StringType(), True),
              StructField('service_class_pair_name', StringType(), True)]

ipdrSchema = StructType(ipdrFields)
  
dailyIPDR = spark.read.csv("s3a://sgupta-s3/shaw/csv/*2017-10*/*", schema=ipdrSchema)\
  .na.drop()
dailyIPDR.printSchema()

# ## Explore the data
# Spark allows data scientists to explore a sample of the data for analysis. Once a set of transformations
# have been developed on the data, the same set of commands can be re-run on the entire dataset by simply
# modifying the original dataframe (ie. removing the sample() function). Here's a sample of the CSV data
#dailyIPDR = dailyIPDR.sample(False, 0.05) # Comment this line out to run analysis on ALL data
dailyIPDR.persist()
dailyIPDR.show(5)

# Let's start by computing an aggregate view of the daily upload and download activity
dailyVolume = dailyIPDR.groupBy('creation_date')\
  .agg((F.sum('bytes_downstream')/GB).alias('download'), (F.sum('bytes_upstream')/GB).alias('upload'))
dailyVolume.persist()

# ### Plot the total download and upload volume by day
# Below is a visualization that shows the daily data volume downloaded and uploaded for the month of October. 
dailyVolume.orderBy('creation_date')\
  .toPandas().plot(kind='line', x='creation_date', title='Daily Download & Upload GB')
  
# ### Basic Statistics on Upload and Download volume
dv = dailyVolume.toPandas()
dv.describe()
  
# ### Plot a boxplot of download GB by day of week
# Let's see if there are any patterns between days of the week. Doesn't look like it:
dv['dates'] = pd.to_datetime(dv['creation_date'])
dv['day_of_week'] = dv['dates'].dt.weekday_name
sb.boxplot(x="day_of_week", y="download", data=dv, whis=np.inf, palette="vlag")

# ### Plot the top downloaders by area code
# We want to understand which area codes download and upload the most data
dailyIPDR.groupBy('creation_date','area_code')\
  .agg((F.sum('bytes_downstream')/GB).alias('download'))\
  .groupBy('area_code')\
  .agg(F.avg('download').alias('download'))\
  .orderBy('download')\
  .toPandas().plot(kind='bar', x='area_code', y='download', title='Top Area Codes by Avg GB Downloaded', legend=False)
  
# ### Plot the MAC Addresses in "CG" area code using the most data
# It seems that the CG area code is the top downloader, let's plot the top MAC addresses within this area code
volumeByMAC = dailyIPDR.filter('area_code="cg"')\
  .groupBy('cmmacaddress')\
  .agg((F.sum('bytes_downstream')/MB).alias('download'), (F.sum('bytes_upstream')/MB).alias('upload'))\
  .withColumn('total', F.col('download')+F.col('upload'))\
  .orderBy('total', ascending=False)\
  .limit(20)
  
volumeByMAC.limit(20).toPandas().plot.barh(stacked=True, x='cmmacaddress', title='Top 20 MAC Addresses for CG Area Code');