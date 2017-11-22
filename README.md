# IPDR Analysis
By Samir Gupta (sgupta@cloudera.com)

## Project Overview
This project uses Python, Spark and Impala to analyze daily IPDR records from a Telco's network data. The records are 
stored in AWS S3 organized by date. 

Each record has the following feilds:
* CMTSDEVICE - device ID
* ADDRESS_RESOLUTION - IP address
* CREATION_DATE - date
* CMMACADDRESS - MAC address
* HUB_CODE - hub code
* AREA_CODE - area code
* BYTES_UPSTREAM - number of upstream bytes
* BYTES_DOWNSTREAM - number of downstream bytes
* DXSERVICELEVEL - service level
* OPTICAL_RX - optical RX
* TPIA - TPIA services
* SERVICE_CLASS_PAIR_NAME - service class name

## Loading Data
Scripts for creating Impala external tables on top of IPDR CSV files are located in in the ```Impala Scripts``` directory. 
These are HUE Documents that can be imported into HUE for execution. Data is partitioned by date and organized into
directories in S3 named by date. 

## Analyzing Data in CDSW
```IPDR_Analysis.py``` contains a sample Pyspark application that plots various aspects of the IPDR data. This file can either
be run in the pyspark shell, or in the Cloudera Data Science Workbench. 