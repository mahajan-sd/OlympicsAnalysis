# Databricks notebook source
# 9th Nov 2024 -  Creating notebook for all common imports

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Defining Common paths
class inputPath:

  rootFolder = 'dbfs:/FileStore/OlympicsData/'

  files = {
    'Athletes' : rootFolder + 'Athletes.xlsx',
    'Coaches' : rootFolder + 'Coaches.xlsx',
    'EntriesGender' : rootFolder + 'EntriesGender.xlsx',
    'Medals' : rootFolder + 'Medals.xlsx',
    'Teams' : rootFolder + 'Teams.xlsx',
  }

class outputPath:

  rootFolder = 'dbfs:/FileStore/OlympicsData/table/'

  table = {
    'medal' : rootFolder + 'medal/'
  }


# COMMAND ----------

print('Files loaded successfully')
