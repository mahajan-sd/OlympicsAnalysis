# Databricks notebook source
# Data Analysis 

#Goal
# To cleanse the data and preapare it for dahshboards
# Create Dashbords accoring to the datasets prepared 

# COMMAND ----------

# DBTITLE 1,Comman imports
# MAGIC %run ../OLYMPICS_DATA_ANALYSIS/Common/Constants

from Common.Constants import *

# COMMAND ----------

# DBTITLE 1,Reading Paths
# Reading the path for files where fiels are present
# Input Paths

atheletes = inputPath.files['Athletes']
Coaches = inputPath.files['Coaches'] 
EntriesGender = inputPath.files['EntriesGender']
Medals = inputPath.files['Medals']
Teams = inputPath.files['Teams']

# COMMAND ----------

# DBTITLE 1,Creating DF
# Creating dataFrames for all datasources

# atheletes_df = spark.read.format('com.crealytics.spark.excel').option('header', 'true').option('inferSchema', 'true').load(atheletes)

# Coaches_df = spark.read.format('com.crealytics.spark.excel').option('header', 'true').option('inferSchema', 'true').load(Coaches)

# EntriesGender_df = spark.read.format('com.crealytics.spark.excel').option('header', 'true').option('inferSchema', 'true').load(EntriesGender)

# Medals_df = spark.read.format('com.crealytics.spark.excel').option('header', 'true').option('inferSchema', 'true').load(Medals)

# Teams_df = spark.read.format('com.crealytics.spark.excel').option('header', 'true').option('inferSchema', 'true').load(Teams)


# # COMMAND ----------

# # DBTITLE 1,Athletes

# atheletes_df_renammed = atheletes_df.withColumnRenamed('Name', 'PlayerName').withColumnRenamed('NOC', 'Country').withColumnRenamed('Discipline', 'SportName').filter((col('PlayerName').isNotNull()) & (col('Country').isNotNull()) & (col('SportName').isNotNull()) )

# display(atheletes_df_renammed.groupBy('Country').agg(count('PlayerName').alias('count')).orderBy(col('count').desc()).limit(20))
# display(atheletes_df_renammed.groupBy('SportName').agg(count('PlayerName').alias('count')).limit(20).orderBy(col('count').desc()))


print(atheletes)