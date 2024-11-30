# Databricks notebook source
# Data Analysis 

#Goal
# To cleanse the data and preapare it for dahshboards
# Create Dashbords accoring to the requirements

# COMMAND ----------

# DBTITLE 1,Comman imports
# MAGIC %run ../OLYMPICS_DATA_ANALYSIS/Common/Constants

# COMMAND ----------

# DBTITLE 1,Reading Paths
# Input Paths

atheletes = inputPath.files['Athletes']

# COMMAND ----------

# DBTITLE 1,Creating DF
# Creating dataFrames for all datasources

atheletes_df = spark.read.format('com.crealytics.spark.excel').option('header', 'true').option('inferSchema', 'true').load(atheletes)



# COMMAND ----------

# DBTITLE 1,Athletes
atheletes_df_renammed = atheletes_df.withColumnRenamed('Name', 'PlayerName').withColumnRenamed('NOC', 'Country').withColumnRenamed('Discipline', 'SportName').filter((col('PlayerName').isNotNull()) & (col('Country').isNotNull()) & (col('SportName').isNotNull()) )

print('Total Number of Athletes: ' + str(atheletes_df_renammed.count()))
print('Total Number of Different Sports PLayed: ' + str(atheletes_df_renammed.select('SportName').distinct().agg(count('*')).collect()[0][0]))
print('Total Number of Different Countries Participated: ' + str(atheletes_df_renammed.select('Country').distinct().agg(count('*')).collect()[0][0]))


# COMMAND ----------

# DBTITLE 1,Athletes
display(atheletes_df_renammed.groupBy('Country').agg(count('PlayerName').alias('count')).orderBy(col('count').desc()).limit(20))
display(atheletes_df_renammed.groupBy('SportName').agg(count('PlayerName').alias('count')).limit(20).orderBy(col('count').desc()))

