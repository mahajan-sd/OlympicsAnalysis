# Databricks notebook source
# Notebook for Coaches Data Cleanse and display

# COMMAND ----------

# MAGIC %run ../OLYMPICS_DATA_ANALYSIS/Common/Constants

# COMMAND ----------

Coaches = inputPath.files['Coaches'] 

Coaches_df = spark.read.format('com.crealytics.spark.excel').option('header', 'true').option('inferSchema', 'true').load(Coaches)

# COMMAND ----------

Coaches_df_filtered = Coaches_df.withColumnRenamed('Name', 'CoachName').withColumnRenamed('NOC', 'Country').withColumnRenamed('Discipline', 'SportName').withColumnRenamed('Event', 'Gender').filter((col('CoachName').isNotNull()) & (col('Country').isNotNull()) & (col('SportName').isNotNull()) )


# COMMAND ----------

# DBTITLE 1,Test Cases
print('Total Number of Different Coaches: ', Coaches_df_filtered.select('CoachName').distinct().agg(count('*')).collect()[0][0])

print('Total Number of Coaches Coaching multiple Teams: ', Coaches_df_filtered.groupBy('CoachName').agg(countDistinct('SportName').alias('cnt')).filter(col('cnt')>1).count())

# COMMAND ----------

display( Coaches_df_filtered.groupBy('Country').agg(countDistinct('CoachName').alias('Coach_Count')).orderBy(col('Coach_count').desc()).limit(20))



# COMMAND ----------

display( Coaches_df_filtered.groupBy('SportName').agg(countDistinct('CoachName').alias('Coach_Count')).orderBy(col('Coach_count').desc()).limit(20))
