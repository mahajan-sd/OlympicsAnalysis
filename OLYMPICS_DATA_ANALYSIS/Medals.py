# Databricks notebook source
# Medals 

# COMMAND ----------

# MAGIC %run ../OLYMPICS_DATA_ANALYSIS/Common/Constants

# COMMAND ----------

Medals = inputPath.files['Medals']

Medals_df = spark.read.format('com.crealytics.spark.excel').option('header', 'true').option('inferSchema', 'true').load(Medals)

# COMMAND ----------

gold = Medals_df.withColumn('Medal', lit('Gold')).withColumn('Count', col('Gold')).drop('gold', 'silver', 'bronze', 'Total', 'Rank by Total')

display( gold.groupBy('Team/NOC', 'Medal').agg(sum('Count'))) 

silver = Medals_df.withColumn('Medal', lit('Silver')).withColumn('Count', col('Silver')).drop('gold', 'silver', 'bronze', 'Total', 'Rank by Total')

display( silver.groupBy('Team/NOC', 'Medal').agg(sum('Count'))) 

bronze = Medals_df.withColumn('Medal', lit('Bronze')).withColumn('Count', col('Bronze')).drop('gold', 'silver', 'bronze', 'Total', 'Rank by Total')

display( bronze.groupBy('Team/NOC', 'Medal').agg(sum('Count'))) 

# COMMAND ----------

display(Medals_df.groupBy('Team/NOC').agg(sum('Total').alias('Rank')).orderBy(col('Rank').desc()).limit(35))
