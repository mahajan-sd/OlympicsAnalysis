# Databricks notebook source
# To cleanse and create dashboard for Entries

# COMMAND ----------

# MAGIC %run ../OLYMPICS_DATA_ANALYSIS/Common/Constants
# MAGIC

# COMMAND ----------

EntriesGender = inputPath.files['EntriesGender']

EntriesGender_df = spark.read.format('com.crealytics.spark.excel').option('header', 'true').option('inferSchema', 'true').load(EntriesGender)


# COMMAND ----------

print('Total Female participated in games: ', EntriesGender_df.agg((sum('Female'))).collect()[0][0] )

print('Total Male participated in games: ', EntriesGender_df.agg(sum('Male')).collect()[0][0] )


# COMMAND ----------

df = EntriesGender_df.groupBy('Discipline').agg(sum('Female').alias('Female_Sum'),sum('Male').alias('Male_Sum')).orderBy(col('Male_Sum').desc()).limit(20)

male_df = df.select(col("Discipline"), col("Male_Sum").alias("count"), lit("male").alias("gender"))
female_df = df.select(col("Discipline"), col("Female_Sum").alias("count"), lit("female").alias("gender"))

finalDF = male_df.union(female_df).withColumnRenamed('Discipline', 'SportName')

display(finalDF)
