# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # DATA READING

# COMMAND ----------

df = spark.read.format('parquet')\
            .option('inferSchema',True)\
            .load('abfss://bronze@carvaibhavdatalake.dfs.core.windows.net/rawdata')

# COMMAND ----------

# MAGIC %md
# MAGIC # Transform Data

# COMMAND ----------

df = df.withColumn('model_category', split(col('Model_id'), '-')[0])

# COMMAND ----------

df = df.withColumn('RevPerUnit',col('Revenue')/col('Units_Sold'))

# COMMAND ----------

# MAGIC %md
# MAGIC # AD-HOC

# COMMAND ----------

# MAGIC %md 
# MAGIC # Data Writting

# COMMAND ----------

df.write.format('parquet')\
    .mode('overwrite')\
    .option('path', 'abfss://silver@carvaibhavdatalake.dfs.core.windows.net/carsales')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC # Querying Silver Data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM parquet.`abfss://silver@carvaibhavdatalake.dfs.core.windows.net/carsales`

# COMMAND ----------

