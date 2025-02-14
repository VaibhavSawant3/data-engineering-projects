# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md 
# MAGIC # CREATE FLAG PARAMETER

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')

# COMMAND ----------

# MAGIC %md 
# MAGIC # CREATING DIMENSION Model

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Fetch Relative Column

# COMMAND ----------

df_src = spark.sql('''
    SELECT DISTINCT(Dealer_ID), Dealer_name 
    FROM parquet.`abfss://silver@carvaibhavdatalake.dfs.core.windows.net/carsales`
    ''')

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_model Sink Initial and Incremental (Just bring the schema if table NOT EXISTS)

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):

    df_sink = spark.sql('''
    SELECT dim_dealer_key, Dealer_ID, Dealer_name 
    FROM cars_catalog.gold.dim_dealer
    ''')

else:

    df_sink = spark.sql('''
    SELECT 1 AS dim_dealer_key, Dealer_ID, Dealer_name
    FROM parquet.`abfss://silver@carvaibhavdatalake.dfs.core.windows.net/carsales`
    WHERE 1=0
    ''')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Filtering new records and old records

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src['Dealer_ID'] == df_sink['Dealer_ID'], 'left').select(df_src['Dealer_ID'], df_src['Dealer_name'], df_sink['dim_dealer_key'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## df_filter_old

# COMMAND ----------

df_filter_old = df_filter.filter(col('dim_dealer_key').isNotNull())

# COMMAND ----------

# MAGIC %md 
# MAGIC ## df_filter_new

# COMMAND ----------

df_filter_new = df_filter.filter(col('dim_dealer_key').isNull()).select(df_src['Dealer_ID'], df_src['Dealer_name'])

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Surogate Key

# COMMAND ----------

# MAGIC %md 
# MAGIC **Fetch the max Surrogate Key From existing table**

# COMMAND ----------

if (incremental_flag == '0'):
    max_value = 1
else:
    max_value_df = spark.sql('SELECT MAX(dim_dealer_key) FROM cars_catalog.gold.dim_dealer') 
    max_value = max_value_df.collect()[0][0]+1

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Surrogate key column and ADD the max surrogate key**

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_dealer_key',max_value+monotonically_increasing_id())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Create Final DF - df_filter_old + df_filter_new

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD TYPE - 1 (UPSERT)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

#Incremental Run
if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
    delta_tbl = DeltaTable.forPath(spark, 'abfss://gold@carvaibhavdatalake.dfs.core.windows.net/dim_dealer')

    delta_tbl.alias('trg').merge(df_final.alias('src'), "trg.dim_dealer_key = src.dim_dealer_key")\
                         .whenMatchedUpdateAll()\
                         .whenNotMatchedInsertAll()\
                         .execute()


#Intial Run
else:
    df_final.write.format('delta')\
         .mode('overwrite')\
         .option('path', 'abfss://gold@carvaibhavdatalake.dfs.core.windows.net/dim_dealer')\
         .saveAsTable('cars_catalog.gold.dim_dealer')   

# COMMAND ----------

