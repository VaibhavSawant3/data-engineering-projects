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
    SELECT DISTINCT(Date_ID)
    FROM parquet.`abfss://silver@carvaibhavdatalake.dfs.core.windows.net/carsales`
    ''')

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_model Sink Initial and Incremental (Just bring the schema if table NOT EXISTS)

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_date'):

    df_sink = spark.sql('''
    SELECT dim_date_key, Date_ID
    FROM cars_catalog.gold.dim_date
    ''')

else:

    df_sink = spark.sql('''
    SELECT 1 AS dim_date_key, Date_ID
    FROM parquet.`abfss://silver@carvaibhavdatalake.dfs.core.windows.net/carsales`
    WHERE 1=0
    ''')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Filtering new records and old records

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src['Date_ID'] == df_sink['Date_ID'], 'left').select(df_src['Date_ID'],df_sink['dim_date_key'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## df_filter_old

# COMMAND ----------

df_filter_old = df_filter.filter(col('dim_date_key').isNotNull())

# COMMAND ----------

# MAGIC %md 
# MAGIC ## df_filter_new

# COMMAND ----------

df_filter_new = df_filter.filter(col('dim_date_key').isNull()).select(df_src['Date_ID'])

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
    max_value_df = spark.sql('SELECT MAX(dim_date_key) FROM cars_catalog.gold.dim_date') 
    max_value = max_value_df.collect()[0][0]+1

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Surrogate key column and ADD the max surrogate key**

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_date_key',max_value+monotonically_increasing_id())

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
if spark.catalog.tableExists('cars_catalog.gold.dim_date'):
    delta_tbl = DeltaTable.forPath(spark, 'abfss://gold@carvaibhavdatalake.dfs.core.windows.net/dim_date')

    delta_tbl.alias('trg').merge(df_final.alias('src'), "trg.dim_date_key = src.dim_date_key")\
                         .whenMatchedUpdateAll()\
                         .whenNotMatchedInsertAll()\
                         .execute()


#Intial Run
else:
    df_final.write.format('delta')\
         .mode('overwrite')\
         .option('path', 'abfss://gold@carvaibhavdatalake.dfs.core.windows.net/dim_date')\
         .saveAsTable('cars_catalog.gold.dim_date')   

# COMMAND ----------

