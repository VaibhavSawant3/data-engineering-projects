# Databricks notebook source
# MAGIC %md
# MAGIC # Create Fact Table

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading Silver Data**

# COMMAND ----------

df_silver = spark.sql('''
                      SELECT * FROM 
                      parquet.`abfss://silver@carvaibhavdatalake.dfs.core.windows.net/carsales`
                      ''')

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading All the Dimmension**

# COMMAND ----------

df_model = spark.sql("SELECT * FROM cars_catalog.gold.dim_model")

df_branch = spark.sql("SELECT * FROM cars_catalog.gold.dim_branch")

df_dealer = spark.sql("SELECT * FROM cars_catalog.gold.dim_dealer")

df_date = spark.sql("SELECT * FROM cars_catalog.gold.dim_date")

# COMMAND ----------

# MAGIC %md 
# MAGIC **Bringing Keys to Fact Table**

# COMMAND ----------

df_fact = df_silver.join(df_model, df_silver['Model_id'] == df_model['Model_ID'],how='left')\
                   .join(df_branch, df_silver['Branch_ID'] == df_branch['Branch_ID'],how='left')\
                   .join(df_dealer, df_silver['Dealer_ID'] == df_dealer['Dealer_ID'],how='left')\
                   .join(df_date, df_silver['Date_ID'] == df_date['Date_ID'],how='left')\
                   .select(df_silver['revenue'],df_silver['units_sold'],df_silver['RevPerUnit'],df_model['dim_model_key'],df_branch['dim_branch_key'],df_dealer['dim_dealer_key'],df_date['dim_date_key'])
                   

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing Fact Table

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('factsales'):
    deltatbl = DeltaTable.forName(spark, 'cars_catalog.gold.factsales')
    
    deltatbl.alias('trg').merge(df_fact.alias('src'),'trg.dim_model_key = src.dim_model_key and trg.dim_branch_key = src.dim_branch_key and trg.dim_dealer_key = src.dim_dealer_key and trg.dim_date_key = src.dim_date_key')\
                        .whenMatchedUpdateAll()\
                        .whenNotMatchedInsertAll()\
                        .execute()

else:
    df_fact.write.format('delta')\
           .mode('Overwrite')\
           .option('path','abfss://gold@carvaibhavdatalake.dfs.core.windows.net/factsales')\
           .saveAsTable('cars_catalog.gold.factsales')

# COMMAND ----------

