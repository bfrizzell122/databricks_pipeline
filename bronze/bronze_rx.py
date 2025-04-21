# Databricks notebook source
# MAGIC %md
# MAGIC # Requirements
# MAGIC 1. 
# MAGIC
# MAGIC #Assumptions
# MAGIC 1. 
# MAGIC
# MAGIC # Improvements
# MAGIC 1. 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Set Variables
bucket_name = 'millimandatalake'
file_path = 'raw/rx/'
full_path = f"s3://{bucket_name}/{file_path}*"
catalog = "milliman_data_lake"
database = "db_bronze"
table = "rx_claims"
checkpoint_location = f"s3://{bucket_name}/_checkpoints/{database}/{table}/"

# COMMAND ----------

# DBTITLE 1,Read Prescription Claims files
from pyspark.sql.functions import expr

df_rx_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("pathGlobFilter", "*.csv")
    .load(full_path)
    .select(
        "*",
        expr("current_timestamp()").alias("created_at")
    )
)

df_rx_stream.writeStream \
    .format("delta") \
    .option("checkpointLocation", checkpoint_location) \
    .outputMode("append") \
    .trigger(once=True) \
    .table(f"{catalog}.{database}.{table}")
