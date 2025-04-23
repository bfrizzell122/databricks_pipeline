# Databricks notebook source
# MAGIC %md
# MAGIC # Requirements
# MAGIC 1. Import medical claims data from CSV file(s).
# MAGIC
# MAGIC #Assumptions
# MAGIC 1. There is no set schema for incoming files. Therefore, I will use schema evolution to merge schemas between differing files and save any new columns in a "rescued_data" column.
# MAGIC
# MAGIC # Improvements
# MAGIC 1. Enforce schema for incoming files by filtering out any records where the "rescued_data" column is not null and save these records to a separate audit table for triaging.

# COMMAND ----------

# DBTITLE 1,Set Variables
bucket_name = 'millimandatalake'
file_path = 'raw/rx/'
full_path = f"s3://{bucket_name}/{file_path}*"
catalog = "milliman_data_lake"
database = "db_bronze"
table = "rx_claims"
checkpoint_location = f"s3://{bucket_name}/_checkpoints/{database}/{table}/"
schema_location = f"s3://{bucket_name}/_schemas/{database}/{table}/"

# COMMAND ----------

# DBTITLE 1,Read Prescription Claims files
from pyspark.sql.functions import expr

df_claims_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.includeExistingFiles", "true")  
    .option("cloudFiles.schemaLocation", schema_location)
    .option("pathGlobFilter", "*.csv")
    .option("rescuedDataColumn", "_rescued_data")
    .load(full_path)
    .select(
        "*",
        expr("current_timestamp()").alias("created_at")
    )
)

df_claims_stream.writeStream \
    .format("delta") \
    .option("checkpointLocation", checkpoint_location) \
    .option("mergeSchema", "true") \
    .outputMode("append") \
    .trigger(once=True) \
    .table(f"{catalog}.{database}.{table}")
