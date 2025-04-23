# Databricks notebook source
# MAGIC %md
# MAGIC # Requirements
# MAGIC 1. Analyze and parse the content from the documents into their respective domains. 
# MAGIC     - Medications
# MAGIC     - Problems
# MAGIC
# MAGIC #Assumptions
# MAGIC 1. CCDA XML files will remain small and can be stored as a string in a single column.
# MAGIC 2. Data does not need to be masked.
# MAGIC 3. CCDA XML files will be the only XML files in the target folder.
# MAGIC
# MAGIC # Improvements
# MAGIC 1. In a production environment, there are several approaches that can mitigate any performance issues related to larger CCDA XML files. Options 1 & 2 would be preferred for streaming CCDA documents when we cannot retain the physical file. Option 3 is better when we can retain all the files but requires reading/parsing for every read which adds complexity and performance implications.
# MAGIC     1. Storing the XML as binary to save space and could also improve reads.
# MAGIC     2. Chunking XML data into fixed size columns.
# MAGIC     3. Storing the file path in the table and reading the data at run-time.
# MAGIC 2. For larger volumes of files to be processed per job run, I would process them in small batches using the maxFilesPerTrigger Spark option.

# COMMAND ----------

# DBTITLE 1,Set variables
bucket_name = 'millimandatalake'
file_path = 'raw/ccda/'
full_path = f"s3://{bucket_name}/{file_path}*"
catalog = "milliman_data_lake"
database = "db_bronze"
table = "ccda_docs"
checkpoint_location = f"s3://{bucket_name}/_checkpoints/{database}/{table}/"

# COMMAND ----------

# DBTITLE 1,Read CCDA files
from pyspark.sql.functions import col, expr

df_ccda_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("pathGlobFilter", "*.xml")
    .load(full_path)
    .select(
        expr("split(_metadata.file_name, '_')[0]").alias("patient_id"),
        expr("split(_metadata.file_name, '_')[1]").alias("document_id"),
        col("modificationTime").alias("file_modification_time"),
        col("length").alias("file_size_bytes"),
        col("path").alias("file_path"),
        col("_metadata.file_name").alias("file_name"),
        expr("decode(content,'utf-8')").alias("xml_string"),
        expr("current_timestamp()").alias("created_at")
    )
)

df_ccda_stream.writeStream \
    .format("delta") \
    .option("checkpointLocation", checkpoint_location) \
    .outputMode("append") \
    .trigger(once=True) \
    .table(f"{catalog}.{database}.{table}")
