# Databricks notebook source
# DBTITLE 1,Re-Initialize Data
bucket_name = 'millimandatalake'
database = "db_bronze"

checkpoint_location = f"s3://{bucket_name}/_checkpoints/{database}/"
schema_location = f"s3://{bucket_name}/_schemas/{database}/"

dbutils.fs.rm(checkpoint_location, recurse=True)
dbutils.fs.rm(schema_location, recurse=True)
