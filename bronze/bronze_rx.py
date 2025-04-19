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

# Basic file listing using dbutils
files = dbutils.fs.ls("s3://millimandatalake/raw/rx")
display(files)


# COMMAND ----------

# Reading a specific file using spark.read
path_claims = "s3://millimandatalake/raw/rx/data_engineer_exam_rx_final.csv"

df = spark.read.format("csv").option("header", "true").load(path_claims)
display(df)
