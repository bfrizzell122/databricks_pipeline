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
files = dbutils.fs.ls("s3://millimandatalake/raw/claims")
display(files)

# COMMAND ----------

# Reading a specific file using spark.read
path_claims = "s3://millimandatalake/raw/claims/data_engineer_exam_claims_final.csv"

df = (spark.read.format("csv").option("header", "true").load(path_claims)).createOrReplaceTempView("claims")

df2 = spark.sql("select * from claims")

display(df2)
