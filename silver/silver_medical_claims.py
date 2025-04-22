# Databricks notebook source
# MAGIC %md
# MAGIC # Requirements
# MAGIC 1. 
# MAGIC
# MAGIC # Assumptions
# MAGIC 1. 
# MAGIC
# MAGIC # Improvements
# MAGIC 1. 

# COMMAND ----------

# DBTITLE 1,Set Variables
catalog = "milliman_data_lake"
database = "db_bronze"
table = "medical_claims"

# COMMAND ----------

display(spark.sql(f"select * from {catalog}.{database}.{table}"))
