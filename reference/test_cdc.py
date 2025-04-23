# Databricks notebook source
spark.sql(f"CREATE DATABASE IF NOT EXISTS test")

spark.sql("DROP TABLE IF EXISTS my_table")

spark.sql("""
CREATE TABLE IF NOT EXISTS test.my_table (
    id INT,
    name STRING,
    updated_at TIMESTAMP
) USING DELTA
TBLPROPERTIES (
    delta.enableChangeDataFeed = true
)
""")




# COMMAND ----------

# Example of inserting data
spark.sql("INSERT INTO my_table VALUES (1, 'Alice', current_timestamp())")
spark.sql("INSERT INTO my_table VALUES (2, 'Bob', current_timestamp())")

# Example of updating data
spark.sql("UPDATE my_table SET name = 'Alice Smith' WHERE id = 1")

# Example of deleting data
spark.sql("DELETE FROM my_table WHERE id = 2")


# COMMAND ----------

from pyspark.sql import functions as F

latest_commit = spark.read \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .table("milliman_data_lake.db_bronze.ccda_docs") \
    .agg(F.max("_commit_timestamp")) \
    .collect()[0][0]

earliest_commit = spark.read \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .table("milliman_data_lake.db_bronze.ccda_docs") \
    .agg(F.min("_commit_timestamp")) \
    .collect()[0][0]

# if latest_commit is None:
#     print("No changes found.")
# else:
#     print(latest_commit)

print(latest_commit)
print(earliest_commit)
