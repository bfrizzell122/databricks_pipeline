# Databricks notebook source
# DBTITLE 1,Set Variables
catalog = "milliman_data_lake"
db_gold = "db_gold"

# COMMAND ----------

# DBTITLE 1,Choose Person to Review
person_lookup = '0b28oik4-0280-7rbo-hd6r-i8v10m92yaw2'

# COMMAND ----------

# DBTITLE 1,Person
display(
    spark.sql(f"""
        select * 
        from {catalog}.{db_gold}.person
        where person_id = '{person_lookup}'
    """)
)

# COMMAND ----------

# DBTITLE 1,Visits
display(
    spark.sql(f"""
        select * 
        from {catalog}.{db_gold}.visit_occurrence
        where person_id = '{person_lookup}'
    """)
)

# COMMAND ----------

# DBTITLE 1,Conditions
display(
    spark.sql(f"""
        select * 
        from {catalog}.{db_gold}.condition_occurrence
        where person_id = '{person_lookup}'
    """)
)

# COMMAND ----------

# DBTITLE 1,Procedures
display(
    spark.sql(f"""
        select * 
        from {catalog}.{db_gold}.procedure_occurrence
        where person_id = '{person_lookup}'
    """)
)

# COMMAND ----------

# DBTITLE 1,Drugs
display(
    spark.sql(f"""
        select * 
        from {catalog}.{db_gold}.drug_exposure
        where person_id = '{person_lookup}'
    """)
)

# COMMAND ----------

# DBTITLE 1,Costs
display(
    spark.sql(f"""
        select * 
        from {catalog}.{db_gold}.cost
        where person_id = '{person_lookup}'
    """)
)
