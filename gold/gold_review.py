# Databricks notebook source
# MAGIC %md
# MAGIC # Instructions
# MAGIC 1. Use this notebook to review the OMOP CDM.
# MAGIC 2. In the cell titled `Choose Person to Review`, set the person_id/MemberID to the `person_lookup` variable.
# MAGIC 3. Click `Run All` to retrieve information on that person from each of the OMOP tables.

# COMMAND ----------

# DBTITLE 1,Set Variables
catalog = "milliman_data_lake"
db_gold = "db_gold"

# COMMAND ----------

# DBTITLE 1,Choose Person to Review
person_lookup = '057b56kq-b724-7sb0-h3c8-geul36s33db3'

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

# DBTITLE 1,Providers
display(
    spark.sql(f"""
        with ctePerson as (
            select explode(split(provider_id, ',')) as providers
            from {catalog}.{db_gold}.person
            where person_id = '{person_lookup}'
        )
        select * 
        from {catalog}.{db_gold}.provider as pr
            inner join ctePerson as pe on pr.provider_id = pe.providers
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
        with cteVisits as (
            select visit_occurrence_id
            from {catalog}.{db_gold}.visit_occurrence
            where person_id = '{person_lookup}'
        )
        , cteDrugs as (
            select drug_exposure_id
            from {catalog}.{db_gold}.drug_exposure
            where person_id = '{person_lookup}'
        )
        select * 
        from {catalog}.{db_gold}.cost as c
            inner join cteVisits as v on c.cost_event_id = v.visit_occurrence_id and c.cost_domain_id = 'visit'

        union
        
        select * 
        from {catalog}.{db_gold}.cost as c
            inner join cteDrugs as d on c.cost_event_id = d.drug_exposure_id and c.cost_domain_id = 'drug'
    """)
)
