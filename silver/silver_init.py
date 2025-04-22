# Databricks notebook source
# DBTITLE 1,Set Variables
s3_bucket = 's3://millimandatalake/'
catalog = 'milliman_data_lake'
db_silver = 'db_silver'
db_location = f'{s3_bucket}/{db_silver}'

tbl_silver_claims_claim = 'medical_claims_claim'
tbl_silver_rx_patients = 'rx_claims_claim'

# COMMAND ----------

# DBTITLE 1,Create silver database
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{db_silver} MANAGED LOCATION '{db_location}'")

# COMMAND ----------

# DBTITLE 1,Create CCDA Patient silver table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{db_silver}.ccda_docs_patient (
        patient_id STRING
        , document_id STRING
        , document_type STRING
        , effective_time TIMESTAMP
        , document_title STRING
        , patient_fname STRING
        , patient_lname STRING
        , gender_code STRING
        , gender STRING
        , race STRING
        , ethnicity STRING
        , birth_time STRING
        , birth_date TIMESTAMP
        , street_address STRING
        , city STRING
        , state STRING
        , zip_code INT
        , country STRING
        , home_phone STRING
        , mobile_phone STRING
        , marital_code STRING
        , marital_status STRING
        , religion STRING
        , language STRING
        , provider_name STRING
        , provider_phone STRING
        , provider_address STRING
        , provider_city STRING
        , provider_state STRING
        , provider_zip_code INT
        , provider_country STRING
        , problem_count INT
        , medication_count INT
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

# DBTITLE 1,Create CCDA Medication silver table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{db_silver}.ccda_docs_medication (
        medication_id STRING
        , patient_id STRING
        , document_id STRING
        , drug_concept_id STRING
        , drug_concept_value STRING
        , drug_source_value STRING
        , drug_source_concept_id STRING
        , drug_exposure_start_date TIMESTAMP
        , drug_exposure_end_date TIMESTAMP
        , dose_unit_concept_id STRING
        , dose_unit_source_value FLOAT
        , route_concept_id STRING
        , route_source_value STRING
        , drug_supply_quantity FLOAT
        , drug_supply_refill FLOAT
        , provider_id STRING
        , provider_fname STRING
        , provider_lname STRING
        , provider_entry_date TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

# DBTITLE 1,Create CCDA Problem silver table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{db_silver}.ccda_docs_problem (
        problem_id STRING
        , patient_id STRING
        , document_id STRING
        , concern_type_code STRING
        , concern_type_name STRING
        , problem_status STRING
        , condition_concept_id STRING
        , condition_concept_value STRING
        , condition_start_date TIMESTAMP
        , condition_end_date TIMESTAMP
        , condition_type_concept_id STRING
        , condition_source_value STRING
        , condition_source_concept_id STRING
        , provider_id STRING
        , provider_fname STRING
        , provider_lname STRING
        , provider_entry_date TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

# DBTITLE 1,Create Medical Claims Claim silver table


# COMMAND ----------

# DBTITLE 1,Create Medical Claims Diagnosis silver table


# COMMAND ----------

# DBTITLE 1,Create Medical Claims Procedure silver table


# COMMAND ----------

# DBTITLE 1,Create Prescription Claims Claim silver table


# COMMAND ----------

# DBTITLE 1,For Testing Purposes - DELETE
# spark.sql(f"drop table if exists {catalog}.{db_silver}.{tbl_bronze_ccda}")

# spark.sql(f"drop database if exists {catalog}.{db_silver}")

# spark.sql(f"TRUNCATE TABLE {catalog}.{db_silver}.{tbl_bronze_ccda}")
