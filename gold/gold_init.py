# Databricks notebook source
# DBTITLE 1,Set Variables
s3_bucket = 's3://millimandatalake/'
catalog = 'milliman_data_lake'
db_gold = 'db_gold'
db_location = f'{s3_bucket}/{db_gold}'

# COMMAND ----------

# DBTITLE 1,Create gold database
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{db_gold} MANAGED LOCATION '{db_location}'")

# COMMAND ----------

# DBTITLE 1,Create OMOP Person gold table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{db_gold}.PERSON (
        person_id STRING
        , gender_concept_id STRING
        , year_of_birth INT
        , month_of_birth INT
        , day_of_birth INT
        , birth_datetime TIMESTAMP
        , race_concept_id STRING
        , ethnicity_concept_id STRING
        , location_id STRING
        , provider_id STRING
        , care_site_id STRING
        , person_source_value STRING
        , gender_source_value STRING
        , gender_source_concept_id STRING
        , race_source_value STRING
        , race_source_concept_id STRING
        , ethnicity_source_value STRING
        , ethnicity_source_concept_id STRING
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")


# COMMAND ----------

# DBTITLE 1,Create OMOP Provider gold table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{db_gold}.PROVIDER (
        provider_id STRING
        , provider_name STRING
        , npi STRING
        , dea STRING
        , specialty_concept_id STRING
        , care_site_id STRING
        , year_of_birth INT
        , gender_concept_id STRING
        , provider_source_value STRING
        , specialty_source_value STRING
        , specialty_source_concept_id STRING
        , gender_source_value STRING
        , gender_source_concept_id STRING
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")


# COMMAND ----------

# DBTITLE 1,Create OMOP Condition Occurence gold table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{db_gold}.CONDITION_OCCURRENCE (
        condition_occurrence_id STRING
        , person_id STRING
        , condition_concept_id STRING
        , condition_start_date DATE
        , condition_start_datetime TIMESTAMP
        , condition_end_date DATE
        , condition_end_datetime TIMESTAMP
        , condition_type_concept_id STRING
        , condition_status_concept_id STRING
        , stop_reason STRING
        , provider_id STRING
        , visit_occurrence_id STRING
        , visit_detail_id STRING
        , condition_source_value STRING
        , condition_source_concept_id STRING
        , condition_status_source_value STRING
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")


# COMMAND ----------

# DBTITLE 1,Create OMOP Drug Exposure gold table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{db_gold}.DRUG_EXPOSURE (
        drug_exposure_id STRING
        , person_id STRING
        , drug_concept_id STRING
        , drug_exposure_start_date DATE
        , drug_exposure_start_datetime TIMESTAMP
        , drug_exposure_end_date DATE
        , drug_exposure_end_datetime TIMESTAMP
        , verbatim_end_date DATE
        , drug_type_concept_id STRING
        , stop_reason STRING
        , refills INT
        , quantity DOUBLE
        , days_supply INT
        , sig STRING
        , route_concept_id STRING
        , lot_number STRING
        , provider_id STRING
        , visit_occurrence_id STRING
        , visit_detail_id STRING
        , drug_source_value STRING
        , drug_source_concept_id STRING
        , route_source_value STRING
        , dose_unit_source_value FLOAT
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")


# COMMAND ----------

# DBTITLE 1,Create OMOP Procedure Occurrence gold table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{db_gold}.PROCEDURE_OCCURRENCE (
        procedure_occurrence_id STRING
        , person_id STRING
        , procedure_concept_id STRING
        , procedure_start_date DATE
        , procedure_start_datetime TIMESTAMP
        , procedure_end_date DATE
        , procedure_end_datetime TIMESTAMP
        , procedure_type_concept_id STRING
        , modifier_concept_id STRING
        , quantity INT
        , provider_id STRING
        , visit_occurrence_id STRING
        , visit_detail_id STRING
        , procedure_source_value STRING
        , procedure_source_concept_id STRING
        , modifier_source_value STRING
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")


# COMMAND ----------

# DBTITLE 1,Create OMOP Visit Occurrence gold table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{db_gold}.VISIT_OCCURRENCE (
        visit_occurrence_id STRING
        , person_id STRING
        , visit_concept_id INT
        , visit_start_date DATE
        , visit_start_datetime TIMESTAMP
        , visit_end_date DATE
        , visit_end_datetime TIMESTAMP
        , visit_type_concept_id STRING
        , provider_id STRING
        , care_site_id STRING
        , visit_source_value STRING
        , visit_source_concept_id STRING
        , admitted_from_concept_id STRING
        , admitted_from_source_value STRING
        , discharged_to_concept_id STRING
        , discharged_to_source_value INT
        , preceding_visit_occurrence_id STRING
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")


# COMMAND ----------

# DBTITLE 1,Create OMOP Cost gold table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{db_gold}.COST (
        cost_id STRING
        , cost_event_id STRING
        , cost_domain_id STRING
        , cost_type_concept_id STRING
        , currency_concept_id STRING
        , total_charge DOUBLE
        , total_cost DOUBLE
        , total_paid DOUBLE
        , paid_by_payer DOUBLE
        , paid_by_patient DOUBLE
        , paid_patient_copay DOUBLE
        , paid_patient_coinsurance DOUBLE
        , paid_patient_deductible DOUBLE
        , paid_by_primary DOUBLE
        , paid_ingredient_cost DOUBLE
        , paid_dispensing_fee DOUBLE
        , payer_plan_period_id STRING
        , amount_allowed DOUBLE
        , revenue_code_concept_id STRING
        , revenue_code_source_value STRING
        , drg_concept_id STRING
        , drg_source_value STRING
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

