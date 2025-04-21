# Databricks notebook source
# MAGIC %sql
# MAGIC INSERT INTO omop.procedure_occurrence
# MAGIC SELECT
# MAGIC   monotonically_increasing_id() AS procedure_occurrence_id,         -- Unique ID
# MAGIC   member_id AS person_id,                                           -- FK to person
# MAGIC   TO_DATE(from_date, 'yyyy-MM-dd') AS procedure_date,               -- Date of procedure
# MAGIC   TO_DATE(from_date, 'yyyy-MM-dd') AS procedure_datetime,
# MAGIC   -- Use a vocabulary mapping table here if available
# MAGIC   hcpcs AS procedure_concept_id,                                    -- Should be mapped to OMOP standard concept
# MAGIC   NULL AS procedure_type_concept_id,                                -- Set if you know the source (e.g., EHR vs claim)
# MAGIC   provider_id AS provider_id,
# MAGIC   NULL AS visit_occurrence_id,                                      -- Could link from ClaimID if visits are modeled
# MAGIC   NULL AS procedure_source_value,
# MAGIC   hcpcs AS procedure_source_concept_id,                             -- Ideally mapped
# MAGIC   NULL AS qualifier_concept_id,
# MAGIC   NULL AS modifier_concept_id
# MAGIC FROM silver.medical_claims
# MAGIC WHERE hcpcs IS NOT NULL;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example for first 5 diagnosis codes
# MAGIC INSERT INTO omop.condition_occurrence
# MAGIC SELECT
# MAGIC   monotonically_increasing_id() AS condition_occurrence_id,
# MAGIC   member_id AS person_id,
# MAGIC   TO_DATE(from_date, 'yyyy-MM-dd') AS condition_start_date,
# MAGIC   TO_DATE(from_date, 'yyyy-MM-dd') AS condition_start_datetime,
# MAGIC   NULL AS condition_end_date,
# MAGIC   NULL AS condition_end_datetime,
# MAGIC   diag_code AS condition_concept_id,                     -- Needs vocabulary mapping
# MAGIC   NULL AS condition_type_concept_id,
# MAGIC   provider_id AS provider_id,
# MAGIC   NULL AS visit_occurrence_id,
# MAGIC   diag_code AS condition_source_value,
# MAGIC   diag_code AS condition_source_concept_id,
# MAGIC   NULL AS condition_status_concept_id
# MAGIC FROM (
# MAGIC   SELECT member_id, from_date, provider_id, ICDDiag1 AS diag_code FROM silver.medical_claims WHERE ICDDiag1 IS NOT NULL
# MAGIC   UNION ALL
# MAGIC   SELECT member_id, from_date, provider_id, ICDDiag2 FROM silver.medical_claims WHERE ICDDiag2 IS NOT NULL
# MAGIC   UNION ALL
# MAGIC   SELECT member_id, from_date, provider_id, ICDDiag3 FROM silver.medical_claims WHERE ICDDiag3 IS NOT NULL
# MAGIC   UNION ALL
# MAGIC   SELECT member_id, from_date, provider_id, ICDDiag4 FROM silver.medical_claims WHERE ICDDiag4 IS NOT NULL
# MAGIC   UNION ALL
# MAGIC   SELECT member_id, from_date, provider_id, ICDDiag5 FROM silver.medical_claims WHERE ICDDiag5 IS NOT NULL
# MAGIC ) AS diag_flat;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO omop.visit_occurrence
# MAGIC SELECT
# MAGIC   monotonically_increasing_id() AS visit_occurrence_id,
# MAGIC   member_id AS person_id,
# MAGIC   TO_DATE(from_date, 'yyyy-MM-dd') AS visit_start_date,
# MAGIC   TO_DATE(from_date, 'yyyy-MM-dd') AS visit_start_datetime,
# MAGIC   TO_DATE(to_date, 'yyyy-MM-dd') AS visit_end_date,
# MAGIC   TO_DATE(to_date, 'yyyy-MM-dd') AS visit_end_datetime,
# MAGIC   CASE 
# MAGIC     WHEN pos = '21' THEN 9201  -- Inpatient Visit
# MAGIC     WHEN pos = '11' THEN 9202  -- Outpatient Visit
# MAGIC     WHEN pos = '12' THEN 9203  -- Emergency Room
# MAGIC     ELSE 0                    -- Unknown or other
# MAGIC   END AS visit_concept_id,
# MAGIC   provider_id,
# MAGIC   NULL AS care_site_id,
# MAGIC   pos AS visit_source_value,
# MAGIC   NULL AS visit_source_concept_id,
# MAGIC   NULL AS admitting_source_concept_id,
# MAGIC   admit_source AS admitting_source_value,
# MAGIC   NULL AS discharge_to_concept_id,
# MAGIC   discharge_status AS discharge_to_source_value,
# MAGIC   billtype AS preceding_visit_occurrence_id
# MAGIC FROM silver.medical_claims
# MAGIC GROUP BY member_id, claimid, from_date, to_date, pos, provider_id, admit_source, discharge_status, billtype;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO omop.drug_exposure
# MAGIC SELECT
# MAGIC   monotonically_increasing_id() AS drug_exposure_id,
# MAGIC   member_id AS person_id,
# MAGIC   TO_DATE(from_date, 'yyyy-MM-dd') AS drug_exposure_start_date,
# MAGIC   TO_DATE(from_date, 'yyyy-MM-dd') AS drug_exposure_start_datetime,
# MAGIC   TO_DATE(to_date, 'yyyy-MM-dd') AS drug_exposure_end_date,
# MAGIC   TO_DATE(to_date, 'yyyy-MM-dd') AS drug_exposure_end_datetime,
# MAGIC   hcpcs AS drug_concept_id,                        -- Should map to RxNorm (OMOP standard)
# MAGIC   NULL AS drug_type_concept_id,                    -- Type of record source (e.g., claims, EHR)
# MAGIC   NULL AS stop_reason,
# MAGIC   NULL AS refills,
# MAGIC   units AS quantity,
# MAGIC   NULL AS days_supply,
# MAGIC   NULL AS sig,
# MAGIC   provider_id,
# MAGIC   NULL AS visit_occurrence_id,
# MAGIC   hcpcs AS drug_source_value,
# MAGIC   hcpcs AS drug_source_concept_id,
# MAGIC   NULL AS route_concept_id
# MAGIC FROM silver.medical_claims
# MAGIC WHERE hcpcs IS NOT NULL AND hcpcs RLIKE '^[A-HJ-NP-Z0-9]+';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO omop.measurement
# MAGIC SELECT
# MAGIC   monotonically_increasing_id() AS measurement_id,
# MAGIC   member_id AS person_id,
# MAGIC   hcpcs AS measurement_concept_id,                  -- Must map to standard concept (e.g., LOINC, SNOMED)
# MAGIC   TO_DATE(from_date, 'yyyy-MM-dd') AS measurement_date,
# MAGIC   TO_DATE(from_date, 'yyyy-MM-dd') AS measurement_datetime,
# MAGIC   NULL AS measurement_time,
# MAGIC   NULL AS measurement_type_concept_id,
# MAGIC   NULL AS operator_concept_id,
# MAGIC   NULL AS value_as_number,
# MAGIC   NULL AS value_as_concept_id,
# MAGIC   NULL AS unit_concept_id,
# MAGIC   NULL AS range_low,
# MAGIC   NULL AS range_high,
# MAGIC   NULL AS provider_id,
# MAGIC   NULL AS visit_occurrence_id,
# MAGIC   hcpcs AS measurement_source_value,
# MAGIC   hcpcs AS measurement_source_concept_id,
# MAGIC   NULL AS unit_source_value,
# MAGIC   NULL AS value_source_value
# MAGIC FROM silver.medical_claims
# MAGIC WHERE hcpcs IS NOT NULL AND service_type = 'LAB';
# MAGIC

# COMMAND ----------

# GOLD LAYER - Create aggregated business-level Delta tables
def process_gold_layer():
    print("Processing Gold Layer...")
    
    # Create patient summary table
    spark.sql(f"""
    CREATE OR REPLACE TABLE {gold_db}.patient_summary AS
    SELECT
        p.patient_id,
        p.first_name,
        p.last_name,
        p.gender,
        p.birth_date,
        p.state,
        COUNT(DISTINCT m.medication_name) AS medication_count,
        COUNT(DISTINCT pr.problem_name) AS problem_count,
        COUNT(DISTINCT proc.procedure_name) AS procedure_count,
        current_timestamp() AS updated_at
    FROM {silver_db}.patient_demographics p
    LEFT JOIN {silver_db}.medications m ON p.patient_id = m.patient_id
    LEFT JOIN {silver_db}.problems pr ON p.patient_id = pr.patient_id
    LEFT JOIN {silver_db}.procedures proc ON p.patient_id = proc.patient_id
    GROUP BY p.patient_id, p.first_name, p.last_name, p.gender, p.birth_date, p.state
    """)
    
    # Create medication analytics
    spark.sql(f"""
    CREATE OR REPLACE TABLE {gold_db}.medication_analytics AS
    SELECT
        medication_name,
        COUNT(DISTINCT patient_id) AS patient_count,
        COUNT(*) AS prescription_count,
        current_timestamp() AS updated_at
    FROM {silver_db}.medications
    GROUP BY medication_name
    """)
    
    # Create problem/condition analytics
    spark.sql(f"""
    CREATE OR REPLACE TABLE {gold_db}.problem_analytics AS
    SELECT
        problem_name,
        COUNT(DISTINCT patient_id) AS patient_count,
        current_timestamp() AS updated_at
    FROM {silver_db}.problems
    WHERE problem_name IS NOT NULL
    GROUP BY problem_name
    """)
    
    # Create procedure analytics
    spark.sql(f"""
    CREATE OR REPLACE TABLE {gold_db}.procedure_analytics AS
    SELECT
        procedure_name,
        COUNT(DISTINCT patient_id) AS patient_count,
        COUNT(*) AS procedure_count,
        current_timestamp() AS updated_at
    FROM {silver_db}.procedures
    WHERE procedure_name IS NOT NULL
    GROUP BY procedure_name
    """)
    
    # Create demographic analytics
    spark.sql(f"""
    CREATE OR REPLACE TABLE {gold_db}.demographic_analytics AS
    SELECT
        state,
        gender,
        COUNT(DISTINCT patient_id) AS patient_count,
        current_timestamp() AS updated_at
    FROM {silver_db}.patient_demographics
    WHERE state IS NOT NULL AND gender IS NOT NULL
    GROUP BY state, gender
    """)
    
    print("Gold Layer processing completed")

