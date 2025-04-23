# Databricks notebook source
# DBTITLE 1,Set Variables
s3_bucket = 's3://millimandatalake/'
catalog = 'milliman_data_lake'
db_silver = 'db_silver'
db_location = f'{s3_bucket}/{db_silver}'

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
        , provider_fname STRING
        , provider_lname STRING
        , provider_org STRING
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
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{db_silver}.medical_claims_claim (
        medical_claim_id STRING
        , SequenceNumber INT
        , ClaimID STRING
        , LineNum INT
        , ContractID STRING
        , MemberID STRING
        , FromDate DATE
        , ToDate DATE
        , AdmitDate DATE
        , DischDate DATE
        , Inpatient_flag INT
        , PaidDate DATE
        , RevCode INT
        , HCPCS STRING
        , Modifier STRING
        , Modifier2 STRING
        , srcPOS INT
        , POS INT
        , srcSpecialty INT
        , Specialty STRING
        , EncounterFlag STRING
        , ProviderID STRING
        , BillType INT
        , AdmitSource STRING
        , AdmitType STRING
        , Billed DOUBLE
        , Allowed DOUBLE
        , Paid DOUBLE
        , COB DOUBLE
        , Copay DOUBLE
        , Coinsurance DOUBLE
        , Deductible DOUBLE
        , PatientPay DOUBLE
        , Days INT
        , Units FLOAT
        , ICDVersion INT
        , DischargeStatus INT
        , AdmitDiag STRING
        , ICD9Diags STRING
        , ICD9Procs STRING
        , OON STRING
        , srcLOB STRING
        , LOB STRING
        , srcProduct INT
        , Product STRING
        , GroupID STRING
        , UserDefNum2 DOUBLE
        , UserDefNum3 DOUBLE
        , YearMo INT
        , PaidMo INT
        , MSA INT
        , ExclusionCode STRING
        , PartialYearGroup INT
        , MedicareCovered INT
        , HCGIndicator STRING
        , Industry STRING
        , HCG_DRG STRING
        , HCG_ErrorCode STRING
        , CaseAdmitID STRING
        , MR_Line STRING
        , MR_Line_Det STRING
        , MR_Line_Case STRING
        , PBP_Line STRING
        , MA_Line STRING
        , MA_Line_Det STRING
        , Spec_Type STRING
        , FacilityCaseID STRING
        , TelehealthFlag STRING
        , MR_Cases_Admits INT
        , MR_Units_Days FLOAT
        , MR_Procs INT
        , MR_Cases_Admits_PatientPay INT
        , MR_Units_Days_PatientPay INT
        , MR_Procs_PatientPay FLOAT
        , MR_Billed DOUBLE
        , MR_Allowed DOUBLE
        , MR_Paid DOUBLE
        , MR_Copay DOUBLE
        , MR_Coinsurance DOUBLE
        , MR_Deductible DOUBLE
        , MR_PatientPay DOUBLE
        , PBP_Cases_Admits INT
        , PBP_Cases_Admits_PatientPay INT
        , ServiceType STRING
        , RVUAdjustedUnits FLOAT
        , MedicareInclude DOUBLE
        , MedicareAllowedNtnwd DOUBLE
        , MedicareAllowedArea DOUBLE
        , MedicareAllowedFull DOUBLE
        , MedicareFeeSchedule STRING
        , MedicareAPC_DRG_HCPCS STRING
        , MedicareStatusLookup STRING
        , MedicareAdjustedUnits FLOAT
        , MedicareAdjudicated INT
        , MedicareOutlier DOUBLE
        , MedicareDSH_UCP DOUBLE
        , MedicareCapIME DOUBLE
        , MedicareOpIME DOUBLE
        , RVUInclude INT
        , RVUFinalRVUs DOUBLE
        , RVUAdjudicated INT
        , RVUUnitCost DOUBLE
        , RVUImputeBasis STRING
        , RVUFinalRVUStep INT
        , WorkRVUs DOUBLE
        , PracticeRVUs DOUBLE
        , MalpracticeRVUs DOUBLE
        , MedicareHCGInclude INT
        , ContributorID STRING
        , HCG_GRVU_Include INT
        , HCG_Quality_Indicator INT
        , DRG_Quality_Indicator INT
        , Specialty_Quality_Indicator INT
        , Year INT
        , part INT
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

# DBTITLE 1,Create Medical Claims Diagnosis silver table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{db_silver}.medical_claims_diagnosis (
        condition_occurrence_id STRING
        , medical_claim_id STRING
        , ClaimID STRING
        , MemberID STRING
        , ProviderID STRING
        , condition_start_date DATE
        , condition_end_date DATE
        , ICDVersion INT
        , Diagnosis_Num INT
        , Diagnosis_Type STRING
        , Diagnosis_Code STRING
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

# DBTITLE 1,Create Medical Claims Procedure silver table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{db_silver}.medical_claims_procedure (
        procedure_occurrence_id STRING
        , medical_claim_id STRING
        , MemberID STRING
        , procedure_start_date DATE
        , procedure_end_date DATE
        , modifier_concept_id STRING
        , quantity FLOAT
        , ProviderID STRING
        , ClaimID STRING
        , ICDVersion INT
        , Procedure_Num INT
        , Procedure_Code STRING
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

# DBTITLE 1,Create Prescription Claims Claim silver table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{db_silver}.prescription_claim (
        prescription_claim_id STRING
        , SequenceNumber INT
        , ClaimID STRING
        , ContractID STRING
        , MemberID STRING
        , FromDate DATE
        , PaidDate DATE
        , NDC STRING
        , EncounterFlag STRING
        , MedicalCovered STRING
        , ProviderID STRING
        , MailOrder STRING
        , IngredientCost DOUBLE
        , DispensingFee DOUBLE
        , Billed DOUBLE
        , Allowed DOUBLE
        , Paid DOUBLE
        , COB DOUBLE
        , Copay DOUBLE
        , Coinsurance DOUBLE
        , Deductible DOUBLE
        , PatientPay DOUBLE
        , Units FLOAT
        , DaysSupply INT
        , QuantityDispensed FLOAT
        , OON STRING
        , srcLOB STRING
        , LOB STRING
        , srcProduct INT
        , Product STRING
        , GroupID STRING
        , UserDefNum3 INT
        , YearMo INT
        , PaidMo INT
        , MSA INT
        , ExclusionCode STRING
        , PartialYearGroup INT
        , MedicareCovered INT
        , HCGIndicator STRING
        , Industry STRING
        , CaseAdmitID STRING
        , MR_Line STRING
        , MR_Line_Det STRING
        , MR_Line_Case STRING
        , PBP_Line STRING
        , MA_Line STRING
        , MA_Line_Det STRING
        , MR_Units_Days FLOAT
        , MR_Procs INT
        , MR_Scripts_30Day INT
        , MR_Units_Days_PatientPay FLOAT
        , MR_Procs_PatientPay INT
        , MR_Billed DOUBLE
        , MR_Allowed DOUBLE
        , MR_Paid DOUBLE
        , MR_Copay DOUBLE
        , MR_Coinsurance DOUBLE
        , MR_Deductible DOUBLE
        , MR_PatientPay DOUBLE
        , RVUInclude INT
        , RVUFinalRVUs DOUBLE
        , RVUAdjudicated INT
        , RVUImputeBasis STRING
        , RVUFinalRVUStep INT
        , RVUAdjustedUnits FLOAT
        , ContributorID STRING
        , HCG_GRVU_Include INT
        , HCG_Quality_Indicator INT
        , DRG_Quality_Indicator INT
        , Specialty_Quality_Indicator INT
        , Year INT
        , part INT
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

