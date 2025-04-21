# Databricks notebook source
# DBTITLE 1,Set Variables
s3_bucket = 's3://millimandatalake/'
catalog = 'milliman_data_lake'
db_bronze = 'db_bronze'
db_location = f'{s3_bucket}/{db_bronze}'
tbl_bronze_ccda = 'ccda_docs'
tbl_bronze_claims = 'medical_claims'
tbl_bronze_rx = 'rx_claims'

# COMMAND ----------

# DBTITLE 1,Create bronze database
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{db_bronze} MANAGED LOCATION '{db_location}'")

# COMMAND ----------

# DBTITLE 1,Create CCDA bronze table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{db_bronze}.{tbl_bronze_ccda} (
        patient_id STRING
        , document_id STRING
        , file_modification_time TIMESTAMP
        , file_size_bytes LONG
        , file_path STRING
        , file_name STRING
        , xml_string STRING
        , created_at TIMESTAMP NOT NULL
    ) 
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

# DBTITLE 1,Create Claims bronze table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{db_bronze}.{tbl_bronze_claims} (
        SequenceNumber STRING
        , ClaimID STRING
        , LineNum STRING
        , ContractID STRING
        , MemberID STRING
        , FromDate STRING
        , ToDate STRING
        , AdmitDate STRING
        , DischDate STRING
        , PaidDate STRING
        , RevCode STRING
        , HCPCS STRING
        , Modifier STRING
        , Modifier2 STRING
        , srcPOS STRING
        , POS STRING
        , srcSpecialty STRING
        , Specialty STRING
        , EncounterFlag STRING
        , ProviderID STRING
        , BillType STRING
        , AdmitSource STRING
        , AdmitType STRING
        , Billed STRING
        , Allowed STRING
        , Paid STRING
        , COB STRING
        , Copay STRING
        , Coinsurance STRING
        , Deductible STRING
        , PatientPay STRING
        , Days STRING
        , Units STRING
        , ICDVersion STRING
        , DischargeStatus STRING
        , AdmitDiag STRING
        , ICDDiag1 STRING
        , ICDDiag2 STRING
        , ICDDiag3 STRING
        , ICDDiag4 STRING
        , ICDDiag5 STRING
        , ICDDiag6 STRING
        , ICDDiag7 STRING
        , ICDDiag8 STRING
        , ICDDiag9 STRING
        , ICDDiag10 STRING
        , ICDDiag11 STRING
        , ICDDiag12 STRING
        , ICDDiag13 STRING
        , ICDDiag14 STRING
        , ICDDiag15 STRING
        , ICDDiag16 STRING
        , ICDDiag17 STRING
        , ICDDiag18 STRING
        , ICDDiag19 STRING
        , ICDDiag20 STRING
        , ICDDiag21 STRING
        , ICDDiag22 STRING
        , ICDDiag23 STRING
        , ICDDiag24 STRING
        , ICDDiag25 STRING
        , ICDDiag26 STRING
        , ICDDiag27 STRING
        , ICDDiag28 STRING
        , ICDDiag29 STRING
        , ICDDiag30 STRING
        , ICDProc1 STRING
        , ICDProc2 STRING
        , ICDProc3 STRING
        , ICDProc4 STRING
        , ICDProc5 STRING
        , ICDProc6 STRING
        , ICDProc7 STRING
        , ICDProc8 STRING
        , ICDProc9 STRING
        , ICDProc10 STRING
        , ICDProc11 STRING
        , ICDProc12 STRING
        , ICDProc13 STRING
        , ICDProc14 STRING
        , ICDProc15 STRING
        , ICDProc16 STRING
        , ICDProc17 STRING
        , ICDProc18 STRING
        , ICDProc19 STRING
        , ICDProc20 STRING
        , ICDProc21 STRING
        , ICDProc22 STRING
        , ICDProc23 STRING
        , ICDProc24 STRING
        , ICDProc25 STRING
        , ICDProc26 STRING
        , ICDProc27 STRING
        , ICDProc28 STRING
        , ICDProc29 STRING
        , ICDProc30 STRING
        , ICD9Diags STRING
        , ICD9Procs STRING
        , OON STRING
        , srcLOB STRING
        , LOB STRING
        , srcProduct STRING
        , Product STRING
        , GroupID STRING
        , UserDefNum2 STRING
        , UserDefNum3 STRING
        , YearMo STRING
        , PaidMo STRING
        , MSA STRING
        , ExclusionCode STRING
        , PartialYearGroup STRING
        , MedicareCovered STRING
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
        , MR_Cases_Admits STRING
        , MR_Units_Days STRING
        , MR_Procs STRING
        , MR_Cases_Admits_PatientPay STRING
        , MR_Units_Days_PatientPay STRING
        , MR_Procs_PatientPay STRING
        , MR_Billed STRING
        , MR_Allowed STRING
        , MR_Paid STRING
        , MR_Copay STRING
        , MR_Coinsurance STRING
        , MR_Deductible STRING
        , MR_PatientPay STRING
        , PBP_Cases_Admits STRING
        , PBP_Cases_Admits_PatientPay STRING
        , ServiceType STRING
        , RVUAdjustedUnits STRING
        , MedicareInclude STRING
        , MedicareAllowedNtnwd STRING
        , MedicareAllowedArea STRING
        , MedicareAllowedFull STRING
        , MedicareFeeSchedule STRING
        , MedicareAPC_DRG_HCPCS STRING
        , MedicareStatusLookup STRING
        , MedicareAdjustedUnits STRING
        , MedicareAdjudicated STRING
        , MedicareOutlier STRING
        , MedicareDSH_UCP STRING
        , MedicareCapIME STRING
        , MedicareOpIME STRING
        , RVUInclude STRING
        , RVUFinalRVUs STRING
        , RVUAdjudicated STRING
        , RVUUnitCost STRING
        , RVUImputeBasis STRING
        , RVUFinalRVUStep STRING
        , WorkRVUs STRING
        , PracticeRVUs STRING
        , MalpracticeRVUs STRING
        , MedicareHCGInclude STRING
        , ContributorID STRING
        , HCG_GRVU_Include STRING
        , HCG_Quality_Indicator STRING
        , DRG_Quality_Indicator STRING
        , Specialty_Quality_Indicator STRING
        , Year STRING
        , part STRING
        , created_at TIMESTAMP NOT NULL
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

# DBTITLE 1,Create Rx bronze table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{db_bronze}.{tbl_bronze_rx} (
        SequenceNumber STRING
        , ClaimID STRING
        , ContractID STRING
        , MemberID STRING
        , FromDate STRING
        , PaidDate STRING
        , NDC STRING
        , EncounterFlag STRING
        , MedicalCovered STRING
        , ProviderID STRING
        , MailOrder STRING
        , IngredientCost STRING
        , DispensingFee STRING
        , Billed STRING
        , Allowed STRING
        , Paid STRING
        , COB STRING
        , Copay STRING
        , Coinsurance STRING
        , Deductible STRING
        , PatientPay STRING
        , Units STRING
        , DaysSupply STRING
        , QuantityDispensed STRING
        , OON STRING
        , srcLOB STRING
        , LOB STRING
        , srcProduct STRING
        , Product STRING
        , GroupID STRING
        , UserDefNum3 STRING
        , YearMo STRING
        , PaidMo STRING
        , MSA STRING
        , ExclusionCode STRING
        , PartialYearGroup STRING
        , MedicareCovered STRING
        , HCGIndicator STRING
        , Industry STRING
        , CaseAdmitID STRING
        , MR_Line STRING
        , MR_Line_Det STRING
        , MR_Line_Case STRING
        , PBP_Line STRING
        , MA_Line STRING
        , MA_Line_Det STRING
        , MR_Units_Days STRING
        , MR_Procs STRING
        , MR_Scripts_30Day STRING
        , MR_Units_Days_PatientPay STRING
        , MR_Procs_PatientPay STRING
        , MR_Billed STRING
        , MR_Allowed STRING
        , MR_Paid STRING
        , MR_Copay STRING
        , MR_Coinsurance STRING
        , MR_Deductible STRING
        , MR_PatientPay STRING
        , RVUInclude STRING
        , RVUFinalRVUs STRING
        , RVUAdjudicated STRING
        , RVUImputeBasis STRING
        , RVUFinalRVUStep STRING
        , RVUAdjustedUnits STRING
        , ContributorID STRING
        , HCG_GRVU_Include STRING
        , HCG_Quality_Indicator STRING
        , DRG_Quality_Indicator STRING
        , Specialty_Quality_Indicator STRING
        , Year STRING
        , part STRING
        , created_at TIMESTAMP NOT NULL
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

# DBTITLE 1,For Testing Purposes - DELETE
# spark.sql(f"drop table if exists {catalog}.{db_bronze}.{tbl_bronze_ccda}")

# spark.sql(f"drop database if exists {catalog}.{db_bronze}")

# spark.sql(f"TRUNCATE TABLE {catalog}.{db_bronze}.{tbl_bronze_ccda}")
