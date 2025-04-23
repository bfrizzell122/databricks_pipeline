# Databricks notebook source
# MAGIC %md
# MAGIC # Requirements
# MAGIC 1. Structure this data to feed into a Common Data Model format for consumption in a Data Warehouse / Lakehouse with a focus on FHIR/HL7
# MAGIC
# MAGIC # Assumptions
# MAGIC 1. Universe of values present in CSV files are the only values available.
# MAGIC
# MAGIC # Improvements
# MAGIC 1. Use a schema/data contract file to standardize columns/values dynamically.

# COMMAND ----------

# DBTITLE 1,Set Variables
catalog = "milliman_data_lake"
db_bronze = "db_bronze"
db_silver = "db_silver"
table = "medical_claims"

# COMMAND ----------

# DBTITLE 1,Process Medical Claims
df_medical_claims = spark.table(f"{catalog}.{db_bronze}.{table}")

icd_cols = ",".join([col for col in df_medical_claims.columns if col.startswith("ICDProc") or col.startswith("ICDDiag")])

df_medical_claims = spark.sql(f"""
    select distinct
        sha2(
            concat_ws(
                '||'
                , coalesce(ClaimID, '')
                , coalesce(LineNum, '')
            ), 256
        ) as medical_claim_id
        , try_cast(SequenceNumber as int) as SequenceNumber
        , ClaimID
        , try_cast(LineNum as int) as LineNum
        , ContractID
        , MemberID
        , make_date(
            split(FromDate, '/')[2]
            , split(FromDate, '/')[0]
            , split(FromDate, '/')[1]
        ) as FromDate
        , make_date(
            split(ToDate, '/')[2]
            , split(ToDate, '/')[0]
            , split(ToDate, '/')[1]
        ) as ToDate
        , make_date(
            split(AdmitDate, '/')[2]
            , split(AdmitDate, '/')[0]
            , split(AdmitDate, '/')[1]
        ) as AdmitDate
        , make_date(
            split(DischDate, '/')[2]
            , split(DischDate, '/')[0]
            , split(DischDate, '/')[1]
        ) as DischDate
        , iff(AdmitDate is not null or DischDate is not null, 1, 0) as Inpatient_flag
        , make_date(
            split(PaidDate, '/')[2]
            , split(PaidDate, '/')[0]
            , split(PaidDate, '/')[1]
        ) as PaidDate
        , try_cast(RevCode as int) as RevCode
        , HCPCS
        , Modifier
        , Modifier2
        , try_cast(srcPOS as int) as srcPOS
        , try_cast(POS as int) as POS
        , try_cast(srcSpecialty as int) as srcSpecialty
        , Specialty
        , EncounterFlag
        , ProviderID
        , try_cast(BillType as int) as BillType
        , AdmitSource
        , AdmitType
        , round(try_cast(Billed as double), 2) as Billed
        , round(try_cast(Allowed as double), 2) as Allowed
        , round(try_cast(Paid as double), 2) as Paid
        , round(try_cast(COB as double), 2) as COB
        , round(try_cast(Copay as double), 2) as Copay
        , round(try_cast(Coinsurance as double), 2) as Coinsurance
        , round(try_cast(Deductible as double), 2) as Deductible
        , round(try_cast(PatientPay as double), 2) as PatientPay
        , try_cast(Days as int) as Days
        , try_cast(Units as float) as Units
        , try_cast(ICDVersion as int) as ICDVersion
        , try_cast(DischargeStatus as int) as DischargeStatus
        , AdmitDiag
        , {icd_cols}
        , ICD9Diags
        , ICD9Procs
        , OON
        , srcLOB
        , LOB
        , try_cast(srcProduct as int) as srcProduct
        , Product
        , GroupID
        , try_cast(UserDefNum2 as double) as UserDefNum2
        , try_cast(UserDefNum3 as double) as UserDefNum3
        , try_cast(YearMo as int) as YearMo
        , try_cast(PaidMo as int) as PaidMo
        , try_cast(MSA as int) as MSA
        , ExclusionCode
        , try_cast(PartialYearGroup as int) as PartialYearGroup
        , try_cast(MedicareCovered as int) as MedicareCovered
        , HCGIndicator
        , Industry
        , HCG_DRG
        , HCG_ErrorCode
        , CaseAdmitID
        , MR_Line
        , MR_Line_Det
        , MR_Line_Case
        , PBP_Line
        , MA_Line
        , MA_Line_Det
        , Spec_Type
        , FacilityCaseID
        , TelehealthFlag
        , try_cast(MR_Cases_Admits as int) as MR_Cases_Admits
        , try_cast(MR_Units_Days as float) as MR_Units_Days
        , try_cast(MR_Procs as int) as MR_Procs
        , try_cast(MR_Cases_Admits_PatientPay as int) as MR_Cases_Admits_PatientPay
        , try_cast(MR_Units_Days_PatientPay as int) as MR_Units_Days_PatientPay
        , try_cast(MR_Procs_PatientPay as float) as MR_Procs_PatientPay
        , round(try_cast(MR_Billed as double), 2) as MR_Billed
        , round(try_cast(MR_Allowed as double), 2) as MR_Allowed
        , round(try_cast(MR_Paid as double), 2) as MR_Paid
        , round(try_cast(MR_Copay as double), 2) as MR_Copay
        , round(try_cast(MR_Coinsurance as double), 2) as MR_Coinsurance
        , round(try_cast(MR_Deductible as double), 2) as MR_Deductible
        , round(try_cast(MR_PatientPay as double), 2) as MR_PatientPay
        , try_cast(PBP_Cases_Admits as int) as PBP_Cases_Admits
        , try_cast(PBP_Cases_Admits_PatientPay as int) as PBP_Cases_Admits_PatientPay
        , ServiceType
        , try_cast(RVUAdjustedUnits as float) as RVUAdjustedUnits
        , round(try_cast(MedicareInclude as double), 2) as MedicareInclude
        , round(try_cast(MedicareAllowedNtnwd as double), 2) as MedicareAllowedNtnwd
        , round(try_cast(MedicareAllowedArea as double), 2) as MedicareAllowedArea
        , round(try_cast(MedicareAllowedFull as double), 2) as MedicareAllowedFull
        , MedicareFeeSchedule
        , MedicareAPC_DRG_HCPCS
        , MedicareStatusLookup
        , try_cast(MedicareAdjustedUnits as float) as MedicareAdjustedUnits
        , try_cast(MedicareAdjudicated as int) as MedicareAdjudicated
        , round(try_cast(MedicareOutlier as double), 2) as MedicareOutlier
        , round(try_cast(MedicareDSH_UCP as double), 2) as MedicareDSH_UCP
        , round(try_cast(MedicareCapIME as double), 2) as MedicareCapIME
        , round(try_cast(MedicareOpIME as double), 2) as MedicareOpIME
        , try_cast(RVUInclude as int) as RVUInclude
        , try_cast(RVUFinalRVUs as double) as RVUFinalRVUs
        , try_cast(RVUAdjudicated as int) as RVUAdjudicated
        , round(try_cast(RVUUnitCost as double), 2) as RVUUnitCost
        , RVUImputeBasis
        , try_cast(RVUFinalRVUStep as int) as RVUFinalRVUStep
        , try_cast(WorkRVUs as double) as WorkRVUs
        , try_cast(PracticeRVUs as double) as PracticeRVUs
        , try_cast(MalpracticeRVUs as double) as MalpracticeRVUs
        , try_cast(MedicareHCGInclude as int) as MedicareHCGInclude
        , ContributorID
        , try_cast(HCG_GRVU_Include as int) as HCG_GRVU_Include
        , try_cast(HCG_Quality_Indicator as int) as HCG_Quality_Indicator
        , try_cast(DRG_Quality_Indicator as int) as DRG_Quality_Indicator
        , try_cast(Specialty_Quality_Indicator as int) as Specialty_Quality_Indicator
        , try_cast(Year as int) as Year
        , try_cast(part as int) as part
        
    from {catalog}.{db_bronze}.{table}
""")

df_medical_claims.createOrReplaceTempView("medical_claims")

df_medical_claims_claim = spark.sql(f"""
    select * except({icd_cols}) 
    from medical_claims
""")

# display(df_medical_claims)
# display(df_medical_claims_claim)

# COMMAND ----------

# DBTITLE 1,Process Medical Claims Procedures
icd_procs = ",".join([col for col in df_medical_claims.columns if col.startswith("ICDProc")]) # or col.startswith("ICD9Proc")

df_medical_claims_px = spark.sql(f"""  
    with ctePx as (
        select
            medical_claim_id
            , MemberID
            , coalesce(AdmitDate, FromDate) as procedure_start_date
            , coalesce(DischDate, ToDate) as procedure_end_date
            , coalesce(modifier, modifier2) as modifier_concept_id
            , coalesce(days, units) as quantity
            , ProviderID
            , ClaimID
            , ICDVersion
            , posexplode(array({icd_procs})) as (Procedure_Num, Procedure_Code)
        from medical_claims
    )
    , cteFormat as (
        select
            medical_claim_id
            , MemberID
            , procedure_start_date
            , procedure_end_date
            , modifier_concept_id
            , quantity
            , ProviderID
            , ClaimID
            , ICDVersion
            , Procedure_Num + 1 as Procedure_Num
            , Procedure_Code
        from ctePx
        where Procedure_Code is not null
    )
    select distinct
        sha2(
            concat_ws(
                '||'
                , coalesce(medical_claim_id, '')
                , coalesce(MemberID, '')
                , coalesce(procedure_start_date, '')
                , coalesce(procedure_end_date, '')
                , coalesce(modifier_concept_id, '')
                , coalesce(quantity, '')
                , coalesce(ProviderID, '')
                , coalesce(ClaimID, '')
                , coalesce(ICDVersion, '')
                , coalesce(Procedure_Num, '')
                , coalesce(Procedure_Code, '')
            ), 256
        ) as procedure_occurrence_id
        , * 
    from cteFormat
    order by medical_claim_id, Procedure_Num
""")

# display(df_medical_claims_px)

# COMMAND ----------

# DBTITLE 1,Process Medical Claims Diagnoses
icd_diags = ",".join([col for col in df_medical_claims.columns if col.startswith("ICDDiag")])

df_medical_claims_dx = spark.sql(f"""  
    with cteDx as (
        select
            medical_claim_id
            , ClaimID
            , MemberID
            , ProviderID
            , coalesce(AdmitDate, FromDate) as condition_start_date
            , coalesce(DischDate, ToDate) as condition_end_date
            , ICDVersion
            , posexplode(array({icd_diags})) as (Diagnosis_Num, Diagnosis_Code)
        from medical_claims
    )
    , cteUnion as (
        select
            medical_claim_id
            , ClaimID
            , MemberID
            , ProviderID
            , condition_start_date
            , condition_end_date
            , ICDVersion
            , Diagnosis_Num + 1 as Diagnosis_Num
            , 'Secondary' as Diagnosis_Type
            , Diagnosis_Code
        from cteDx
        where Diagnosis_Code is not null

        union all

        select
            medical_claim_id
            , ClaimID
            , MemberID
            , ProviderID
            , coalesce(AdmitDate, FromDate) as condition_start_date
            , coalesce(DischDate, ToDate) as condition_end_date
            , ICDVersion
            , 0 AS Diagnosis_Num
            , 'Primary' AS Diagnosis_Type
            , AdmitDiag AS Diagnosis_Code
        from medical_claims
        where AdmitDiag is not null
    )
    select distinct
        sha2(
            concat_ws(
                '||'
                , coalesce(medical_claim_id, '')
                , coalesce(ClaimID, '')
                , coalesce(MemberID, '')
                , coalesce(ProviderID, '')
                , coalesce(condition_start_date, '')
                , coalesce(condition_end_date, '')
                , coalesce(ICDVersion, '')
                , coalesce(Diagnosis_Num, '')
                , coalesce(Diagnosis_Type, '')
                , coalesce(Diagnosis_Code, '')
            ), 256
        ) as condition_occurrence_id
        , *
    from cteUnion

""").sort("medical_claim_id", "Diagnosis_Num")

# display(df_medical_claims_dx)

# COMMAND ----------

# DBTITLE 1,Write to silver
df_medical_claims_claim.write.mode("overwrite").saveAsTable(f"{catalog}.{db_silver}.{table}_claim")

df_medical_claims_px.write.mode("overwrite").saveAsTable(f"{catalog}.{db_silver}.{table}_procedure")

df_medical_claims_dx.write.mode("overwrite").saveAsTable(f"{catalog}.{db_silver}.{table}_diagnosis")
