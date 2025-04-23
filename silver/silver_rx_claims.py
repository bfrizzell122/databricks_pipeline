# Databricks notebook source
# MAGIC %md
# MAGIC # Requirements
# MAGIC 1. Structure this data to feed into a Common Data Model format for consumption in a Data Warehouse / Lakehouse with a focus on FHIR/HL7.
# MAGIC
# MAGIC # Assumptions
# MAGIC 1. Universe of values present in CSV files are the only values available.
# MAGIC
# MAGIC # Improvements
# MAGIC 1. Use a schema/data contract file to standardize columns/values dynamically.
# MAGIC 2. partitionBy

# COMMAND ----------

catalog = "milliman_data_lake"
db_bronze = "db_bronze"
db_silver = "db_silver"
table = "rx_claims"

# COMMAND ----------

# DBTITLE 1,Process Prescription Claims
df_rx_claims = spark.sql(f"""
    select distinct
        sha2(
            concat_ws(
                '||'
                , coalesce(SequenceNumber, '')
                , coalesce(ClaimID, '')
                , coalesce(MemberID, '')
                , coalesce(MemberID, '')
                , coalesce(FromDate , '')
            ), 256
        ) as prescription_claim_id
        , try_cast(SequenceNumber as int) as SequenceNumber
        , ClaimID
        , ContractID
        , MemberID
        , make_date(
            split(FromDate, '/')[2]
            , split(FromDate, '/')[0]
            , split(FromDate, '/')[1]
        ) as FromDate
        , make_date(
            split(PaidDate, '/')[2]
            , split(PaidDate, '/')[0]
            , split(PaidDate, '/')[1]
        ) as PaidDate
        , NDC
        , EncounterFlag
        , MedicalCovered
        , ProviderID
        , MailOrder
        , round(try_cast(IngredientCost	as double), 2) as IngredientCost
        , round(try_cast(DispensingFee as double), 2) as DispensingFee
        , round(try_cast(Billed as double), 2) as Billed
        , round(try_cast(Allowed as double), 2) as Allowed
        , round(try_cast(Paid as double), 2) as Paid
        , round(try_cast(COB as double), 2) as COB
        , round(try_cast(Copay as double), 2) as Copay
        , round(try_cast(Coinsurance as double), 2) as Coinsurance
        , round(try_cast(Deductible as double), 2) as Deductible
        , round(try_cast(PatientPay as double), 2) as PatientPay
        , try_cast(Units as float) as Units
        , try_cast(DaysSupply as int) as DaysSupply
        , try_cast(QuantityDispensed as float) as QuantityDispensed
        , OON
        , srcLOB
        , LOB
        , try_cast(srcProduct as int) as srcProduct
        , Product
        , GroupID
        , try_cast(UserDefNum3 as int) as UserDefNum3
        , try_cast(YearMo as int) as YearMo
        , try_cast(PaidMo as int) as PaidMo
        , try_cast(MSA as int) as MSA
        , ExclusionCode
        , try_cast(PartialYearGroup as int) as PartialYearGroup
        , try_cast(MedicareCovered as int) as MedicareCovered
        , HCGIndicator
        , Industry
        , CaseAdmitID
        , MR_Line
        , MR_Line_Det
        , MR_Line_Case
        , PBP_Line
        , MA_Line
        , MA_Line_Det
        , try_cast(MR_Units_Days as float) as MR_Units_Days
        , try_cast(MR_Procs as int) as MR_Procs
        , try_cast(MR_Scripts_30Day as int) as MR_Scripts_30Day
        , try_cast(MR_Units_Days_PatientPay as float) as MR_Units_Days_PatientPay
        , try_cast(MR_Procs_PatientPay as int) as MR_Procs_PatientPay
        , round(try_cast(MR_Billed as double), 2) as MR_Billed
        , round(try_cast(MR_Allowed as double), 2) as MR_Allowed
        , round(try_cast(MR_Paid as double), 2) as MR_Paid
        , round(try_cast(MR_Copay as double), 2) as MR_Copay
        , round(try_cast(MR_Coinsurance as double), 2) as MR_Coinsurance
        , round(try_cast(MR_Deductible as double), 2) as MR_Deductible
        , round(try_cast(MR_PatientPay as double), 2) as MR_PatientPay
        , try_cast(RVUInclude as int) as RVUInclude
        , try_cast(RVUFinalRVUs as double) as RVUFinalRVUs
        , try_cast(RVUAdjudicated as int) as RVUAdjudicated
        , RVUImputeBasis
        , try_cast(RVUFinalRVUStep as int) as RVUFinalRVUStep
        , try_cast(RVUAdjustedUnits as float) as RVUAdjustedUnits
        , ContributorID
        , try_cast(HCG_GRVU_Include as int) as HCG_GRVU_Include
        , try_cast(HCG_Quality_Indicator as int) as HCG_Quality_Indicator
        , try_cast(DRG_Quality_Indicator as int) as DRG_Quality_Indicator
        , try_cast(Specialty_Quality_Indicator as int) as Specialty_Quality_Indicator
        , try_cast(Year as int) as Year
        , try_cast(part as int) as part

    from {catalog}.{db_bronze}.{table}
    order by MemberID, ClaimID, SequenceNumber
""")

# display(df_rx_claims)

# COMMAND ----------

# DBTITLE 1,Write to silver
df_rx_claims.write.mode("overwrite").saveAsTable(f"{catalog}.{db_silver}.prescription_claim")
