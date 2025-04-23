# Databricks notebook source
# MAGIC %md
# MAGIC # Requirements
# MAGIC 1. Combine clinical data with other supplied claims data.
# MAGIC 2. Structure this data to feed into a Common Data Model format for consumption in a Data Warehouse / Lakehouse with a focus on FHIR/HL7.
# MAGIC
# MAGIC # Assumptions
# MAGIC 1. Proper mapping of incoming data ponits is not absolutely essential for this project. This project is more to show how I would build a data pipeline and not how familiar I am with modeling healthcare data.
# MAGIC
# MAGIC # Improvements
# MAGIC 1. Create a detailed mapping document for values from source files to OMOP CDM.
# MAGIC 2. Abstract unique ID creation as a function for reproducibility.
# MAGIC 3. When writing tables, I'd prefer to do incremental updates. If the source data doesn't change, I would do append only updates using checkpoints and writeStream. If the source data could change, then a MERGE statement would be more appropriate.

# COMMAND ----------

# DBTITLE 1,Set Variables
catalog = "milliman_data_lake"
db_silver = "db_silver"
db_gold = "db_gold"
table = "ccda_docs"

# COMMAND ----------

# DBTITLE 1,Process OMOP Person gold table

df_omop_person = spark.sql(f"""
    select 
        -- * 
        patient_id as person_id
        , null as gender_concept_id
        , try_cast(nullif(concat_ws(',',collect_set(year(birth_date))),'') as int) as year_of_birth
        , try_cast(nullif(concat_ws(',',collect_set(month(birth_date))),'') as int) as month_of_birth
        , try_cast(nullif(concat_ws(',',collect_set(day(birth_date))),'') as int) as day_of_birth
        , try_cast(nullif(concat_ws(',',collect_set(birth_date)),'') as timestamp) as birth_datetime
        , null as race_concept_id
        , null as ethnicity_concept_id
        , null as location_id
        , nullif(
            concat_ws(',',
                collect_set(
                    sha2(
                        concat_ws(
                            '||'
                            , ''
                            , coalesce(provider_fname, '')
                            , coalesce(provider_lname, '')
                            , coalesce(provider_org, '')
                            , coalesce(provider_phone, '')
                            , coalesce(provider_address, '')
                            , coalesce(provider_city, '')
                            , coalesce(provider_state, '')
                            , coalesce(provider_zip_code, '')
                            , coalesce(provider_country, '')
                        ), 256
                    )
                )
            ),''
        ) as provider_id
        , null as care_site_id
        , null as person_source_value
        , nullif(concat_ws(',',collect_set(gender)),'') as gender_source_value
        , null as gender_source_concept_id
        , nullif(concat_ws(',',collect_set(race)),'') as race_source_value
        , null as race_source_concept_id
        , nullif(concat_ws(',',collect_set(ethnicity)),'') as ethnicity_source_value
        , null as ethnicity_source_concept_id

    from {catalog}.{db_silver}.ccda_docs_patient
    group by patient_id
""")

# display(df_omop_person)
# df_omop_person.printSchema()

df_omop_person.write.mode("overwrite").saveAsTable(f"{catalog}.{db_gold}.person")

# COMMAND ----------

# DBTITLE 1,Process OMOP Drug Exposure gold table
df_omop_drug = spark.sql(f"""
    select
        medication_id as drug_exposure_id
        , patient_id as person_id
        , drug_concept_id
        , try_cast(drug_exposure_start_date as date) as drug_exposure_start_date
        , drug_exposure_start_date as drug_exposure_start_datetime
        , try_cast(drug_exposure_end_date as date) as drug_exposure_end_date
        , drug_exposure_end_date as drug_exposure_end_datetime
        , null as verbatim_end_date
        , null as drug_type_concept_id
        , null as stop_reason
        , try_cast(drug_supply_refill as int) as refills
        , try_cast(drug_supply_quantity as double) as quantity
        , null as days_supply
        , null as sig
        , route_concept_id
        , null as lot_number
        , sha2(
                concat_ws(
                    '||'
                    , coalesce(provider_id, '')
                    , coalesce(provider_fname, '')
                    , coalesce(provider_lname, '')
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                ), 256
        ) as provider_id
        , null as visit_occurrence_id
        , null as visit_detail_id
        , drug_source_value
        , drug_source_concept_id
        , route_source_value
        , dose_unit_source_value

    from {catalog}.{db_silver}.ccda_docs_medication

    union 

    select 
        prescription_claim_id as drug_exposure_id
        , MemberID as person_id
        , 'NDC' as drug_concept_id
        , try_cast(FromDate as date) as drug_exposure_start_date
        , try_cast(FromDate as timestamp) as drug_exposure_start_datetime
        , null as drug_exposure_end_date
        , null as drug_exposure_end_datetime
        , null as verbatim_end_date
        , null as drug_type_concept_id
        , null as stop_reason
        , MR_Scripts_30Day as refills
        , QuantityDispensed as quantity
        , DaysSupply as days_supply
        , null as sig
        , null as route_concept_id
        , null as lot_number
        , sha2(concat_ws('||', coalesce(ProviderID, ''), '', '', '', '', '', '', '', '', ''), 256) as provider_id
        , null as visit_occurrence_id
        , null as visit_detail_id
        , NDC as drug_source_value
        , 'NDC' as drug_source_concept_id
        , null as route_source_value
        , null as dose_unit_source_value

    from {catalog}.{db_silver}.prescription_claim
""")

# display(df_omop_drug)
# df_omop_drug.printSchema()

df_omop_drug.write.mode("overwrite").saveAsTable(f"{catalog}.{db_gold}.drug_exposure")

# COMMAND ----------

# DBTITLE 1,Process OMOP Condition Occurrence gold table
df_omop_problem = spark.sql(f"""
    select 
        problem_id as condition_occurrence_id
        , patient_id as person_id
        , condition_concept_id
        , try_cast(condition_start_date as date) as condition_start_date
        , condition_start_date as condition_start_datetime
        , try_cast(condition_end_date as date) as condition_end_date
        , condition_end_date as condition_end_datetime
        , condition_type_concept_id
        , null as condition_status_concept_id
        , null as stop_reason
        , sha2(concat_ws('||', coalesce(provider_id, ''), coalesce(provider_fname, ''), coalesce(provider_lname, ''), '', '', '', '', '', '', ''), 256) as provider_id
        , null as visit_occurrence_id
        , null as visit_detail_id
        , condition_source_value
        , condition_source_concept_id
        , problem_status as condition_status_source_value

    from {catalog}.{db_silver}.ccda_docs_problem

    union

    select
        condition_occurrence_id
        , MemberID as person_id
        , '2.16.840.1.113883.6.90' as condition_concept_id
        , condition_start_date as condition_start_date
        , try_cast(condition_start_date as timestamp) as condition_start_datetime
        , condition_end_date as condition_end_date
        , try_cast(condition_end_date as timestamp) as condition_end_datetime
        , 'CLAIMS' as condition_type_concept_id
        , null as condition_status_concept_id
        , null as stop_reason
        , sha2(concat_ws('||', coalesce(ProviderID, ''), '', '', '', '', '', '', '', '', ''), 256) as provider_id
        , null as visit_occurrence_id
        , null as visit_detail_id
        , Diagnosis_Code as condition_source_value
        , null as condition_source_concept_id
        , Diagnosis_Type as condition_status_source_value

    from {catalog}.{db_silver}.medical_claims_diagnosis
""")

# display(df_omop_problem)
# df_problem.printSchema()

df_omop_problem.write.mode("overwrite").saveAsTable(f"{catalog}.{db_gold}.condition_occurrence")

# COMMAND ----------

# DBTITLE 1,Process OMOP Provider gold table
df_omop_provider = spark.sql(f"""
    with cteProviders as (
        select
            sha2(
                concat_ws(
                    '||'
                    , ''
                    , coalesce(provider_fname, '')
                    , coalesce(provider_lname, '')
                    , coalesce(provider_org, '')
                    , coalesce(provider_phone, '')
                    , coalesce(provider_address, '')
                    , coalesce(provider_city, '')
                    , coalesce(provider_state, '')
                    , coalesce(provider_zip_code, '')
                    , coalesce(provider_country, '')
                ), 256
            ) as provider_id
            , null as provider_id_source
            , provider_fname
            , provider_lname
            , provider_org
            , provider_phone
            , provider_address
            , provider_city
            , provider_state
            , provider_zip_code
            , provider_country

        from {catalog}.{db_silver}.ccda_docs_patient

        union 

        select
            sha2(
                concat_ws(
                    '||'
                    , coalesce(provider_id, '')
                    , coalesce(provider_fname, '')
                    , coalesce(provider_lname, '')
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                ), 256
            ) as provider_id
            , provider_id as provider_id_source
            , provider_fname
            , provider_lname
            , null as provider_org
            , null as provider_phone
            , null as provider_address
            , null as provider_city
            , null as provider_state
            , null as provider_zip_code
            , null as provider_country

        from {catalog}.{db_silver}.ccda_docs_medication

        union

        select
            sha2(
                concat_ws(
                    '||'
                    , coalesce(provider_id, '')
                    , coalesce(provider_fname, '')
                    , coalesce(provider_lname, '')
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                ), 256
            ) as provider_id
            , provider_id as provider_id_source
            , provider_fname
            , provider_lname
            , null as provider_org
            , null as provider_phone
            , null as provider_address
            , null as provider_city
            , null as provider_state
            , null as provider_zip_code
            , null as provider_country

        from {catalog}.{db_silver}.ccda_docs_problem

        union

        select
            sha2(
                concat_ws(
                    '||'
                    , coalesce(ProviderID, '')
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                ), 256
            ) as provider_id
            , ProviderID as provider_id_source
            , null as provider_fname
            , null as provider_lname
            , null as provider_org
            , null as provider_phone
            , null as provider_address
            , null as provider_city
            , null as provider_state
            , null as provider_zip_code
            , null as provider_country

        from {catalog}.{db_silver}.medical_claims_claim

        union

        select
            sha2(
                concat_ws(
                    '||'
                    , coalesce(ProviderID, '')
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                ), 256
            ) as provider_id
            , ProviderID as provider_id_source
            , null as provider_fname
            , null as provider_lname
            , null as provider_org
            , null as provider_phone
            , null as provider_address
            , null as provider_city
            , null as provider_state
            , null as provider_zip_code
            , null as provider_country

        from {catalog}.{db_silver}.medical_claims_diagnosis

        union

        select
            sha2(
                concat_ws(
                    '||'
                    , coalesce(ProviderID, '')
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                ), 256
            ) as provider_id
            , ProviderID as provider_id_source
            , null as provider_fname
            , null as provider_lname
            , null as provider_org
            , null as provider_phone
            , null as provider_address
            , null as provider_city
            , null as provider_state
            , null as provider_zip_code
            , null as provider_country

        from {catalog}.{db_silver}.medical_claims_procedure

        union

        select
            sha2(
                concat_ws(
                    '||'
                    , coalesce(ProviderID, '')
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                    , ''
                ), 256
            ) as provider_id
            , ProviderID as provider_id_source
            , null as provider_fname
            , null as provider_lname
            , null as provider_org
            , null as provider_phone
            , null as provider_address
            , null as provider_city
            , null as provider_state
            , null as provider_zip_code
            , null as provider_country

        from {catalog}.{db_silver}.prescription_claim
    )

    select 
        provider_id
        , iff(provider_lname is null, null, concat_ws(', ', provider_lname, provider_fname)) as provider_name
        , provider_id_source as npi
        , null as dea
        , null as specialty_concept_id
        , null as care_site_id
        , null as year_of_birth
        , null as gender_concept_id
        , null as provider_source_value
        , null as specialty_source_value
        , null as specialty_source_concept_id
        , null as gender_source_value
        , null as gender_source_concept_id

    from cteProviders
""")

# display(df_omop_provider)
# df_ccda_person.printSchema()

df_omop_provider.write.mode("overwrite").saveAsTable(f"{catalog}.{db_gold}.provider")

# COMMAND ----------

# DBTITLE 1,Process OMOP Procedure gold table
df_omop_procedure = spark.sql(f"""
    select 
        procedure_occurrence_id
        , MemberID as person_id
        , 'ICD-10-PCS' as procedure_concept_id
        , procedure_start_date
        , try_cast(procedure_start_date as timestamp) as procedure_start_datetime
        , procedure_end_date
        , try_cast(procedure_end_date as timestamp) as procedure_end_datetime
        , 'CLAIMS' as procedure_type_concept_id
        , modifier_concept_id
        , try_cast(quantity as int) as quantity
        , sha2(concat_ws('||', coalesce(ProviderID, ''), '', '', '', '', '', '', '', '', ''), 256) as provider_id
        , null as visit_occurrence_id
        , null as visit_detail_id
        , Procedure_Code as procedure_source_value
        , 'ICD-10-PCS' as procedure_source_concept_id
        , null as modifier_source_value

    from {catalog}.{db_silver}.medical_claims_procedure
""")

# display(df_omop_procedure)
# df_omop_procedure.printSchema()

df_omop_procedure.write.mode("overwrite").saveAsTable(f"{catalog}.{db_gold}.procedure_occurrence")

# COMMAND ----------

# DBTITLE 1,Process OMOP Cost gold table

df_omop_cost = spark.sql(f"""
    (
        with cteCost as (
            select 
                c.medical_claim_id
                , sha2(
                    concat_ws(
                        '||'
                        , coalesce(medical_claim_id, '')
                        , coalesce(MemberId, '')
                        , coalesce(POS, '')
                        , coalesce(coalesce(AdmitDate, FromDate), '')
                        , coalesce(coalesce(DischDate, ToDate), '')
                        , coalesce('CLAIMS', '')
                        , coalesce(sha2(concat_ws('||', coalesce(ProviderID, ''), '', '', '', '', '', '', '', '', ''), 256), '')
                        , coalesce(null, '')
                        , coalesce(ClaimID, '')
                        , coalesce(null, '')
                        , coalesce(null, '')
                        , coalesce(AdmitSource, '')
                        , coalesce(null, '')
                        , coalesce(DischargeStatus, '')
                        , coalesce(null, '')
                    ), 256
                ) as cost_event_id
                , 'visit' as cost_domain_id
                , 'CLAIMS' as cost_type_concept_id
                , 'USD' as currency_concept_id
                , c.Billed as total_charge
                , null as total_cost
                , c.Paid + c.COB + c.Copay + c.Coinsurance + c.Deductible + c.PatientPay as total_paid
                , c.Paid as paid_by_payer
                , c.PatientPay as paid_by_patient
                , c.Copay as paid_patient_copay
                , c.Coinsurance as paid_patient_coinsurance
                , c.Deductible as paid_patient_deductible
                , c.Paid as paid_by_primary
                , null as paid_ingredient_cost
                , null as paid_dispensing_fee
                , null as payer_plan_period_id
                , c.Allowed as amount_allowed
                , null as revenue_code_concept_id
                , null as revenue_code_source_value
                , null as drg_concept_id
                , null as drg_source_value

            from {catalog}.{db_silver}.medical_claims_claim as c      
        )
        select
            sha2(
                concat_ws(
                    '||'
                    , coalesce(medical_claim_id, '')
                    , coalesce(cost_event_id, '')
                    , coalesce(cost_domain_id, '')
                    , coalesce(cost_type_concept_id, '')
                    , coalesce(currency_concept_id, '')
                    , coalesce(total_charge, '')
                    , coalesce(total_cost, '')
                    , coalesce(total_paid, '')
                ) 
                , 256
            ) as cost_id
            , * except(medical_claim_id)

        from cteCost
    )

    union

    (
        with cteCost2 as (
            select 
                prescription_claim_id as cost_event_id
                , 'drug' as cost_domain_id
                , 'CLAIMS' as cost_type_concept_id
                , 'USD' as currency_concept_id
                , Billed as total_charge
                , null as total_cost
                , Paid + COB + Copay + Coinsurance + Deductible + PatientPay as total_paid
                , Paid as paid_by_payer
                , PatientPay as paid_by_patient
                , Copay as paid_patient_copay
                , Coinsurance as paid_patient_coinsurance
                , Deductible as paid_patient_deductible
                , Paid as paid_by_primary
                , IngredientCost as paid_ingredient_cost
                , DispensingFee as paid_dispensing_fee
                , null as payer_plan_period_id
                , Allowed as amount_allowed
                , null as revenue_code_concept_id
                , null as revenue_code_source_value
                , null as drg_concept_id
                , null as drg_source_value

            from {catalog}.{db_silver}.prescription_claim

        )
        select
            sha2(
                concat_ws(
                    '||'
                    , coalesce(cost_event_id, '')
                    , coalesce(cost_domain_id, '')
                    , coalesce(cost_type_concept_id, '')
                    , coalesce(currency_concept_id, '')
                    , coalesce(total_charge, '')
                    , coalesce(total_cost, '')
                    , coalesce(total_paid, '')
                ) 
                , 256
            ) as cost_id
            , *

        from cteCost2
    )
""")

# display(df_omop_cost)
# df_omop_cost.printSchema()

df_omop_cost.write.mode("overwrite").saveAsTable(f"{catalog}.{db_gold}.cost")

# COMMAND ----------

# DBTITLE 1,Process OMOP Visit Occurrence gold table
df_omop_visit = spark.sql(f"""
    (
        with cteVisitClaim as (
            select 
                medical_claim_id
                , MemberId as person_id
                , POS as visit_concept_id
                , coalesce(AdmitDate, FromDate) as visit_start_date
                , try_cast(coalesce(AdmitDate, FromDate) as timestamp) as visit_start_datetime
                , coalesce(DischDate, ToDate) as visit_end_date
                , try_cast(coalesce(DischDate, ToDate) as timestamp) as visit_end_datetime
                , 'CLAIMS' as visit_type_concept_id
                , sha2(concat_ws('||', coalesce(ProviderID, ''), '', '', '', '', '', '', '', '', ''), 256) as provider_id
                , null as care_site_id
                , ClaimID as visit_source_value
                , null as visit_source_concept_id
                , null as admitted_from_concept_id
                , AdmitSource as admitted_from_source_value
                , null as discharged_to_concept_id
                , DischargeStatus as discharged_to_source_value
                , null as preceding_visit_occurrence_id

            from {catalog}.{db_silver}.medical_claims_claim
        )
        select
            sha2(
                concat_ws(
                    '||'
                    , coalesce(medical_claim_id, '')
                    , coalesce(person_id, '')
                    , coalesce(visit_concept_id, '')
                    , coalesce(visit_start_date, '')
                    , coalesce(visit_end_date, '')
                    , coalesce(visit_type_concept_id, '')
                    , coalesce(provider_id, '')
                    , coalesce(care_site_id, '')
                    , coalesce(visit_source_value, '')
                    , coalesce(visit_source_concept_id, '')
                    , coalesce(admitted_from_concept_id, '')
                    , coalesce(admitted_from_source_value, '')
                    , coalesce(discharged_to_concept_id, '')
                    , coalesce(discharged_to_source_value, '')
                    , coalesce(preceding_visit_occurrence_id, '')
                ), 256
            ) as visit_occurrence_id
            , * except(medical_claim_id)

        from cteVisitClaim
    )

    union

    (
        with cteVisitCCDA as (
            select 
                patient_id as person_id
                , null as visit_concept_id
                , try_cast(effective_time as date) as visit_start_date
                , effective_time as visit_start_datetime
                , try_cast(effective_time as date) as visit_end_date
                , effective_time as visit_end_datetime
                , 'CCDA' as visit_type_concept_id
                , sha2(
                    concat_ws(
                        '||'
                        , ''
                        , coalesce(provider_fname, '')
                        , coalesce(provider_lname, '')
                        , coalesce(provider_org, '')
                        , coalesce(provider_phone, '')
                        , coalesce(provider_address, '')
                        , coalesce(provider_city, '')
                        , coalesce(provider_state, '')
                        , coalesce(provider_zip_code, '')
                        , coalesce(provider_country, '')
                    ), 256
                ) as provider_id
                , null as care_site_id
                , document_id as visit_source_value
                , null as visit_source_concept_id
                , null as admitted_from_concept_id
                , null as admitted_from_source_value
                , null as discharged_to_concept_id
                , null as discharged_to_source_value
                , null as preceding_visit_occurrence_id

            from {catalog}.{db_silver}.ccda_docs_patient
        )
        select
            sha2(
                concat_ws(
                    '||'
                    , coalesce(person_id, '')
                    , coalesce(visit_concept_id, '')
                    , coalesce(visit_start_date, '')
                    , coalesce(visit_end_date, '')
                    , coalesce(visit_type_concept_id, '')
                    , coalesce(provider_id, '')
                    , coalesce(care_site_id, '')
                    , coalesce(visit_source_value, '')
                    , coalesce(visit_source_concept_id, '')
                    , coalesce(admitted_from_concept_id, '')
                    , coalesce(admitted_from_source_value, '')
                    , coalesce(discharged_to_concept_id, '')
                    , coalesce(discharged_to_source_value, '')
                    , coalesce(preceding_visit_occurrence_id, '')
                ), 256
            ) as visit_occurrence_id
            , *

        from cteVisitCCDA
    )

""")
# display(df_omop_visit)
# df_omop_visit.printSchema()

df_omop_visit.write.mode("overwrite").saveAsTable(f"{catalog}.{db_gold}.visit_occurrence")
