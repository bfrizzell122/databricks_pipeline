# Databricks notebook source
# MAGIC %md
# MAGIC # Requirements
# MAGIC 1. Analyze and parse the content from the documents into their respective domains. 
# MAGIC     - Medications
# MAGIC     - Problems
# MAGIC 2. Structure this data to feed into a Common Data Model format for consumption in a Data Warehouse / Lakehouse with a focus on FHIR/HL7
# MAGIC
# MAGIC # Assumptions
# MAGIC 1. All required data points can only be identified by the XML attributes identified in the code.
# MAGIC     - Medications -> root="2.16.840.1.113883.10.20.22.2.1.1
# MAGIC     - Problems -> root="2.16.840.1.113883.10.20.22.2.5.1
# MAGIC
# MAGIC # Improvements
# MAGIC 1. In a brief search for Python libraries to parse CCDA XML documents, I did not find any that were production-grade. The ones I found were no longer supported or still in development. I assume that there are libraries available that could make parsing easier and more reliable, but the time constraints of this project did not allow me to find them. With that said, future improvements would include utilizing a more robust parsing framework and becoming more familiar with CCDA XML document structure in order to reliably capture all available data points.
# MAGIC 2. Apply for data quality checks based on CCDA XML field value expectations, if possible.
# MAGIC 3. When writing tables, I'd prefer to do incremental updates. If the source data doesn't change, I would do append only updates using checkpoints and writeStream. If the source data could change, then a MERGE statement would be more appropriate.

# COMMAND ----------

# DBTITLE 1,Set Variables
catalog = "milliman_data_lake"
db_bronze = "db_bronze"
db_silver = "db_silver"
table = "ccda_docs"

# COMMAND ----------

# DBTITLE 1,Process CCDA XML
import xml.etree.ElementTree as et
import re
from datetime import datetime as dt
from pyspark.sql.functions import udf, col, explode, expr, sha2, concat_ws, coalesce, lit
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, TimestampType, Row, ArrayType, FloatType

def format_date(date_string):
    """
    Converts a date string in various format to a date/timestamp value.

    Args:
        date_string (str): Date string in various format.

    Returns:
        Standardized datetime object, or 
        None if the format is incorrect or the date_string parameter value is None.
    """
    if date_string is None:
        return None
    elif len(date_string) == 8:
        try:
            return dt.strptime(date_string, '%Y%m%d') if re.match(r'\d{8}$', date_string) else None
        except Exception:
            return None
    elif len(date_string) == 14:
        try:
            return dt.strptime(date_string, '%Y%m%d%H%M%S') if re.match(r'\d{14}$', date_string) else None
        except Exception:
            return None
    else:
        return None

def parse_ccda(xml_content):
    """
    Parses a CCDA XML file content using XPATH and extracts required information.

    Args:
        xml_content (str): Content of the CCDA XML file.

    Returns:
        Row: A Row object containing extracted data from the CCDA XML file.
    """

    xml_tree = et.ElementTree(et.fromstring(xml_content))
    xml_root = xml_tree.getroot()
    
    # Define namespaces
    namespaces = {'hl7': 'urn:hl7-org:v3'}
    
    # Null Checks
    null_check_get = lambda x, y: x.get(y) if x is not None else None
    null_check_text = lambda x: x.text if x is not None else None
    null_check_int = lambda x: int(x.text) if x is not None else None

    # Data Type Conversions
    cast_float_type = lambda x: float(x) if x is not None else None

    # Extract Patient and Provider data
    document_type = null_check_get(xml_root.find('.//hl7:code[@code="34133-9"]', namespaces), 'displayName')
    effective_time = format_date(null_check_get(xml_root.find('.//hl7:effectiveTime', namespaces), 'value'))
    document_title = null_check_text(xml_root.find('.//hl7:title', namespaces))
    patient_fname = null_check_text(xml_root.find('.//hl7:patient/hl7:name/hl7:given', namespaces))
    patient_lname = null_check_text(xml_root.find('.//hl7:patient/hl7:name/hl7:family', namespaces))
    gender_code = null_check_get(xml_root.find('.//hl7:patient/hl7:administrativeGenderCode', namespaces), 'code')
    gender = null_check_get(xml_root.find('.//hl7:patient/hl7:administrativeGenderCode', namespaces), 'displayName')
    race = null_check_get(xml_root.find('.//hl7:patient/hl7:raceCode', namespaces), 'displayName')
    ethnicity = null_check_get(xml_root.find('.//hl7:patient/hl7:ethnicGroupCode', namespaces), 'displayName')
    birth_time = null_check_get(xml_root.find('.//hl7:patient/hl7:birthTime', namespaces), 'value')
    birth_date = format_date(birth_time)
    street_address = null_check_text(xml_root.find('.//hl7:patientRole/hl7:addr/hl7:streetAddressLine', namespaces))
    city = null_check_text(xml_root.find('.//hl7:patientRole/hl7:addr/hl7:city', namespaces))
    state = null_check_text(xml_root.find('.//hl7:patientRole/hl7:addr/hl7:state', namespaces))
    zip_code = null_check_int(xml_root.find('.//hl7:patientRole/hl7:addr/hl7:postalCode', namespaces))
    country = null_check_text(xml_root.find('.//hl7:patientRole/hl7:addr/hl7:country', namespaces))
    home_phone = null_check_get(xml_root.find('.//hl7:patientRole/hl7:telecom[@use="HP"]', namespaces), 'value')
    mobile_phone = null_check_get(xml_root.find('.//hl7:patientRole/hl7:telecom[@use="MC"]', namespaces), 'value')
    marital_code = null_check_get(xml_root.find('.//hl7:patient/hl7:maritalStatusCode', namespaces), 'code')
    marital_status = null_check_get(xml_root.find('.//hl7:patient/hl7:maritalStatusCode', namespaces), 'displayName')
    religion = null_check_get(xml_root.find('.//hl7:patient/hl7:religiousAffiliationCode', namespaces), 'displayName')
    language = null_check_get(xml_root.find('.//hl7:patient/hl7:languageCommunication/hl7:languageCode', namespaces), 'code')
    provider_fname = null_check_text(xml_root.find('.//hl7:documentationOf/hl7:serviceEvent/hl7:performer/hl7:assignedEntity/hl7:assignedPerson/hl7:name/hl7:given', namespaces))
    provider_lname = null_check_text(xml_root.find('.//hl7:documentationOf/hl7:serviceEvent/hl7:performer/hl7:assignedEntity/hl7:assignedPerson/hl7:name/hl7:family', namespaces))
    provider_org = null_check_text(xml_root.find('.//hl7:providerOrganization/hl7:name', namespaces))
    provider_phone = null_check_get(xml_root.find('.//hl7:providerOrganization/hl7:telecom', namespaces), 'value')
    provider_address = null_check_text(xml_root.find('.//hl7:providerOrganization/hl7:addr/hl7:streetAddressLine', namespaces))
    provider_city = null_check_text(xml_root.find('.//hl7:providerOrganization/hl7:addr/hl7:city', namespaces))
    provider_state = null_check_text(xml_root.find('.//hl7:providerOrganization/hl7:addr/hl7:state', namespaces))
    provider_zip_code = null_check_int(xml_root.find('.//hl7:providerOrganization/hl7:addr/hl7:postalCode', namespaces))
    provider_country = null_check_text(xml_root.find('.//hl7:providerOrganization/hl7:addr/hl7:country', namespaces))

    # Get all sections from the document body
    all_sections = xml_root.findall('.//hl7:section', namespaces)
    
    # Initialize lists in case no problems or medications are found
    problems = []
    medications = []

    if all_sections is not None:

        problem_section_list = []
        medication_section = []

        for section in all_sections:

            # Filter for sections with root="2.16.840.1.113883.10.20.22.2.5.1" -> Problems and root="2.16.840.1.113883.10.20.22.2.1.1" -> Medications
            problem_section_list = section.findall('./hl7:templateId[@root="2.16.840.1.113883.10.20.22.2.5.1"]', namespaces)
            medication_section_list = section.findall('./hl7:templateId[@root="2.16.840.1.113883.10.20.22.2.1.1"]', namespaces)

            if problem_section_list is not None and len(problem_section_list) > 0:

                problem_entry_list = []
                problem_entry_list = section.findall('./hl7:entry/hl7:act', namespaces)

                for problem in problem_entry_list:

                    # Some problem sections are present, but have no problems in them. This check will skip those sections.
                    no_problem_check = null_check_get(problem.find('./hl7:entryRelationship/hl7:observation/hl7:value', namespaces), 'nullFlavor')

                    if no_problem_check is None or no_problem_check != "NI":
                        
                        observation = problem.find('./hl7:entryRelationship/hl7:observation', namespaces)

                        concern_type_code = null_check_get(problem.find('./hl7:code', namespaces), 'code')
                        concern_type_name = null_check_get(problem.find('./hl7:code', namespaces), 'displayName')
                        problem_status = null_check_get(problem.find('./hl7:statusCode', namespaces), 'code')
                        condition_concept_id = null_check_get(observation.find('.hl7:value', namespaces), 'codeSystem')
                        condition_concept_value = null_check_get(observation.find('./hl7:value', namespaces), 'codeSystemName')
                        condition_start_date = format_date(null_check_get(observation.find('./hl7:effectiveTime/hl7:low', namespaces), 'value'))
                        condition_end_date = format_date(null_check_get(observation.find('./hl7:effectiveTime/hl7:high', namespaces), 'value'))
                        condition_type_concept_id = 'CCDA_XML_DOCS'
                        condition_source_value = null_check_get(observation.find('./hl7:value', namespaces), 'code')
                        condition_source_concept_id = null_check_get(observation.find('./hl7:value', namespaces), 'displayName')
                        provider_id = null_check_get(observation.find('./hl7:author/hl7:assignedAuthor/hl7:id[@root="2.16.840.1.113883.4.6"]', namespaces), 'extension')
                        provider_fname = null_check_text(observation.find('./hl7:author/hl7:assignedAuthor/hl7:assignedPerson/hl7:name/hl7:given', namespaces))
                        provider_lname = null_check_text(observation.find('./hl7:author/hl7:assignedAuthor/hl7:assignedPerson/hl7:name/hl7:family', namespaces))
                        provider_entry_date = format_date(null_check_get(observation.find('./hl7:author/hl7:time', namespaces), 'value'))

                        if condition_concept_id is None \
                            or condition_concept_value is None \
                            or condition_source_value is None \
                            or condition_source_concept_id is None:
                            
                            translations = []
                            translations = observation.findall('./hl7:value/hl7:translation', namespaces)

                            if translations is not None and len(translations) > 0:
                                condition_concept_id = null_check_get(translations[0], 'codeSystem')
                                condition_concept_value = null_check_get(translations[0], 'codeSystemName')
                                condition_source_value = null_check_get(translations[0], 'code')
                                condition_source_concept_id = null_check_get(translations[0], 'displayName')   

                        problems.append(Row(
                            concern_type_code = concern_type_code
                            , concern_type_name = concern_type_name
                            , problem_status = problem_status
                            , condition_concept_id = condition_concept_id
                            , condition_concept_value = condition_concept_value
                            , condition_start_date = condition_start_date
                            , condition_end_date = condition_end_date
                            , condition_type_concept_id = condition_type_concept_id
                            , condition_source_value = condition_source_value
                            , condition_source_concept_id = condition_source_concept_id
                            , provider_id = provider_id
                            , provider_fname = provider_fname
                            , provider_lname = provider_lname
                            , provider_entry_date = provider_entry_date
                        ))

                    # ELSE NO PROBLEMS FOUND

            else:
                pass
                ### RAISE ERROR OR LOG FOR INVALID CCDA XML STRUCTURE: NO PROBLEM SECTION ELEMENTS

            if medication_section_list is not None and len(medication_section_list) > 0:

                medication_entry_list = []
                medication_entry_list = section.findall('./hl7:entry/hl7:substanceAdministration', namespaces)

                for sub_admin_element in medication_entry_list:

                    material_element_list = []
                    material_element_list = sub_admin_element.find('./hl7:consumable/hl7:manufacturedProduct/hl7:manufacturedMaterial', namespaces)

                    if material_element_list is not None and len(material_element_list) > 0:

                        code_element = material_element_list.find('./hl7:code', namespaces)

                        drug_concept_id = null_check_get(code_element, 'codeSystem')
                        drug_concept_value = null_check_get(code_element, 'codeSystemName')
                        drug_source_value = null_check_get(code_element, 'displayName')
                        drug_source_concept_id = null_check_get(code_element, 'code')
                        drug_exposure_start_date = format_date(null_check_get(sub_admin_element.find('./hl7:effectiveTime/hl7:low', namespaces), 'value'))
                        drug_exposure_end_date = format_date(null_check_get(sub_admin_element.find('./hl7:effectiveTime/hl7:high', namespaces), 'value'))
                        dose_unit_concept_id = null_check_get(sub_admin_element.find('./hl7:doseQuantity', namespaces), 'unit')
                        dose_unit_source_value = cast_float_type(null_check_get(sub_admin_element.find('./hl7:doseQuantity', namespaces), 'value'))
                        route_concept_id = null_check_get(sub_admin_element.find('./hl7:routeCode', namespaces), 'code')
                        route_source_value = null_check_get(sub_admin_element.find('./hl7:routeCode', namespaces), 'displayName')
                        drug_supply_quantity = cast_float_type(null_check_get(sub_admin_element.find('./hl7:entryRelationship/hl7:supply/hl7:quantity', namespaces), 'value'))
                        drug_supply_refill = cast_float_type(null_check_get(sub_admin_element.find('./hl7:entryRelationship/hl7:supply/hl7:repeatNumber', namespaces), 'value'))
                        provider_id = null_check_get(sub_admin_element.find('./hl7:author/hl7:assignedAuthor/hl7:id[@root="2.16.840.1.113883.4.6"]', namespaces), 'extension')
                        provider_fname = null_check_text(sub_admin_element.find('./hl7:author/hl7:assignedAuthor/hl7:assignedPerson/hl7:name/hl7:given', namespaces))
                        provider_lname = null_check_text(sub_admin_element.find('./hl7:author/hl7:assignedAuthor/hl7:assignedPerson/hl7:name/hl7:family', namespaces))
                        provider_entry_date = format_date(null_check_get(sub_admin_element.find('./hl7:author/hl7:time', namespaces), 'value'))

                        if drug_concept_id is None:
                            drug_concept_id_temp = code_element.find('./hl7:translation[@codeSystem="2.16.840.1.113883.6.88"]', namespaces)
                            drug_concept_id = null_check_get(drug_concept_id_temp, 'codeSystem')

                            if drug_concept_id is None:
                                drug_concept_id = null_check_get(code_element.find('./hl7:translation', namespaces),'codeSystem')

                        if drug_concept_value is None:
                            drug_concept_value_temp = code_element.find('./hl7:translation[@codeSystem="2.16.840.1.113883.6.88"]', namespaces)
                            drug_concept_value = null_check_get(drug_concept_value_temp, 'codeSystemName')

                            if drug_concept_value is None:
                                drug_concept_value = null_check_get(code_element.find('./hl7:translation', namespaces),'codeSystemName')

                        if drug_source_value is None:
                            drug_source_value_temp = code_element.find('./hl7:translation[@codeSystem="2.16.840.1.113883.6.88"]', namespaces)
                            drug_source_value = null_check_get(drug_source_value_temp, 'displayName')

                            if drug_source_value is None:
                                drug_source_value = null_check_get(code_element.find('./hl7:translation', namespaces),'displayName')  
                            
                                if drug_source_value is None:
                                    drug_source_value = null_check_text(sub_admin_element.find('./hl7:consumable/hl7:manufacturedProduct/hl7:manufacturedMaterial/hl7:name', namespaces))
                        
                        if drug_source_concept_id is None:
                            drug_source_concept_id_temp = code_element.find('./hl7:translation[@codeSystem="2.16.840.1.113883.6.88"]', namespaces)
                            drug_source_concept_id = null_check_get(drug_source_concept_id_temp, 'code')

                            if drug_source_concept_id is None:
                                drug_source_concept_id = null_check_get(code_element.find('./hl7:translation', namespaces),'code')

                        medications.append(Row(
                            drug_concept_id = drug_concept_id
                            , drug_concept_value = drug_concept_value
                            , drug_source_value = drug_source_value
                            , drug_source_concept_id = drug_source_concept_id
                            , drug_exposure_start_date = drug_exposure_start_date
                            , drug_exposure_end_date = drug_exposure_end_date
                            , dose_unit_concept_id = dose_unit_concept_id
                            , dose_unit_source_value = dose_unit_source_value
                            , route_concept_id = route_concept_id
                            , route_source_value = route_source_value
                            , drug_supply_quantity = drug_supply_quantity
                            , drug_supply_refill = drug_supply_refill
                            , provider_id = provider_id
                            , provider_fname = provider_fname
                            , provider_lname = provider_lname
                            , provider_entry_date = provider_entry_date
                        ))

                    # ELSE NO MEDICATIONS FOUND

            else:
                pass
                ### RAISE ERROR FOROR LOG INVALID CCDA XML STRUCTURE: NO MEDICATION SECTION ELEMENTS

    else:
        pass
        ### RAISE ERROR OR LOG FOR INVALID CCDA XML STRUCTURE: NO SECTION ELEMENTS

    return Row(
        document_type = document_type
        , effective_time = effective_time
        , document_title = document_title
        , patient_fname = patient_fname
        , patient_lname = patient_lname
        , gender_code = gender_code
        , gender = gender
        , race = race
        , ethnicity = ethnicity
        , birth_time = birth_time
        , birth_date = birth_date
        , street_address = street_address
        , city = city
        , state = state
        , zip_code = zip_code
        , country = country
        , home_phone = home_phone
        , mobile_phone = mobile_phone
        , marital_code = marital_code
        , marital_status = marital_status
        , religion = religion
        , language = language
        , provider_fname = provider_fname
        , provider_lname = provider_lname
        , provider_org = provider_org
        , provider_phone = provider_phone
        , provider_address = provider_address
        , provider_city = provider_city
        , provider_state = provider_state
        , provider_zip_code = provider_zip_code
        , provider_country = provider_country
        , problems = problems
        , medications = medications
    )


parse_ccda_udf = udf(lambda xml: parse_ccda(xml), 
                    StructType([
                        StructField('document_type', StringType(), True)
                        , StructField('effective_time', TimestampType(), True)
                        , StructField('document_title', StringType(), True)
                        , StructField('patient_fname', StringType(), True)
                        , StructField('patient_lname', StringType(), True)
                        , StructField('gender_code', StringType(), True)
                        , StructField('gender', StringType(), True)
                        , StructField('race', StringType(), True)
                        , StructField('ethnicity', StringType(), True)
                        , StructField('birth_time', StringType(), True)
                        , StructField('birth_date', TimestampType(), True)
                        , StructField('street_address', StringType(), True)
                        , StructField('city', StringType(), True)
                        , StructField('state', StringType(), True)
                        , StructField('zip_code', IntegerType(), True)
                        , StructField('country', StringType(), True)
                        , StructField('home_phone', StringType(), True)
                        , StructField('mobile_phone', StringType(), True)
                        , StructField('marital_code', StringType(), True)
                        , StructField('marital_status', StringType(), True)
                        , StructField('religion', StringType(), True)
                        , StructField('language', StringType(), True)
                        , StructField('provider_fname', StringType(), True)
                        , StructField('provider_lname', StringType(), True)
                        , StructField('provider_org', StringType(), True)
                        , StructField('provider_phone', StringType(), True)
                        , StructField('provider_address', StringType(), True)
                        , StructField('provider_city', StringType(), True)
                        , StructField('provider_state', StringType(), True)
                        , StructField('provider_zip_code', IntegerType(), True)
                        , StructField('provider_country', StringType(), True)
                        , StructField('problems', ArrayType(StructType([
                            StructField('concern_type_code', StringType(), True)
                            , StructField('concern_type_name', StringType(), True)
                            , StructField('problem_status', StringType(), True)
                            , StructField('condition_concept_id', StringType(), True)
                            , StructField('condition_concept_value', StringType(), True)
                            , StructField('condition_start_date', TimestampType(), True)
                            , StructField('condition_end_date', TimestampType(), True)
                            , StructField('condition_type_concept_id', StringType(), True)
                            , StructField('condition_source_value', StringType(), True)
                            , StructField('condition_source_concept_id', StringType(), True)
                            , StructField('provider_id', StringType(), True)
                            , StructField('provider_fname', StringType(), True)
                            , StructField('provider_lname', StringType(), True)
                            , StructField('provider_entry_date', TimestampType(), True)
                        ])), True)
                        , StructField('medications', ArrayType(StructType([
                            StructField('drug_concept_id', StringType(), True)
                            , StructField('drug_concept_value', StringType(), True)
                            , StructField('drug_source_value', StringType(), True)
                            , StructField('drug_source_concept_id', StringType(), True)
                            , StructField('drug_exposure_start_date', TimestampType(), True)
                            , StructField('drug_exposure_end_date', TimestampType(), True)
                            , StructField('dose_unit_concept_id', StringType(), True)
                            , StructField('dose_unit_source_value', FloatType(), True)
                            , StructField('route_concept_id', StringType(), True)
                            , StructField('route_source_value', StringType(), True)
                            , StructField('drug_supply_quantity', FloatType(), True)
                            , StructField('drug_supply_refill', FloatType(), True)
                            , StructField('provider_id', StringType(), True)
                            , StructField('provider_fname', StringType(), True)
                            , StructField('provider_lname', StringType(), True)
                            , StructField('provider_entry_date', TimestampType(), True)
                        ])), True)
                    ]))


df_ccda_docs = (spark.table(f"{catalog}.{db_bronze}.{table}")).join(spark.table(f"{catalog}.{db_silver}.ccda_docs_patient"), on="document_id", how="left_anti") ###NEW

display(df_ccda_docs)

df_ccda_parsed = (df_ccda_docs
    .withColumn("parsed", parse_ccda_udf(col("xml_string")))
    .select("patient_id", "document_id", "parsed.*")
)     

df_ccda_docs_problem = (df_ccda_parsed
    .withColumn("problem", explode(col("problems")))
    .select("patient_id", "document_id", "problem.*")
)

df_ccda_docs_medication = (df_ccda_parsed
    .withColumn("medication", explode(col("medications")))
    .select("patient_id", "document_id", "medication.*")
)

df_ccda_docs_patient = df_ccda_parsed.withColumn("problem_count", expr("size(problems)")).withColumn("medication_count", expr("size(medications)")).drop(col("problems")).drop(col("medications")).distinct()
df_ccda_docs_problem = df_ccda_docs_problem.withColumn("problem_id", sha2(concat_ws("||", *[coalesce(col(x), lit('')) for x in df_ccda_docs_problem.columns]), 256)).distinct()
df_ccda_docs_medication = df_ccda_docs_medication.withColumn("medication_id", sha2(concat_ws("||", *[coalesce(col(x), lit('')) for x in df_ccda_docs_medication.columns]), 256)).distinct()

# display(df_ccda_docs_patient)
# display(df_ccda_docs_problem)
# display(df_ccda_docs_medication)

# COMMAND ----------

# DBTITLE 1,Write to silver
df_ccda_docs_patient.write.mode("overwrite").saveAsTable(f"{catalog}.{db_silver}.{table}_patient")

df_ccda_docs_problem.write.mode("overwrite").saveAsTable(f"{catalog}.{db_silver}.{table}_problem")

df_ccda_docs_medication.write.mode("overwrite").saveAsTable(f"{catalog}.{db_silver}.{table}_medication")
