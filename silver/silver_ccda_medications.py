# Databricks notebook source
# MAGIC %md
# MAGIC # Requirements
# MAGIC 1. Analyze and parse the content from the documents into their respective domains. 
# MAGIC     - Medications
# MAGIC     - Problems
# MAGIC
# MAGIC # Assumptions
# MAGIC 1. 
# MAGIC
# MAGIC # Improvements
# MAGIC 1. 

# COMMAND ----------

# DBTITLE 1,Set Variables
catalog = "milliman_data_lake"
database = "db_bronze"
table = "ccda_docs"

# COMMAND ----------

# DBTITLE 1,Process CCDA XML
import xml.etree.ElementTree as et
from pprint import pprint
import re
from datetime import datetime as dt
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, TimestampType, Row

def parse_birth_time(birth_time):
    """
    Parses a birth time string in the format 'YYYYMMDD' and returns a date object.

    Args:
        birth_time (str): Birth time string in the format 'YYYYMMDD'.

    Returns:
        date: Parsed date object or None if the format is incorrect.
    """

    try:
        return dt.strptime(birth_time, '%Y%m%d').date() if re.match(r'\d{8}$', birth_time) else None
    except Exception:
        return None

##### generalize this function and the first function to convert any string value to a date or timestamp value
def parse_effective_time(effective_time):
    """
    Parses an effective time string in the format 'YYYYMMDDHHMMSS' and returns a datetime object.

    Args:
        effective_time (str): Effective time string in the format 'YYYYMMDDHHMMSS'.

    Returns:
        datetime: Parsed datetime object or None if the format is incorrect.
    """

    try:
        return dt.strptime(effective_time, '%Y%m%d%H%M%S') if re.match(r'\d{14}$', effective_time) else None
    except Exception:
        return None

def parse_ccda(xml_content):
    """
    Parses a CCDA XML file content and extracts relevant information.

    Args:
        xml_content (str): Content of the CCDA XML file.

    Returns:
        Row: A Row object containing extracted data from the CCDA.
    """

    xml_tree = et.ElementTree(et.fromstring(xml_content))
    xml_root = xml_tree.getroot()
    
    # Define namespaces
    namespaces = {'hl7': 'urn:hl7-org:v3'}
    
    # Data Quality Checks
    null_check_get = lambda x, y: x.get(y) if x is not None else None
    null_check_text = lambda x: x.text if x is not None else None
    null_check_int = lambda x: int(x.text) if x is not None else None
    # parse_date = lambda x: dt.strptime(x, '%Y%m%d%H%M%S') if x != '' else None

    # Extract data using XPath
    document_type = null_check_get(xml_root.find('.//hl7:code[@code="34133-9"]', namespaces), 'displayName')
    effective_time = parse_effective_time(null_check_get(xml_root.find('.//hl7:effectiveTime', namespaces), 'value'))
    document_title = null_check_text(xml_root.find('.//hl7:title', namespaces))
    patient_fname = null_check_text(xml_root.find('.//hl7:patient/hl7:name/hl7:given', namespaces))
    patient_lname = null_check_text(xml_root.find('.//hl7:patient/hl7:name/hl7:family', namespaces))
    gender_code = null_check_get(xml_root.find('.//hl7:patient/hl7:administrativeGenderCode', namespaces), 'code')
    gender = null_check_get(xml_root.find('.//hl7:patient/hl7:administrativeGenderCode', namespaces), 'displayName')
    race = null_check_get(xml_root.find('.//hl7:patient/hl7:raceCode', namespaces), 'displayName')
    ethnicity = null_check_get(xml_root.find('.//hl7:patient/hl7:ethnicGroupCode', namespaces), 'displayName')
    birth_time = null_check_get(xml_root.find('.//hl7:patient/hl7:birthTime', namespaces), 'value')
    birth_date = parse_birth_time(birth_time)
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
    provider_name = null_check_text(xml_root.find('.//hl7:providerOrganization/hl7:name', namespaces))
    provider_phone = null_check_get(xml_root.find('.//hl7:providerOrganization/hl7:telecom', namespaces), 'value')
    provider_address = null_check_text(xml_root.find('.//hl7:providerOrganization/hl7:addr/hl7:streetAddressLine', namespaces))
    provider_city = null_check_text(xml_root.find('.//hl7:providerOrganization/hl7:addr/hl7:city', namespaces))
    provider_state = null_check_text(xml_root.find('.//hl7:providerOrganization/hl7:addr/hl7:state', namespaces))
    provider_zip_code = null_check_int(xml_root.find('.//hl7:providerOrganization/hl7:addr/hl7:postalCode', namespaces))
    provider_country = null_check_text(xml_root.find('.//hl7:providerOrganization/hl7:addr/hl7:country', namespaces))

    all_sections = xml_root.findall('.//hl7:section', namespaces)
    
    # '''
    # Filter for sections with root="2.16.840.1.113883.10.20.22.2.5.1" -> Problem List

    if all_sections is not None:
        problem_section_list = []
        for section in all_sections:
            print(section)
            problem_section = section.findall('./hl7:templateId[@root="2.16.840.1.113883.10.20.22.2.5.1"]', namespaces)
            print(problem_section)
            if problem_section:
                problem_section_list.append(section)

        print(f"problem_section_list: {len(problem_section_list)}")

        if problem_section_list is not None:
            
            problem_entry_list = []
            for problem in problem_section_list:
                problem_entry_list.extend(problem.findall('./hl7:entry/hl7:act', namespaces))

            if problem_entry_list is not None and len(problem_entry_list) > 0:
                print(f"problem_entry: {len(problem_entry_list)}")

                for i, problem in enumerate(problem_entry_list):

                    no_problem_check = null_check_get(problem.find('./hl7:entryRelationship/hl7:observation/hl7:value', namespaces), 'nullFlavor')

                    print(f"no_problem_check: {no_problem_check}")

                    if no_problem_check is None or no_problem_check != "NI":
                        #'''
                        print(f"\nProblem #{i+1}:")
                        
                        concern_type_code = null_check_get(problem.find('./hl7:code', namespaces), 'code')
                        concern_type_name = null_check_get(problem.find('./hl7:code', namespaces), 'displayName')
                        print(f"Concern type code: {concern_type_code}")
                        print(f"Concern type name: {concern_type_name}")

                        problem_type_code = null_check_get(problem.find('./hl7:entryRelationship/hl7:observation/hl7:code', namespaces), 'code')
                        problem_type_name = null_check_get(problem.find('./hl7:entryRelationship/hl7:observation/hl7:code', namespaces), 'displayName')
                        print(f"Problem type code: {problem_type_code}")
                        print(f"Problem type name: {problem_type_name}")

                        problem_snomed = null_check_get(problem.find('./hl7:entryRelationship/hl7:observation/hl7:value', namespaces), 'code')
                        problem_name = null_check_get(problem.find('./hl7:entryRelationship/hl7:observation/hl7:value', namespaces), 'displayName')
                        print(f"Problem SNOMED code: {problem_snomed}")
                        print(f"Problem name: {problem_name}")

                        problem_status = null_check_get(problem.find('./hl7:statusCode', namespaces), 'code')
                        print(f"Problem status: {problem_status}")

                        problem_start_date = null_check_get(problem.find('./hl7:entryRelationship/hl7:observation/effectiveTime/low', namespaces), 'value')
                        problem_end_date = null_check_get(problem.find('./hl7:entryRelationship/hl7:observation/effectiveTime/high', namespaces), 'value')
                        print(f"Problem start date: {problem_start_date}")
                        print(f"Problem end date: {problem_end_date}")

                        problem_entry_date = null_check_get(problem.find('./hl7:entryRelationship/hl7:observation/hl7:author/hl7:time', namespaces), 'value')
                        print(f"Problem entry date: {problem_entry_date}")            
                        #'''
                    else:
                        print(f"No problem found in section")

            else:
                print("No problem entry sections")
                    
        else:
            pass
            ### ADD LOGIC FOR NO PROBLEM SECTION

    else:
        pass
        ### ADD LOGIC FOR NO SECTIONS

    # '''
    # Filter for sections with code="10160-0"
    medication_section = []
    for section in all_sections:
        codes = section.findall('./hl7:code[@code="10160-0"]', namespaces)
        if codes:
            medication_section.append(section)

    # Collect all consumable elements that are descendants of these specific sections
    medications = []

    for medication in medication_section:
        medications.extend(medication.findall('.//hl7:consumable', namespaces))
        # consumables = (medication.findall('.//hl7:consumable', namespaces))
        # target_consumables.extend(consumables)

    # Print results
    print(f"Found {len(medications)} consumable elements in sections with code 10160-0")
    # print(target_consumables)

    # Process each consumable
    
    for i, consumable in enumerate(medications):
        print(f"\nConsumable #{i+1}:")
        
        manufactured_product = consumable.find('./hl7:manufacturedProduct', namespaces)
        if manufactured_product is not None:
            material = manufactured_product.find('./hl7:manufacturedMaterial', namespaces)
            if material is not None:
                code = material.find('./hl7:code', namespaces)
                if code is not None:
                    display_name = code.get('displayName')
                    if display_name is not None:
                        print(f"Display Name: {display_name}")
                    else:
                        translation = code.find('./hl7:translation[@displayName]', namespaces)
                        if translation is not None:
                            display_name = translation.get('displayName')
                            print(f"Display Name: {display_name}")
                        else:
                            print(f"Display Name: not available")

                    original_text = code.find('./hl7:originalText', namespaces)
                    if original_text is not None:
                        reference = original_text.find('./hl7:reference', namespaces)
                        if reference is not None:
                            print(f"Reference value: {reference.get('value')}")
    

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
        , provider_name = provider_name
        , provider_phone = provider_phone
        , provider_address = provider_address
        , provider_city = provider_city
        , provider_state = provider_state
        , provider_zip_code = provider_zip_code
        , provider_country = provider_country
    )
    #'''
    return 0

# Register the parse_ccda function as a UDF
parse_ccda_udf = udf(lambda xml: parse_ccda(xml), 
                     StructType([
                         StructField('document_type', StringType(), True),
                         StructField('effective_time', TimestampType(), True),
                         StructField('document_title', StringType(), True),
                         StructField('patient_fname', StringType(), True),
                         StructField('patient_lname', StringType(), True),
                         StructField('gender_code', StringType(), True),
                         StructField('gender', StringType(), True),
                         StructField('race', StringType(), True),
                         StructField('ethnicity', StringType(), True),
                         StructField('birth_time', StringType(), True),
                         StructField('birth_date', DateType(), True),
                         StructField('street_address', StringType(), True),
                         StructField('city', StringType(), True),
                         StructField('state', StringType(), True),
                         StructField('zip_code', IntegerType(), True),
                         StructField('country', StringType(), True),
                         StructField('home_phone', StringType(), True),
                         StructField('mobile_phone', StringType(), True),
                         StructField('marital_code', StringType(), True),
                         StructField('marital_status', StringType(), True),
                         StructField('religion', StringType(), True),
                         StructField('language', StringType(), True),
                         StructField('provider_name', StringType(), True),
                         StructField('provider_phone', StringType(), True),
                         StructField('provider_address', StringType(), True),
                         StructField('provider_city', StringType(), True),
                         StructField('provider_state', StringType(), True),
                         StructField('provider_zip_code', IntegerType(), True),
                         StructField('provider_country', StringType(), True)
                     ]))

### REMOVE WHERE STATEMENT
# Read the CCDA bronze table into a DataFrame
df_ccda_docs = spark.sql(f'''select * from {catalog}.{database}.{table} where file_name = "0ww66gj1-5627-705o-1719-2710c04560aa_034c3eab764e9bf9dae33996c3371e5e64ab73b3_masked.xml"''')   
# 0ww66gj1-5627-705o-1719-2710c04560aa_034c3eab764e9bf9dae33996c3371e5e64ab73b3_masked.xml 24 problems; 24 medications
# 0b28oik4-0280-7rbo-hd6r-i8v10m92yaw2_cf4b24658567e79ef660bbce6f71903447b14807_masked.xml 0 problems->value:nullFlavor; 33 medications
# 0b28oik4-0280-7rbo-hd6r-i8v10m92yaw2_cfdac11058c4d921f571adde03b323009c268be9_masked.xml 0 problems->entry=0; 0 medications->entry=0
# 0b28oik4-0280-7rbo-hd6r-i8v10m92yaw2_d7ecb63907d2552f4bb285f420976f2d3a303059_masked.xml 22 problems; 10 medications
# 0b28oik4-0280-7rbo-hd6r-i8v10m92yaw2_de96a0117cf5e9ecc7184ab686e44e8c3c37c141_masked.xml 31 problems; 9 medications
# 0b28oik4-0280-7rbo-hd6r-i8v10m92yaw2_fed90fdc4786a5998b7de14c46d22a5f1001536d_masked.xml 0 problems->entry=0; 0 medications->entry=0

# display(df_ccda_docs)

df_temp_return = parse_ccda(df_ccda_docs.first().xml_string)
display(df_temp_return)


# Apply the UDF to the DataFrame
# df_parsed_udf = df_ccda_docs.withColumn("parsed_data", parse_ccda_udf(col("xml_string")))

# Select the parsed data columns
# df_ccda_data = df_parsed_udf.selectExpr("patient_id", "document_id", "parsed_data.*")

# display(df_ccda_data)

# COMMAND ----------

# Create table for medications
spark.sql(f"""
CREATE OR REPLACE TABLE {silver_db}.medications AS
WITH medication_sections AS (
    SELECT
        document_id,
        patient_id,
        xpath_string(raw_xml, '//section[code/@displayName="Medications"]/entry/substanceAdministration/consumable/manufacturedProduct/manufacturedMaterial/code/@displayName') AS medication_name,
        xpath_string(raw_xml, '//section[code/@displayName="Medications"]/entry/substanceAdministration/effectiveTime/low/@value') AS start_date,
        xpath_string(raw_xml, '//section[code/@displayName="Medications"]/entry/substanceAdministration/effectiveTime/high/@value') AS end_date,
        xpath_string(raw_xml, '//section[code/@displayName="Medications"]/entry/substanceAdministration/doseQuantity/@value') AS dose_value,
        xpath_string(raw_xml, '//section[code/@displayName="Medications"]/entry/substanceAdministration/doseQuantity/@unit') AS dose_unit,
        creation_time,
        ingestion_timestamp
    FROM {bronze_db}.ccda_documents_raw
)
SELECT
    document_id,
    patient_id,
    medication_name,
    start_date,
    end_date,
    dose_value,
    dose_unit,
    creation_time,
    current_timestamp() AS processing_timestamp
FROM medication_sections
WHERE medication_name IS NOT NULL
""")

# Create table for problems/conditions
spark.sql(f"""
CREATE OR REPLACE TABLE {silver_db}.problems AS
SELECT
    document_id,
    patient_id,
    xpath_string(raw_xml, '//section[code/@displayName="Problem List"]/entry/act/entryRelationship/observation/code/@displayName') AS problem_name,
    xpath_string(raw_xml, '//section[code/@displayName="Problem List"]/entry/act/entryRelationship/observation/effectiveTime/low/@value') AS onset_date,
    xpath_string(raw_xml, '//section[code/@displayName="Problem List"]/entry/act/entryRelationship/observation/effectiveTime/high/@value') AS resolution_date,
    xpath_string(raw_xml, '//section[code/@displayName="Problem List"]/entry/act/entryRelationship/observation/value/@displayName') AS status,
    creation_time,
    current_timestamp() AS processing_timestamp
FROM {bronze_db}.ccda_documents_raw
""")

# Create table for procedures
spark.sql(f"""
CREATE OR REPLACE TABLE {silver_db}.procedures AS
SELECT
    document_id,
    patient_id,
    xpath_string(raw_xml, '//section[code/@displayName="Procedures"]/entry/procedure/code/@displayName') AS procedure_name,
    xpath_string(raw_xml, '//section[code/@displayName="Procedures"]/entry/procedure/effectiveTime/@value') AS procedure_date,
    xpath_string(raw_xml, '//section[code/@displayName="Procedures"]/entry/procedure/performer/assignedEntity/id/@extension') AS provider_id,
    creation_time,
    current_timestamp() AS processing_timestamp
FROM {bronze_db}.ccda_documents_raw
""")

print("Silver Layer processing completed")



# COMMAND ----------

# DBTITLE 1,Not used - DELETE
import xml.etree.ElementTree as ET

def parse_ccda(xml_file):
    """
    Parses a CCDA XML file and extracts relevant information.
    
    Args:
        xml_file (str): Path to the CCDA XML file.
    
    Returns:
        dict: A dictionary containing extracted data from the CCDA.
    """
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    # Define namespaces (if needed)
    namespaces = {'hl7': 'urn:hl7-org:v3'}
    # ET.register_namespace('hl7', 'urn:hl7-org:v3')
    
    # Extract data using XPath
    patient_fname = root.find('.//hl7:patient/hl7:name/hl7:given', namespaces).text
    patient_lname = root.find('.//hl7:patient/hl7:name/hl7:family', namespaces).text

    # patient_fname = root.find('.//patient/name/given').text
    # patient_lname = root.find('.//patient/name/hamily').text
    # '''
    # Extracting multiple elements
    # medications = root.findall('.//hl7:section[hl7:code[@code="10160-0"]]', namespaces)
    all_sections = root.findall('.//hl7:section', namespaces)

    # Filter for sections with code="10160-0"
    sections_with_target_code = []
    for section in all_sections:
        codes = section.findall('./hl7:code[@code="10160-0"]', namespaces)
        if codes:
            sections_with_target_code.append(section)

    # Collect all consumable elements that are descendants of these specific sections
    target_consumables = []

    for section in sections_with_target_code:
        consumables = section.findall('.//hl7:consumable', namespaces)
        target_consumables.extend(consumables)

    # Print results
    print(f"Found {len(target_consumables)} consumable elements in sections with code 10160-0")
    print(target_consumables)

    # Process each consumable
    for i, consumable in enumerate(target_consumables):
        print(f"\nConsumable #{i+1}:")
        
        manufactured_product = consumable.find('./hl7:manufacturedProduct', namespaces)
        if manufactured_product is not None:
            material = manufactured_product.find('./hl7:manufacturedMaterial', namespaces)
            if material is not None:
                code = material.find('./hl7:code', namespaces)
                if code is not None:
                    display_name = code.get('displayName')
                    if display_name is not None:
                        print(f"Display Name: {display_name}")
                    else:
                        translation = code.find('./hl7:translation[@displayName]', namespaces)
                        if translation is not None:
                            display_name = translation.get('displayName')
                            print(f"Display Name: {display_name}")
                        else:
                            print(f"Display Name: not available")

                    original_text = code.find('./hl7:originalText', namespaces)
                    if original_text is not None:
                        reference = original_text.find('./hl7:reference', namespaces)
                        if reference is not None:
                            print(f"Reference value: {reference.get('value')}")
                    
    #'''
    # Structure the extracted data
    ccda_data = {
        'patient_fname': patient_fname
        ,'patient_lname': patient_lname
        # ,'medications': medications
    }
    
    return ccda_data


file_path = 's3://millimandatalake/raw/ccda/0ww66gj1-5627-705o-1719-2710c04560aa/0ww66gj1-5627-705o-1719-2710c04560aa_034c3eab764e9bf9dae33996c3371e5e64ab73b3_masked.xml'
parsed_data = parse_ccda(file_path)
print(parsed_data)


# COMMAND ----------

# DBTITLE 1,Not used - DELETE
import boto3

def read_s3_file(bucket_name, file_key):
        aws_access_key_id = dbutils.secrets.get(scope="aws-s3-secret", key="access-key")
        aws_secret_access_key = dbutils.secrets.get(scope="aws-s3-secret", key="secret-key")
        
        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        try:
            obj = s3.get_object(Bucket=bucket_name, Key=file_key)
            return obj['Body'].read().decode('utf-8')
        except NoCredentialsError:
            print("Credentials not available")
            return None

bucket_name = 'millimandatalake'
file_key = 'raw/ccda/0ww66gj1-5627-705o-1719-2710c04560aa/0ww66gj1-5627-705o-1719-2710c04560aa_034c3eab764e9bf9dae33996c3371e5e64ab73b3_masked.xml'
xml_content = read_s3_file(bucket_name, file_key)
