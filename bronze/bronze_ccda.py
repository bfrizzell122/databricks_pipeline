# Databricks notebook source
# MAGIC %md
# MAGIC # Requirements
# MAGIC 1. Analyze and parse the content from the documents into their respective domains. 
# MAGIC     - Medications
# MAGIC     - Problems
# MAGIC
# MAGIC #Assumptions
# MAGIC 1. CCDA XML files will remain small and can be stored as a string in a single column.
# MAGIC 2. Data does not need to be masked.
# MAGIC
# MAGIC # Improvements
# MAGIC 1. In a production environmentm, there are several approaches that can mitigate any performance issues related to larger CCDA XML files. Options 1 & 2 would be preferred for streaming CCDA documents when we cannot retain the physical file. Option 3 is better when we can retain all the files but requires parsing for every read which adds complexity and performance implications.
# MAGIC     1. Storing the XML as binary to save space and could also improve reads.
# MAGIC     2. Chunking XML data into fixed size columns.
# MAGIC     3. Storing the file path in the table and reading the data at run-time.
# MAGIC
# MAGIC

# COMMAND ----------

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


file_path = '0ww66gj1-5627-705o-1719-2710c04560aa_034c3eab764e9bf9dae33996c3371e5e64ab73b3_masked.xml'
parsed_data = parse_ccda(file_path)
print(parsed_data)

