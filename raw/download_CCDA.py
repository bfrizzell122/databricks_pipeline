# Databricks notebook source
# MAGIC %md
# MAGIC # Requirements
# MAGIC 1. Download the clinical documents to your local machine. <br/>
# MAGIC     - This program was built with expediency in mind as there was a limited time available to download these files prior to expiration.
# MAGIC     - I also had not yet setup the AWS/Databricks environment.
# MAGIC     - For portability and reliability, I tried to use standard Python libraries.
# MAGIC     - For expediency and simplicity, I also used mature and widely-supported Python libraries.
# MAGIC
# MAGIC #Assumptions
# MAGIC 1. This program will only be run locally on a single CSV file so these values are hard-coded into the program.
# MAGIC     - This can easily be modified to run in the cloud by changing the file path and pointing it to an S3 bucket.
# MAGIC 2. CCDA files are small and won't require streaming downloads.
# MAGIC 3. When supporting a Common Data Model like OMOP, the data model is patient-centered rather than event-driven. This led me to organize the files by patient ID first, rather by a more standard partioning model like date.
# MAGIC
# MAGIC # Improvements
# MAGIC 1. If the CCDA files to download would continue to be identified within a CSV file at a regular cadence (daily, weekly, etc.), the simplest option would be to use Databricks COPY INTO. This provides idempotency and can be scheduled via Workflows. COPY INTO would identify new CSV "driver" files, and then execute the download function to download all CCDA files via the URLs contained within the CSV file. Because this is a heavy i/o operation, I would implement threading to execute the function and download the files, including rate-limiting features if necessary. I would also validate the downloads against the CSV driver file to ensure each download was successful.
# MAGIC 2. If the CCDA files would be directly pushed to an S3 bucket or other cloud storage by an external data provider without the use of the CSV driver file, then Spark Structured Streaming with Auto Loader could be use to process the files incrementally and idempotently. By default, this process uses "directory listing" mode which scans all files, compares them to metadata in the checkpoint location, and only processes new files. However, this could be time-consuming for extremely large volumes of files. In that case, I would implement File Notification Mode with AWS SQS which allows Auto Loader to know exactly which files are new without scanning the directory. Depending on the downstream requirements (whether data warehouse updates are needed near real-time or less frequent) you could schedule a continuous job or batch job for processing.

# COMMAND ----------

from urllib import parse, request
import os
import csv
import pandas as pd

def download_file(url, save_path, file_name):
    """
    Downloads a file from a URL and saves it to the specified path.

    Args:
        url (str): The URL of the file to download.
        save_path (str): The path where the file should be saved.
        file_name (str): The name of the file to be saved.
    """

    # Create the save directory if it doesn't exist
    if not os.path.exists(save_path):
        os.mkdir(save_path)

    full_path = os.path.join(save_path, file_name)

    try:
        # Download the file
        request.urlretrieve(url, full_path)
    except Exception as e:
        # Normally this would include logging the error, retrying the download, etc.
        # For expediency, simply process to the next download
        pass


# Source file variables
url_source_file_path = ".\SeniorDEAssessment_20250414\Assessment\\"
url_source_file_name = "ccda_pre_signed_urls.csv"
url_source_full_path = ''.join([url_source_file_path,url_source_file_name])

# Save in the Downloads folder in the current directory
save_directory = ".\Downloads" 

# Read source file
df_urls = pd.read_csv(url_source_full_path)

# Parse source file URL into components to easily extract important values: patient_id and file_name
df_urls['parsed'] = (df_urls['pre_signed_urls'].apply(lambda x: (parse.urlparse(x)).path)).str.split('/')

# Header segments for parsed URL
header_list = ['empty','folder1','folder2','patient_id','file_name']

# Extract header segments into separate columns
df_urls[header_list] = df_urls['parsed'].apply(pd.Series)

# Drop unecessary columns
df_urls.drop(columns=['parsed','empty','folder1','folder2'], inplace=True)    

# Download files
df_urls.apply(lambda x: download_file(x.pre_signed_urls, os.path.join(save_directory, x.patient_id), x.file_name), axis=1)
