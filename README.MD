# Intro
This repo contains all the necessary components to rebuild this pipeline in Databricks. There are some additional steps required to set up the Data Lake in AWS S3.

# Setup
## AWS S3
1. Create a new bucket named 'millimandatalake'. You can use an existing bucket, but you will have to modify the `bucket_name` variable in 3 files: `bronze/bronze_ccda`, `bronze/bronze_claims`, and `bronze/bronze_rx`.
2. Create the following folder structure in the S3 bucket.

#### S3 File Structure:
    - root (millimandatalake)
        - ./_checkpoints
        - ./_schemas
        - ./db_bronze
        - ./db_silver
        - ./db_gold
        - ./raw
            - ./ccda
            - ./claims
            - ./rx

3. Add the CCDA XML files to the `raw` folder. I organized them by MemberID. You can modify the code in the `raw/download_CCDA` notebook to create this folder structure automatically.
4. Add the file `data_engineer_exam_claims_final.csv` to the `raw/claims` folder.
5. Add the file `data_engineer_exam_rx_final.csv` to the `raw/rx` folder.

## Databricks
1. Create a new Unity Catalog metastore called `milliman_data_lake`. It needs to point to the S3 bucket mentioned above.
2. The file in this repo's root called `Milliman_Data_Warehouse_job.yaml` can be used to create a Databricks job that will execute the necessary notebooks to create the UC managed databases and Delta tables required to execute the pipeline. Run this job first, or run the notebooks:
    - `bronze/bronze_init`
    - `silver/silver_init`
    - `gold/gold_init`
3. The file in this repo's root called `Milliman_Pipeline_Project_job.yaml` can be used to create a Databricks job that will execute the necessary notebooks to run this data pipeline.


