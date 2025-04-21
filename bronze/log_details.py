# Databricks notebook source
# Log job execution details
def log_job_execution():
    job_id = spark.conf.get("spark.databricks.job.id", "manual_run")
    run_id = spark.conf.get("spark.databricks.job.runId", "manual_run")
    
    # Create a job metadata table if it doesn't exist
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {gold_db}.pipeline_execution_log (
        job_id STRING,
        run_id STRING,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        status STRING,
        files_processed LONG,
        execution_details STRING
    ) USING DELTA
    """)
    
    # Get count of files processed in this run
    bronze_count_df = spark.sql(f"SELECT COUNT(*) as count FROM {bronze_db}.ccda_documents_raw")
    files_processed = bronze_count_df.collect()[0]["count"]
    
    # Log execution details
    current_time = datetime.datetime.now()
    spark.createDataFrame([
        (job_id, run_id, current_time, current_time, "COMPLETED", files_processed, 
         f"Processed bronze, silver, and gold layers at {current_time}")
    ], ["job_id", "run_id", "start_time", "end_time", "status", "files_processed", "execution_details"]) \
    .write.format("delta").mode("append").saveAsTable(f"{gold_db}.pipeline_execution_log")

