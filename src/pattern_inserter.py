import os
import boto3
import logging
import time
import uuid


# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

region = os.environ['AWS_REGION']
s3_output_location = os.environ.get('S3_OUTPUT_LOCATION')

if not region or not s3_output_location:
    raise Exception("FATAL: AWS_REGION and S3_OUTPUT_LOCATION environment variables are required.")


athena_client = boto3.client('athena', region_name=region)

# --- Configuration (can also be passed as env vars if they change) ---
# TODO: convert to env vars
DB_NAME = "qgi_data"
SCDIS_TABLE = "scdis"
COUNTRIES_TABLE = "countries_parquet"
PATTERNS_TABLE_NAME = "patterns"

def run_athena_query(query_string, output_location):
    """Executes a query and waits for it to succeed."""
    logger.info(f"Executing query: \n{query_string}")

    result_output_location = f"{output_location.rstrip('/')}/batch_query_results/{uuid.uuid4()}"

    # Workgroup and OutputLocation can be set on the Workgroup itself
    query_execution = athena_client.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={'Database': DB_NAME},
        ResultConfiguration={'OutputLocation': result_output_location}
    )
    query_execution_id = query_execution['QueryExecutionId']

    while True:
        stats = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = stats['QueryExecution']['Status']['State']
        if status in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
            if status == 'SUCCEEDED':
                logger.info(f"Query {query_execution_id} succeeded.")
                return
            else:
                error_message = stats['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                raise Exception(f"Query {query_execution_id} failed: {error_message}")
        time.sleep(5)

country_name = os.environ['COUNTRY_NAME']
logger.info(f"--- Starting Batch Job for country: {country_name} ---")

insert_query = f"""
INSERT INTO {DB_NAME}.{PATTERNS_TABLE_NAME}
SELECT
    c_b.country_name AS country_b_name, s.length, s.start_year_a, s.start_year_b,
    COUNT(*) AS indicator_count, AVG(s.correlation) AS avg_correlation,
    AVG(s.correlation) * CAST(s.length AS double) * COUNT(*) AS pattern_power_score,
    c_a.country_name, c_a.id AS country_a_id
FROM {DB_NAME}.{SCDIS_TABLE} AS s
JOIN {DB_NAME}.{COUNTRIES_TABLE} AS c_a ON s.country_a_id = c_a.id
JOIN {DB_NAME}.{COUNTRIES_TABLE} AS c_b ON s.country_b_id = c_b.id
WHERE c_a.country_name = '{country_name.replace("'", "''")}'
GROUP BY c_a.country_name, c_b.country_name, s.length, s.start_year_a, s.start_year_b, c_a.id
HAVING COUNT(*) > 1
"""
run_athena_query(insert_query, s3_output_location)
logger.info(f"--- Batch Job for {country_name} completed successfully. ---")
