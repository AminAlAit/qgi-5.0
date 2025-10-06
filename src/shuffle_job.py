import os
import logging
import pandas as pd

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """
    Reads a single Parquet file from an S3 source, and rewrites it to a new
    S3 destination, partitioned by the core temporal keys.
    """
    # --- 1. Load configuration from environment variables ---
    try:
        s3_bucket = os.environ['S3_BUCKET']
        s3_input_key = os.environ['S3_INPUT_KEY']
        s3_output_prefix = os.environ['S3_OUTPUT_PREFIX']
        logging.info(f"S3_BUCKET: {s3_bucket}")
        logging.info(f"S3_INPUT_KEY: {s3_input_key}")
        logging.info(f"S3_OUTPUT_PREFIX: {s3_output_prefix}")
    except KeyError as e:
        logging.error(f"FATAL: Missing required environment variable: {e}")
        raise

    # --- 2. Define S3 paths and partition columns ---
    input_s3_path = f"s3://{s3_bucket}/{s3_input_key}"
    output_s3_path = f"s3://{s3_bucket}/{s3_output_prefix}"
    
    # These are the 5 keys that define a unique temporal signature for a "Pattern"
    # and will be used to partition the data on S3.
    partition_columns = [
        'country_a_id',   # [cite: 245]
        'country_b_id',   # [cite: 246]
        'length',         # [cite: 247]
        'start_year_a',   # [cite: 248]
        'start_year_b'    # [cite: 249]
    ]
    
    logging.info(f"Reading from: {input_s3_path}")
    logging.info(f"Writing to: {output_s3_path}")
    logging.info(f"Partitioning by: {partition_columns}")

    try:
        # --- 3. Read the source Parquet file into a DataFrame ---
        df = pd.read_parquet(input_s3_path, engine='pyarrow')
        
        if df.empty:
            logging.warning(f"Input file {input_s3_path} is empty or contains no data. Nothing to do.")
            return

        # --- 4. Write the DataFrame to the new S3 location with partitioning ---
        # This is the core of the "distributed shuffle". Pandas and PyArrow handle
        # the creation of the directory structure on S3 based on the unique values
        # in the `partition_cols`.
        df.to_parquet(
            output_s3_path,
            engine='pyarrow',
            partition_cols=partition_columns,
            compression='snappy' # Snappy is a good balance of speed and size
        )

        logging.info(f"Successfully processed and wrote {len(df)} rows from {input_s3_path}")

    except Exception as e:
        logging.error(f"An error occurred during processing of {input_s3_path}: {e}")
        # Re-raise the exception to ensure the AWS Batch job is marked as FAILED
        raise

if __name__ == "__main__":
    main()
