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
    # (This part remains the same)
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
    # (This part remains the same)
    input_s3_path = f"s3://{s3_bucket}/{s3_input_key}"
    output_s3_path = f"s3://{s3_bucket}/{s3_output_prefix}"
    partition_columns = [
        'country_a_id', 'country_b_id', 'length', 'start_year_a', 'start_year_b'
    ]
    
    logging.info(f"Reading from: {input_s3_path}")

    try:
        # --- 3. Read the source Parquet file into a DataFrame ---
        # (This part remains the same)
        df = pd.read_parquet(input_s3_path, engine='pyarrow')
        
        if df.empty:
            logging.warning(f"Input file {input_s3_path} is empty or contains no data. Nothing to do.")
            return

        # --- 4. NEW: Dynamically calculate the number of partitions required ---
        num_unique_partitions = df.drop_duplicates(subset=partition_columns).shape[0]
        logging.info(f"This file will be written into {num_unique_partitions} unique partitions.")

        # --- 5. Write the DataFrame with the dynamic limit ---
        df.to_parquet(
            output_s3_path,
            engine='pyarrow',
            partition_cols=partition_columns,
            compression='snappy',
            # Use the dynamically calculated number as the limit
            max_partitions=num_unique_partitions
        )

        logging.info(f"Successfully processed and wrote {len(df)} rows into {num_unique_partitions} partitions.")

    except Exception as e:
        logging.error(f"An error occurred during processing of {input_s3_path}: {e}")
        raise

if __name__ == "__main__":
    main()
