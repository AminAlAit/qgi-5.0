"""
SDCI Finder for AWS Batch

This script runs as a single job in AWS Batch. It processes one indicator,
which is specified by an environment variable. It reads the indicator data
from S3, performs the correlation analysis, and saves the results back to S3.
"""

import os
import logging
from dataclasses import dataclass
from typing import List
from io import BytesIO
import boto3
import pandas as pd
import numpy as np
from scipy.stats import pearsonr


S3_BUCKET = os.environ.get('S3_BUCKET')
S3_INPUT_PREFIX = os.environ.get('S3_INPUT_PREFIX') # e.g., 'pipeline-A-indicator-extractor/output/csv'
S3_OUTPUT_PREFIX = os.environ.get('S3_OUTPUT_PREFIX') # e.g., 'pipeline-B-scdi-finder/output'
INDICATOR_ID = os.environ.get('INDICATOR_ID') # The specific indicator this job will process
MIN_LENGTH = int(os.environ.get('MIN_LENGTH', 3))
CORRELATION_THRESHOLD = float(os.environ.get('CORRELATION_THRESHOLD', 0.5))

# --- Initialize AWS S3 Client and Logger ---
s3_client = boto3.client('s3')
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@dataclass
class SDCI:
    """Represents a Single Dyadic Correlating Index."""
    country_a_id: int
    country_b_id: int
    length: int
    start_year_a: int
    start_year_b: int
    correlation: float

def run_pipeline():
    """Main execution function for the job."""
    if not all([S3_BUCKET, S3_INPUT_PREFIX, S3_OUTPUT_PREFIX, INDICATOR_ID]):
        logging.error("Missing critical environment variables.")
        raise ValueError("Missing critical environment variables.")

    logging.info(f"--- Starting SDCI Finder Job for Indicator: {INDICATOR_ID} ---")

    # 1. Load data from S3
    indicator_df, countries_df = load_data()
    country_name_to_id = {row["name_1"]: row["id"] for _, row in countries_df.iterrows()}
    
    # 2. Preprocess data
    countries = indicator_df["country"].unique()
    preprocessed_data = preprocess_data(indicator_df, countries)
    logging.info(f"Successfully preprocessed data for {len(preprocessed_data)} countries.")

    # 3. Find and save SDCIs
    find_and_save_sdcis(preprocessed_data, country_name_to_id)
    
    logging.info(f"--- Job for Indicator {INDICATOR_ID} completed successfully. ---")

def load_data() -> (pd.DataFrame, pd.DataFrame):
    """Loads the main indicator file and the countries mapping file from S3."""
    try:
        # Load main indicator data
        indicator_key = f"{S3_INPUT_PREFIX}/{INDICATOR_ID}/{INDICATOR_ID}.csv.gz"
        logging.info(f"Loading indicator data from s3://{S3_BUCKET}/{indicator_key}")
        indicator_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=indicator_key)
        indicator_df = pd.read_csv(indicator_obj['Body'], compression='gzip')

        # Load countries mapping data
        countries_key = "pipeline-A-indicator-extractor/input/countries.csv" # Assuming this path is constant
        logging.info(f"Loading countries mapping from s3://{S3_BUCKET}/{countries_key}")
        countries_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=countries_key)
        countries_df = pd.read_csv(countries_obj['Body'])

        return indicator_df, countries_df
    except Exception as e:
        logging.error(f"Failed to load data from S3: {e}", exc_info=True)
        raise

def preprocess_data(df: pd.DataFrame, countries: list) -> list:
    """Prepares data for each country, handling NaNs."""
    preprocessed_data = []
    for country in countries:
        country_data = df[df["country"] == country].sort_values("year")
        values = country_data["value"].values.copy()

        # Simple NaN handling (as per original script)
        nan_indices = pd.isna(values)
        if np.sum(nan_indices) > 1: continue
        if np.sum(nan_indices) == 1:
            nan_pos = np.where(nan_indices)[0][0]
            if nan_pos == 0: values[nan_pos] = values[nan_pos + 1]
            elif nan_pos == len(values) - 1: values[nan_pos] = values[nan_pos - 1]
            else: values[nan_pos] = (values[nan_pos - 1] + values[nan_pos + 1]) / 2
        
        preprocessed_data.append({"country": country, "data": country_data, "values": values})
    return preprocessed_data

def find_and_save_sdcis(preprocessed_data: list, country_name_to_id: dict):
    """Finds all SDCIs and saves results to S3, with checkpointing."""
    num_countries = len(preprocessed_data)
    for i, country_a_data in enumerate(preprocessed_data):
        country_a_name = country_a_data["country"]
        
        # S3 Checkpointing: Check if output already exists
        output_key = f"{S3_OUTPUT_PREFIX}/{INDICATOR_ID}/{country_a_name}.parquet"
        try:
            s3_client.head_object(Bucket=S3_BUCKET, Key=output_key)
            logging.info(f"Skipping '{country_a_name}' ({i+1}/{num_countries}): Output already exists at s3://{S3_BUCKET}/{output_key}")
            continue
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] != '404': raise # Re-raise if it's not a "file not found" error

        logging.info(f"Processing country: {country_a_name} ({i+1}/{num_countries})")
        country_a_sdcis = []
        values_a = country_a_data["values"]

        # The core brute-force algorithm (unchanged)
        for segment_len in range(MIN_LENGTH, len(values_a) + 1):
            for start_a in range(len(values_a) - segment_len + 1):
                segment_a = values_a[start_a : start_a + segment_len]
                for country_b_data in preprocessed_data[i + 1 :]:
                    values_b = country_b_data["values"]
                    if len(values_b) < segment_len: continue
                    for start_b in range(len(values_b) - segment_len + 1):
                        segment_b = values_b[start_b : start_b + segment_len]
                        if np.std(segment_a) == 0 or np.std(segment_b) == 0: continue
                        correlation, _ = pearsonr(segment_a, segment_b)
                        if correlation >= CORRELATION_THRESHOLD:
                            sdc = SDCI(
                                country_a_id=country_name_to_id.get(country_a_name, 0),
                                country_b_id=country_name_to_id.get(country_b_data["country"], 0),
                                length=segment_len,
                                correlation=correlation,
                                start_year_a=country_a_data["data"].iloc[start_a]["year"],
                                start_year_b=country_b_data["data"].iloc[start_b]["year"],
                            )
                            country_a_sdcis.append(sdc)
        
        # Save results to S3 if any SDCIs were found
        if country_a_sdcis:
            logging.info(f"Found and saving {len(country_a_sdcis)} SDCIs for country '{country_a_name}'")
            sdcis_df = convert_sdcis_to_df(country_a_sdcis)
            
            # Write to a parquet file in memory
            parquet_buffer = BytesIO()
            sdcis_df.to_parquet(parquet_buffer, index=False, compression="brotli")
            
            # Upload the in-memory file to S3
            s3_client.put_object(Bucket=S3_BUCKET, Key=output_key, Body=parquet_buffer.getvalue())

def convert_sdcis_to_df(sdcis: List[SDCI]) -> pd.DataFrame:
    """Converts a list of SDCI objects to a memory-optimized DataFrame."""
    if not sdcis: return pd.DataFrame()
    sdcis_df = pd.DataFrame([sdc.__dict__ for sdc in sdcis])
    return sdcis_df.astype({
        "country_a_id": "uint16", "country_b_id": "uint16",
        "length": "uint16", "start_year_a": "uint16", "start_year_b": "uint16",
        "correlation": "float32",
    })

if __name__ == "__main__":
    run_pipeline()
