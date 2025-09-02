"""
A self-contained, cloud-native ETL pipeline for fetching, transforming, and
storing indicators from the World Bank API, designed to run on AWS Lambda.
"""

import os
import logging
import json
from dataclasses import dataclass
from typing import List, Dict, Tuple
import yaml
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import wbgapi as wb
import boto3

# ## REFACTORED: Setup logger for Lambda environment
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# ## REFACTORED: Config now loads from Lambda environment variables
@dataclass(frozen=True)
class PipelineConfig:
    """A structured representation of the pipeline's configuration from env vars."""
    s3_bucket: str
    countries_csv_key: str
    output_s3_prefix: str
    indicators: List[str]

    def __post_init__(self):
        """Perform validation after the object is created."""
        if not self.indicators:
            raise ValueError("Configuration error: 'INDICATORS' env var cannot be empty.")
        if not self.s3_bucket:
            raise ValueError("Configuration error: 'S3_BUCKET' env var cannot be empty.")


@dataclass(frozen=True)
class ETLResult:
    """A summary of the completed ETL pipeline run."""
    processed_indicators: List[str]
    unmatched_economies: Dict[str, List[str]]
    output_location: str
    indicators_with_no_data: List[str]
    status: str = "Success"


class Indicator:
    """Data class to hold all information for a single indicator."""
    id: str = ""
    data: pd.DataFrame = pd.DataFrame()
    indicator_name: str = ""
    source: str = ""
    # Other metadata fields can be added back if needed

class Extractor:
    """Extracts data from the World Bank API for specified indicators."""
    def execute(self, indicator_ids: List[str]) -> List[Indicator]:
        logger.info("Starting data extraction.")
        extracted_indicators = [self._get_one_indicator(indicator_id) for indicator_id in indicator_ids]
        logger.info("Data extraction completed successfully.")
        return extracted_indicators

    def _get_one_indicator(self, indicator_id: str) -> Indicator:
        logger.info(f"Fetching indicator: {indicator_id}")
        indicator = Indicator()
        indicator.id = indicator_id
        indicator.data = wb.data.DataFrame(indicator_id)
        metadata = wb.series.metadata.get(indicator_id).metadata
        indicator.indicator_name = metadata.get("IndicatorName", "")
        indicator.source = metadata.get("Source", "")
        return indicator


class Transformer:
    """Transforms the extracted indicator data into a clean, long format."""
    def __init__(self, s3_bucket: str, countries_csv_key: str):
        self._country_mapping = self._create_country_mapping(s3_bucket, countries_csv_key)

    # ## REFACTORED: Reads the countries mapping file from S3
    def _create_country_mapping(self, s3_bucket: str, csv_key: str) -> Dict[str, str]:
        """Creates a mapping from country codes to names by reading a CSV from S3."""
        logger.info(f"Creating country mapping from s3://{s3_bucket}/{csv_key}")
        s3_uri = f"s3://{s3_bucket}/{csv_key}"
        reference_df = pd.read_csv(s3_uri)
        
        if "name_1" not in reference_df.columns:
            raise KeyError("The key 'name_1' is missing in the country reference file.")
        
        mapping = {}
        for col in reference_df.columns:
            if col != "name_1":
                temp_map = reference_df.dropna(subset=[col]).set_index(col)["name_1"].to_dict()
                mapping.update(temp_map)
        return mapping

    def execute(self, indicators: List[Indicator]) -> Tuple[List[Indicator], Dict[str, List[str]]]:
        logger.info("Starting data transformation.")
        unmatched_report = {}
        transformed_indicators = []
        for ind in indicators:
            if ind.data.empty:
                logger.warning(f"Indicator {ind.id} has no data to transform. Skipping.")
                continue
            transformed_ind, unmatched_list = self._transform_one(ind)
            transformed_indicators.append(transformed_ind)
            if unmatched_list:
                unmatched_report[ind.id] = unmatched_list
        logger.info("Data transformation completed successfully.")
        return transformed_indicators, unmatched_report

    def _transform_one(self, indicator: Indicator) -> Tuple[Indicator, List[str]]:
        """Applies all transformation steps to a single indicator's DataFrame."""
        df = indicator.data.copy().dropna(axis="columns", how="all")
        
        # Pivot from wide to long
        df["economy"] = df.index
        df_long = pd.melt(df, id_vars=["economy"], var_name="year", value_name="value")
        df_long["year"] = df_long["year"].str.extract(r"(\d+)").astype(int)
        df_long = df_long.sort_values(by=["economy", "year"]).reset_index(drop=True)
        
        # Clean country names
        df_long = df_long.rename(columns={"economy": "country"})
        df_long["country"] = df_long["country"].map(self._country_mapping).fillna(df_long["country"])
        all_canonical_names = set(self._country_mapping.values())
        unmatched = list(set(val for val in df_long["country"] if val not in all_canonical_names))
        
        # Remove unmappable rows
        if unmatched:
            logger.warning(f"Deleting rows for {len(unmatched)} unmatched economies.")
            df_long = df_long[~df_long["country"].isin(unmatched)]
            
        indicator.data = df_long
        return indicator, unmatched


class Loader:
    """Loads transformed data into S3."""
    def __init__(self, s3_bucket: str, output_s3_prefix: str):
        self.s3_bucket = s3_bucket
        self.output_s3_prefix = output_s3_prefix
        # ## REFACTORED: Initialize boto3 S3 client
        self.s3_client = boto3.client("s3")

    def execute(self, indicators: List[Indicator]) -> List[str]:
        """Saves each indicator's DataFrame to S3 and creates the indicator registry."""
        skipped_indicators = []
        processed_indicators = []

        for indicator in indicators:
            if indicator.data.empty:
                logger.warning(f"Indicator {indicator.id} has no data to save. Skipping.")
                skipped_indicators.append(indicator.id)
                continue
            
            # ## REFACTORED: Write Parquet file directly to S3
            output_key = f"{self.output_s3_prefix}/{indicator.id}/{indicator.id}.parquet"
            s3_uri = f"s3://{self.s3_bucket}/{output_key}"
            logger.info(f"Writing data for {indicator.id} to {s3_uri}")
            table = pa.Table.from_pandas(indicator.data)
            pq.write_table(table, s3_uri, compression="snappy")
            processed_indicators.append(indicator)

        self._create_indicator_registry(processed_indicators)
        return skipped_indicators

    # ## REFACTORED: Writes the indicator registry YAML file to S3
    def _create_indicator_registry(self, indicators: List[Indicator]):
        """Creates and saves a YAML file mapping indicator IDs to their names in S3."""
        if not indicators:
            logger.warning("No processed indicators to create a registry for.")
            return

        logger.info("Creating indicator registry...")
        registry = {ind.id: ind.indicator_name for ind in indicators if ind.indicator_name}
        
        # Convert dict to YAML string
        yaml_string = yaml.dump(registry, allow_unicode=True)
        
        registry_key = f"{self.output_s3_prefix}/indicator_registry.yaml"
        logger.info(f"Uploading indicator registry to s3://{self.s3_bucket}/{registry_key}")
        
        self.s3_client.put_object(
            Bucket=self.s3_bucket,
            Key=registry_key,
            Body=yaml_string
        )

class Pipeline:
    """Orchestrates the entire ETL (Extract, Transform, Load) process."""
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.extractor = Extractor()
        self.transformer = Transformer(
            s3_bucket=config.s3_bucket,
            countries_csv_key=config.countries_csv_key
        )
        self.loader = Loader(
            s3_bucket=config.s3_bucket,
            output_s3_prefix=config.output_s3_prefix
        )

    def run(self) -> ETLResult:
        """Executes the ETL pipeline from start to finish."""
        logger.info("ETL pipeline starting.")
        
        extracted_indicators = self.extractor.execute(self.config.indicators)
        transformed_indicators, unmatched_report = self.transformer.execute(extracted_indicators)
        skipped_indicators = self.loader.execute(transformed_indicators)
        
        logger.info("ETL pipeline completed successfully.")
        
        return ETLResult(
            processed_indicators=[ind.id for ind in transformed_indicators],
            unmatched_economies=unmatched_report,
            output_location=f"s3://{self.config.s3_bucket}/{self.config.output_s3_prefix}",
            indicators_with_no_data=skipped_indicators,
        )

# ## REFACTORED: This is the entrypoint for AWS Lambda
def lambda_handler(event, context):
    """
    Main handler function for the AWS Lambda service.
    Reads configuration from environment variables and runs the ETL pipeline.
    """
    try:
        # Load configuration from environment variables
        config = PipelineConfig(
            s3_bucket=os.environ['S3_BUCKET'],
            countries_csv_key=os.environ['COUNTRIES_CSV_KEY'],
            output_s3_prefix=os.environ['OUTPUT_S3_PREFIX'],
            indicators=os.environ['INDICATORS'].split(',') # Comma-separated list
        )

        pipeline = Pipeline(config)
        result = pipeline.run()

        logger.info("Pipeline finished with result: %s", result)
        
        # Return a success response for API Gateway or other services
        return {
            'statusCode': 200,
            'body': json.dumps(result.__dict__)
        }

    except Exception as e:
        logger.error("Pipeline failed with an unhandled exception: %s", e, exc_info=True)
        # Return an error response
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
