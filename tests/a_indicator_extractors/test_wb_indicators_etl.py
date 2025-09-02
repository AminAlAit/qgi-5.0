"""Tests for the cloud-native indicator extractor pipeline."""

import json
import yaml
import boto3
from moto import mock_aws
from qgi_pipeline_a_indicator_extractor.wb_indicators_etl import lambda_handler


@mock_aws
def test_pipeline_run_with_mock_s3(
    mocker,
    s3_setup,
    mock_env_vars,
    sample_raw_dataframe,
    sample_indicator_metadata
):
    """
    Test the full pipeline, mocking the WB API and using a mock S3 for I/O.
    """
    # 1. Mock the external World Bank API calls
    # This path is now correct because pytest knows to look inside 'src'
    module_path = "qgi_pipeline_a_indicator_extractor.wb_indicators_etl"
    
    mocker.patch(f"{module_path}.wb.data.DataFrame", return_value=sample_raw_dataframe)
    mocker.patch(f"{module_path}.wb.series.metadata.get", return_value=sample_indicator_metadata)

    # 2. Run the actual lambda_handler
    result = lambda_handler({}, {})

    # 3. For debugging, print the error if the run fails
    if result['statusCode'] != 200:
        print("Lambda failed. Response body:", json.loads(result['body']))

    # 4. Assert that the pipeline SUCCEEDED
    assert result['statusCode'] == 200

    # 5. Verify that the output files were CREATED in the mock S3 bucket
    s3 = boto3.client("s3")

    parquet_key = "processed/wb_indicators/SP.POP.TOTL/SP.POP.TOTL.parquet"
    response = s3.get_object(Bucket="mock-bucket", Key=parquet_key)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    registry_key = "processed/wb_indicators/indicator_registry.yaml"
    response = s3.get_object(Bucket="mock-bucket", Key=registry_key)
    registry_data = yaml.safe_load(response['Body'].read())
    assert "SP.POP.TOTL" in registry_data
