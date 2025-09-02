"""
Fixtures for testing the cloud-native ETL pipeline.

This file sets up a mock AWS environment using 'moto', provides mock data,
and configures the necessary environment variables for tests.
"""

import os
import boto3
import pandas as pd
import pytest
from moto import mock_aws


@pytest.fixture(name="aws_credentials")
def mock_aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1" # Default region for moto

@pytest.fixture(name="s3_setup")
def setup_mock_s3_environment(aws_credentials):
    """
    Creates a mock S3 bucket and uploads a dummy countries.csv file.
    This fixture provides a clean, mocked S3 environment for each test.
    """
    with mock_aws():
        s3 = boto3.client("s3")
        bucket_name = "mock-bucket"
        s3.create_bucket(Bucket=bucket_name)

        # Create a dummy countries.csv in the mock bucket
        csv_content = "code,name_1\nAFE,Africa Eastern and Southern\nUSA,United States\nWLD,World"
        s3.put_object(
            Bucket=bucket_name,
            Key="raw-data/countries.csv",
            Body=csv_content
        )
        yield # Hands control over to the test function

@pytest.fixture(name="mock_env_vars")
def setup_mock_environment_variables(monkeypatch):
    """Sets the environment variables that the Lambda handler expects."""
    monkeypatch.setenv("S3_BUCKET", "mock-bucket")
    monkeypatch.setenv("COUNTRIES_CSV_KEY", "raw-data/countries.csv")
    monkeypatch.setenv("OUTPUT_S3_PREFIX", "processed/wb_indicators")
    monkeypatch.setenv("INDICATORS", "SP.POP.TOTL")

# --- Unchanged Fixtures (Still perfectly valid) ---

@pytest.fixture(name="sample_raw_dataframe")
def mock_raw_wb_dataframe():
    """Returns a sample raw DataFrame mimicking the World Bank API."""
    data = {"YR1960": [100, 200, 300], "YR1961": [110, 210, 310]}
    index = pd.Index(["USA", "AFE", "WLD"], name="economy")
    return pd.DataFrame(data, index=index)

@pytest.fixture(name="sample_indicator_metadata")
def mock_indicator_metadata():
    """Mocks the metadata object returned by wbgapi."""
    class MockMetadata:
        concept = "Test Concept"
        metadata = {"IndicatorName": "Population, total", "Source": "World Bank"}

    return MockMetadata()
