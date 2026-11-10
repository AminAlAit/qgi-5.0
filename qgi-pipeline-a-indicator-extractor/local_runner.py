"""DOCSTRING"""

from dotenv import load_dotenv
from src.qgi_pipeline_a_indicator_extractor.wb_indicators_etl import lambda_handler


load_dotenv()


if __name__ == "__main__":
    print("--- Running pipeline locally ---")
    result = lambda_handler({}, {})
    print("--- Pipeline run finished ---")
    print(result)
