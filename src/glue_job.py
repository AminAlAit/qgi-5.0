import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F

# --- Job Initialization ---
# ADDED: New optional argument for test runs
args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_INPUT_PATH", "S3_OUTPUT_PATH", "INDICATOR_SUBSET"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --- Configuration ---
S3_INPUT_PATH = args["S3_INPUT_PATH"]
S3_OUTPUT_PATH = args["S3_OUTPUT_PATH"]

# --- 1. Read Raw SDCI Data (UPDATED LOGIC) ---
indicator_subset_str = args.get("INDICATOR_SUBSET")

read_options = {
    "connection_type": "s3",
    "format": "parquet"
}

if indicator_subset_str:
    # Test mode: Construct specific S3 paths from the provided indicator list
    indicator_list = [indicator.strip() for indicator in indicator_subset_str.split(',')]
    s3_paths = [f"{S3_INPUT_PATH}{indicator}/" for indicator in indicator_list]
    print(f"RUNNING IN TEST MODE. Reading from {len(s3_paths)} specific indicator paths.")
    read_options["connection_options"] = {"paths": s3_paths}
else:
    # Full run mode: Read all data from the root path recursively
    print("RUNNING IN FULL MODE. Reading all indicators recursively.")
    read_options["connection_options"] = {"paths": [S3_INPUT_PATH], "recurse": True}

input_dyf = glueContext.create_dynamic_frame.from_options(**read_options)
df = input_dyf.toDF()

if df.rdd.isEmpty():
    print("No input data found. Exiting job.")
    sys.exit(0)

# --- 2. Feature Engineering: Extract Indicator ID ---
df = df.withColumn(
    "indicator_id",
    F.regexp_extract(F.input_file_name(), r'output/([^/]+)/[^/]+\.parquet$', 1)
)

# --- 3. Core Aggregation Logic ---
patterns_df = df.groupBy(
    "country_a_id", "country_b_id", "length", "start_year_a", "start_year_b"
).agg(
    F.avg("correlation").alias("avg_correlation"),
    F.count("indicator_id").alias("indicator_count"),
    F.collect_list("indicator_id").alias("indicator_ids")
)

# --- 4. Filtering and Enrichment ---
print(f"Number of potential patterns BEFORE filtering: {patterns_df.count()}")
patterns_df = patterns_df.filter(F.col("indicator_count") > 1)
print(f"Number of final patterns AFTER filtering: {patterns_df.count()}")

patterns_df = patterns_df.withColumn(
    "pattern_power_score",
    F.col("avg_correlation") * F.col("length") * F.col("indicator_count")
)

# --- 5. Write Output to S3 ---
patterns_df.write.mode("overwrite").parquet(S3_OUTPUT_PATH)

job.commit()
