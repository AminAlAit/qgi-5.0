import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F

# --- Job Initialization ---
args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_INPUT_PATH", "S3_OUTPUT_PATH"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --- Configuration ---
S3_INPUT_PATH = args["S3_INPUT_PATH"]  # e.g., s3://qgi-data-lake-<AWS_ACCOUNT_ID>/pipeline-B-scdi-finder/output/
S3_OUTPUT_PATH = args["S3_OUTPUT_PATH"] # e.g., s3://qgi-data-lake-<AWS_ACCOUNT_ID>/pipeline-C-pattern-synthesizer/output/

# --- 1. Read Raw SDCI Data ---
# Read all Parquet files recursively from the Pipeline B output location.
# This scales automatically as new indicators and countries are added.
input_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [S3_INPUT_PATH], "recurse": True},
    format="parquet",
    transformation_ctx="input_dyf",
)

# Convert to a Spark DataFrame for easier manipulation
df = input_dyf.toDF()

if df.rdd.isEmpty():
    print("No input data found. Exiting job.")
    sys.exit(0)

# --- 2. Feature Engineering: Extract Indicator ID ---
# The INDICATOR_ID is part of the S3 path, not the data itself.
# We extract it from the input file path to use it in our aggregation.
# The regex captures the directory name right before the country parquet file.
df = df.withColumn(
    "indicator_id",
    F.regexp_extract(F.input_file_name(), r'output/([^/]+)/[^/]+\.parquet$', 1)
)

# --- 3. Core Aggregation Logic ---
# This is the heart of the Pattern Synthesizer. It groups billions of SDCIs
# into a much smaller, more meaningful dataset of "Patterns". [cite: 107, 109]
patterns_df = df.groupBy(
    "country_a_id", "country_b_id", "length", "start_year_a", "start_year_b"
).agg(
    F.avg("correlation").alias("avg_correlation"),
    F.count("indicator_id").alias("indicator_count"),
    F.collect_list("indicator_id").alias("indicator_ids")
)

# --- 4. Filtering and Enrichment ---
# A "Pattern" is only significant if it's supported by multiple independent indicators.
# We filter out any groups that were only formed from a single SDCI.
patterns_df = patterns_df.filter(F.col("indicator_count") > 1)

# Calculate the 'pattern_power_score' as a composite metric of a pattern's significance.
patterns_df = patterns_df.withColumn(
    "pattern_power_score",
    F.col("avg_correlation") * F.col("length") * F.col("indicator_count")
)

# --- 5. Write Output to S3 ---
# The final, high-confidence "Patterns" dataset is written to S3 as Parquet. [cite: 110]
# repartition(1) creates a single output file as specified in the docs. 
# For massive scale, you might remove this to allow Spark to write multiple partitioned files.
output_df = patterns_df.repartition(1)

# Convert back to DynamicFrame before writing
output_dyf = DynamicFrame.fromDF(output_df, glueContext, "output_dyf")

glueContext.write_dynamic_frame.from_options(
    frame=output_dyf,
    connection_type="s3",
    connection_options={"path": S3_OUTPUT_PATH},
    format="parquet",
    transformation_ctx="output_dyf",
)

job.commit()
