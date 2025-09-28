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
S3_INPUT_PATH = args["S3_INPUT_PATH"]
S3_OUTPUT_PATH = args["S3_OUTPUT_PATH"]

# --- 1. Read Raw SDCI Data ---
input_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [S3_INPUT_PATH], "recurse": True},
    format="parquet",
    transformation_ctx="input_dyf",
)

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
# [cite_start]This is the heart of the Pattern Synthesizer[cite: 97]. It groups SDCIs
# [cite_start]into a smaller, more meaningful dataset of "Patterns"[cite: 97, 142].
patterns_df = df.groupBy(
    "country_a_id", "country_b_id", "length", "start_year_a", "start_year_b"
).agg(
    F.avg("correlation").alias("avg_correlation"),
    F.count("indicator_id").alias("indicator_count"),
    F.collect_list("indicator_id").alias("indicator_ids")
)

# --- 4. Filtering and Enrichment ---
# [cite_start]A "Pattern" is significant only if supported by multiple independent indicators[cite: 199].
patterns_df = patterns_df.filter(F.col("indicator_count") > 1)

patterns_df = patterns_df.withColumn(
    "pattern_power_score",
    F.col("avg_correlation") * F.col("length") * F.col("indicator_count")
)

# --- 5. Write Output to S3 ---
# Use the correct DataFrame name: 'patterns_df'
# mode("overwrite") ensures the job can be re-run for updates.
patterns_df.write.mode("overwrite").parquet(S3_OUTPUT_PATH)

job.commit()
