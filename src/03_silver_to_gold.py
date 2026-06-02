import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET      = "case-taxi-ny-dados"
SILVER_PATH = f"s3://{BUCKET}/silver/"
GOLD_PATH   = f"s3://{BUCKET}/gold/"

df_silver = spark.read.parquet(SILVER_PATH)

df_gold = (
    df_silver
    .select(
        "row_key",
        "VendorID",
        "passenger_count",
        "total_amount",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "year",
        "month"
    )
    .dropna(subset=["VendorID", "passenger_count", "total_amount",
                    "tpep_pickup_datetime", "tpep_dropoff_datetime"])
    .filter(F.col("passenger_count") > 0)
    .filter(F.col("total_amount") > 0)
    .filter(F.col("tpep_pickup_datetime") < F.col("tpep_dropoff_datetime"))
    .dropDuplicates(["row_key"])
)

print(f"[INFO] {df_gold.count():,} records for gold")

(
    df_gold
    .write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("year", "month")
    .save(GOLD_PATH)
)

print(f"[DONE] Gold written to {GOLD_PATH}")
job.commit()