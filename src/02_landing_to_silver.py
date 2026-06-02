import sys
import boto3
from datetime import datetime
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

BUCKET         = "case-taxi-ny-dados"
SILVER_PATH    = f"s3://{BUCKET}/silver/"
RUN_TIMESTAMP  = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
PROCESSED_PATH = f"landing/processados/{RUN_TIMESTAMP}/"

s3 = boto3.client("s3")

response = s3.list_objects_v2(Bucket=BUCKET, Prefix="landing/", Delimiter="/")
pending  = [
    obj["Key"]
    for obj in response.get("Contents", [])
    if obj["Key"].endswith(".parquet")
]

if not pending:
    print("[INFO] No files to process")
    job.commit()
    sys.exit(0)

print(f"[INFO] Files to process: {len(pending)}")

for s3_key in pending:
    filename = s3_key.split("/")[-1]
    print(f"[START] {filename}")

    df = spark.read.parquet(f"s3://{BUCKET}/{s3_key}")

    df = (
        df
        .withColumn("VendorID",        F.col("VendorID").cast("long"))
        .withColumn("passenger_count", F.col("passenger_count").cast("long"))
        .withColumn("total_amount",    F.col("total_amount").cast("double"))
        .withColumn("row_key", F.md5(F.concat_ws("|",
            F.col("VendorID").cast("string"),
            F.col("tpep_pickup_datetime").cast("string"),
            F.col("tpep_dropoff_datetime").cast("string")
        )))
        .withColumn("year",  F.year("tpep_pickup_datetime"))
        .withColumn("month", F.month("tpep_pickup_datetime"))
        .filter(F.col("year") == 2023)
        .filter(F.col("month").between(1, 5))
    )

    (
        df.write
        .format("parquet")
        .mode("append")
        .partitionBy("year", "month")
        .save(SILVER_PATH)
    )

    dest_key = f"{PROCESSED_PATH}{filename}"
    s3.copy_object(Bucket=BUCKET, CopySource={"Bucket": BUCKET, "Key": s3_key}, Key=dest_key)
    s3.delete_object(Bucket=BUCKET, Key=s3_key)
    print(f"[OK] {filename} -> silver + moved to processados")

print("[DONE] Landing to Silver completed")
job.commit()