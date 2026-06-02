import sys
import boto3
import requests
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET   = "case-taxi-ny-dados"
PREFIX   = "landing/"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
YEAR     = "2023"
MONTHS   = ["01", "02", "03", "04", "05"]

s3 = boto3.client("s3")

existing = {
    obj["Key"].replace(PREFIX, "")
    for obj in s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX).get("Contents", [])
}

for month in MONTHS:
    filename = f"yellow_tripdata_{YEAR}-{month}.parquet"

    if filename in existing:
        print(f"[SKIP] {filename} already exists")
        continue

    url    = f"{BASE_URL}/{filename}"
    s3_key = f"{PREFIX}{filename}"

    print(f"[DOWNLOAD] {filename}")
    response = requests.get(url, stream=True, timeout=300)
    response.raise_for_status()

    s3.upload_fileobj(response.raw, BUCKET, s3_key)
    print(f"[OK] {filename} -> s3://{BUCKET}/{s3_key}")

print("[DONE] Ingestion completed")
job.commit()