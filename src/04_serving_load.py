# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Serving Load: S3 Gold → serving.yellow_tripdata_ny

# COMMAND ----------

dbutils.widgets.text("aws_access_key_id", "", "AWS Access Key ID")
dbutils.widgets.text("aws_secret_access_key", "", "AWS Secret Access Key")

# COMMAND ----------

import boto3
import pandas as pd
from io import BytesIO

AWS_ACCESS_KEY_ID     = dbutils.widgets.get("aws_access_key_id")
AWS_SECRET_ACCESS_KEY = dbutils.widgets.get("aws_secret_access_key")
AWS_REGION            = "us-east-2"
BUCKET                = "case-taxi-ny-dados"
GOLD_PREFIX           = "gold/"

REQUIRED_COLS = [
    "VendorID", "passenger_count", "total_amount",
    "tpep_pickup_datetime", "tpep_dropoff_datetime"
]

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criar schema e tabela

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS serving
# MAGIC   COMMENT 'Camada final para consumo via SQL';
# MAGIC
# MAGIC DROP TABLE IF EXISTS serving.yellow_tripdata_ny;
# MAGIC
# MAGIC CREATE TABLE serving.yellow_tripdata_ny (
# MAGIC     VendorID              LONG,
# MAGIC     passenger_count       LONG,
# MAGIC     total_amount          DOUBLE,
# MAGIC     tpep_pickup_datetime  TIMESTAMP,
# MAGIC     tpep_dropoff_datetime TIMESTAMP
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregar arquivo por arquivo da gold/

# COMMAND ----------

MES = "3"  # altere para o mes desejado: 1, 2, 3, 4 ou 5

paginator     = s3.get_paginator("list_objects_v2")
parquet_files = [
    obj["Key"]
    for page in paginator.paginate(Bucket=BUCKET, Prefix=f"{GOLD_PREFIX}year=2023/month={MES}/")
    for obj in page.get("Contents", [])
    if obj["Key"].endswith(".parquet")
]

print(f"[INFO] {len(parquet_files)} files found for month={MES}")

for key in parquet_files:
    obj   = s3.get_object(Bucket=BUCKET, Key=key)
    df_pd = pd.read_parquet(BytesIO(obj["Body"].read()), columns=REQUIRED_COLS)

    df_pd["VendorID"]        = pd.to_numeric(df_pd["VendorID"],        errors="coerce")
    df_pd["passenger_count"] = pd.to_numeric(df_pd["passenger_count"], errors="coerce")
    df_pd["total_amount"]    = pd.to_numeric(df_pd["total_amount"],    errors="coerce")
    df_pd["tpep_pickup_datetime"]  = pd.to_datetime(df_pd["tpep_pickup_datetime"],  errors="coerce", utc=True)
    df_pd["tpep_dropoff_datetime"] = pd.to_datetime(df_pd["tpep_dropoff_datetime"], errors="coerce", utc=True)

    df_pd = df_pd.dropna()

    spark.createDataFrame(df_pd) \
        .write.format("delta").mode("append") \
        .saveAsTable("serving.yellow_tripdata_ny")

    print(f"[OK] {key} - {len(df_pd):,} rows loaded")
    del df_pd

print("[DONE] serving.yellow_tripdata_ny loaded")
