"""
Utility script to ensure MinIO / S3 buckets and folder prefixes exist before running pipelines.
"""

import os
import sys
from minio import Minio
from minio.error import S3Error


def setup_storage():
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    bucket_name = os.getenv("S3_BUCKET", "clickstream-warehouse")

    # Strip scheme if provided
    endpoint_clean = endpoint.replace("http://", "").replace("https://", "")

    print(f"Connecting to MinIO endpoint: {endpoint_clean}...")
    client = Minio(
        endpoint=endpoint_clean,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )

    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Created S3 bucket: '{bucket_name}'.")
        else:
            print(f"S3 bucket '{bucket_name}' already exists.")

        print("MinIO storage initialized successfully.")
    except S3Error as err:
        print(f"MinIO setup error: {err}")
        sys.exit(1)


if __name__ == "__main__":
    setup_storage()
