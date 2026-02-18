import os
import boto3
from pathlib import Path
from io import BytesIO
from config import settings

from logger import get_logger
logger = get_logger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("RAW_PREFIX", "raw-data")

s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

def upload_to_s3(file: str | BytesIO, key: str, bucket: str = MINIO_BUCKET):
    try:
        if isinstance(file, BytesIO):
            s3_client.upload_fileobj(file, bucket, key)
        else:
            s3_client.upload_file(file, bucket, key)
        logger.info(f"Uploaded to {bucket}/{key}")
    except Exception as e:
        logger.error(f"Failed to upload to S3 bucket '{bucket}': {e}")
        raise

def download_from_s3(key: str, local_path: str | Path) -> Path:
    try:
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        s3_client.download_file(MINIO_BUCKET, key, str(local_path))
        logger.info(f"Downloaded {key} to {local_path}")
        return local_path

    except Exception as e:
        logger.error(f"Failed to download {key}: {e}")
        raise

def from_s3_to_mem(key: str, bucket: str = MINIO_BUCKET) -> BytesIO:
    try:
        buffer = BytesIO()
        s3_client.download_fileobj(bucket, key, buffer)
        buffer.seek(0)
        logger.info(f"Loaded {key} from bucket '{bucket}' to memory")
        return buffer
    except Exception as e:
        logger.error(f"Failed to load {key} from bucket '{bucket}' into memory: {e}")
        raise


def list_objects(prefix: str = "") -> list[str]:
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=MINIO_BUCKET, Prefix=prefix)

        keys = []
        for page in page_iterator:
            contents = page.get("Contents", [])
            for obj in contents:
                keys.append(obj["Key"])

        logger.info(f"Listed {len(keys)} object(s) under '{prefix}'")
        return keys

    except Exception as e:
        logger.error(f"Failed to list objects under '{prefix}': {e}")
        raise

def test_s3_connection() -> bool:
    try:
        s3_client.list_objects_v2(Bucket=MINIO_BUCKET, MaxKeys=1)
        logger.info(f"Сonnection OK, bucket '{MINIO_BUCKET}'")
        return True
    except Exception as e:
        logger.error(f"Сonnection failed, bucket '{MINIO_BUCKET}': {e}")
        return False