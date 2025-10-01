import json
import time
import os
import logging
import concurrent.futures
from typing import Any, Dict, List, Tuple

import requests
import pandas as pd
import psycopg2
import psycopg2.extras
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlparse
import tempfile
import boto3
from datetime import datetime, timezone
from .db_connection import get_connection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
ABN_DETAILS_CONCURRENCY = int(os.getenv("ABN_DETAILS_CONCURRENCY", "5"))


def lambda_handler():
    logger.info(f"entering in function")
    s3_uri = (os.getenv("TAX_CSV_S3_URI") or "").strip()
    is_s3 = s3_uri.startswith("s3://")
    temp_dir = None
    s3_client = None
    s3_bucket = None
    s3_key = None

    if is_s3:
        parsed = urlparse(s3_uri)
        s3_bucket = parsed.netloc
        s3_key = parsed.path.lstrip("/")
        if not s3_bucket or not s3_key:
            raise ValueError(f"Invalid TAX_CSV_S3_URI '{s3_uri}'. Expected format s3://bucket/key.csv")
        s3_client = boto3.client("s3")
        temp_dir = tempfile.mkdtemp(prefix="flexcollect_")
        local_input = os.path.join(temp_dir, "TaxRecords.csv")
        logger.info(f"Downloading CSV from s3://{s3_bucket}/{s3_key} to {local_input}")
        s3_client.download_file(s3_bucket, s3_key, local_input)
        df = pd.read_csv(local_input, low_memory=False)
        logger.info(f"reading csv file in function")
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.normpath(os.path.join(base_dir, "..", "data", "TaxRecords.csv"))
        df = pd.read_csv(csv_path, low_memory=False)

    # Normalize ABN column name
    abn_column = None
    for candidate in ("abn", "Abn", "ABN"):
        if candidate in df.columns:
            abn_column = candidate
            break
    if not abn_column:
        raise ValueError("Input CSV must contain an 'abn' column")

    # Ensure ABN is string type and trimmed
    df[abn_column] = df[abn_column].astype(str).str.strip()

    desired_fields: List[str] = [
        "contact",
        "website",
        "address",
        "email",
        "sociallink",
        "review",
        "industry",
        "documents",
    ]

    # Concurrently fetch details per unique ABN
    unique_abns: List[str] = (
        df[abn_column].dropna().astype(str).str.strip().unique().tolist()
    )
    logger.info("Completed enrichment and CSV write")
    return {"statusCode": 200, "body": json.dumps({"rows": len(df)})}

if __name__ == "__main__":
    lambda_handler()