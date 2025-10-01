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
from .db_connection import get_connection, get_last_processed_postcode, set_last_processed_postcode

# --- DB insert function ---
def fetch_batch_to_postgres(abn) -> None:
    conn = get_connection()
    if not isinstance(conn, psycopg2.extensions.connection):
        # Defensive: get_connection might have returned an error dict
        raise RuntimeError("Database connection is not available")

    cursor = conn.cursor()
    sql_query = "SELECT * FROM abn WHERE id = %s;"

    # Execute the query, passing the variable as a tuple
    cursor.execute(sql_query, (abn,))
    rows = cursor.fetchall()
    conn.commit()
    cursor.close()
    return rows
    # Intentionally do not close the connection to enable reuse across invocations
# --- DB insert function end ---


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
ABN_DETAILS_CONCURRENCY = int(os.getenv("ABN_DETAILS_CONCURRENCY", "5"))


def lambda_handler(event, context):
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
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.normpath(os.path.join(base_dir, "..", "data", "TaxRecords.csv"))
        df = pd.read_csv(csv_path, low_memory=False)


    try:
        for i, row in df.iterrows():
            abn = str(row.get("abn") or "").strip()
            data = fetch_batch_to_postgres(abn)
            for row in data:
                print(row[0])
    except requests.exceptions.RequestException as e:
        logger.error(f"Error connecting to or querying the database: {e}")