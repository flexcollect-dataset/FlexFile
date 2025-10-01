import json
import os
import logging
import concurrent.futures
from typing import Any, Dict, List

import pandas as pd
from urllib.parse import urlparse
import tempfile
import boto3
from datetime import datetime, timezone

from src.db_connection import get_connection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ABN_DETAILS_CONCURRENCY = int(os.getenv("ABN_DETAILS_CONCURRENCY", "5"))

DESIRED_FIELDS: List[str] = [
	"contact",
	"website",
	"address",
	"email",
	"sociallink",
	"review",
	"industry",
	"documents",
]

def fetch_details_for_abn(abn: str) -> Dict[str, Any]:
	"""Fetch details for a single ABN from Postgres, returning a dict with desired fields."""
	if not abn:
		return {"abn": "", **{k: "" for k in DESIRED_FIELDS}}
	conn_local = None
	try:
		conn_local = get_connection()
		logger.info("Connect to the dataset")
		with conn_local.cursor() as cur:
			# Try lowercase column name first, then fallback to quoted case
			try:
				cur.execute("SELECT * FROM abn WHERE abn = %s LIMIT 1", (abn,))
			except Exception:
				cur.execute('SELECT * FROM abn WHERE "Abn" = %s LIMIT 1', (abn,))
			row = cur.fetchone()
			if not row:
				return {"abn": abn, **{k: "" for k in DESIRED_FIELDS}}
			colnames = [desc.name for desc in cur.description]
			row_dict = {colnames[i]: row[i] for i in range(len(colnames))}
			result = {"abn": abn}
			for field in DESIRED_FIELDS:
				# Prefer exact field name; allow some common alternates
				candidates = [
					field,
					field.lower(),
					field.upper(),
					field.capitalize(),
				]
				value = ""
				for c in candidates:
					if c in row_dict and row_dict[c] is not None:
						value = row_dict[c]
						break
				result[field] = value
			return result
	except Exception as e:
		logger.warning(f"ABN {abn}: fetch failed: {e}")
		return {"abn": abn, **{k: "" for k in DESIRED_FIELDS}}
	finally:
		if conn_local is not None:
			try:
				conn_local.close()
			except Exception:
				pass


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

    # Concurrently fetch details per unique ABN
    unique_abns: List[str] = (
        df[abn_column].dropna().astype(str).str.strip().unique().tolist()
    )

    logger.info(
        f"Fetching details for {len(unique_abns)} unique ABNs with concurrency={ABN_DETAILS_CONCURRENCY}"
    )
    details: List[Dict[str, Any]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=ABN_DETAILS_CONCURRENCY) as executor:
        future_to_abn = {executor.submit(fetch_details_for_abn, abn): abn for abn in unique_abns}
        for future in concurrent.futures.as_completed(future_to_abn):
            details.append(future.result())

    # Merge details back into the DataFrame on ABN
    details_df = pd.DataFrame(details)
	# If there were no rows returned, ensure columns exist
    if details_df.empty:
        for field in ["abn", *DESIRED_FIELDS]:
            if field not in df.columns:
                df[field] = ""
    else:
        # Normalize merge key to the same column name
        details_df["abn"] = details_df["abn"].astype(str).str.strip()
        df = df.merge(details_df, how="left", left_on=abn_column, right_on="abn", suffixes=("", "_details"))
        # Prefer newly fetched columns; drop helper 'abn' if it is duplicate of abn_column
        if abn_column != "abn":
            df.drop(columns=["abn"], inplace=True)
        # Ensure all desired fields exist (fill missing with empty)
        for field in DESIRED_FIELDS:
            if field not in df.columns:
                df[field] = ""

    # Write back CSV
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if is_s3:
        local_output = os.path.join(temp_dir, "TaxRecords.enriched.csv")
        df.to_csv(local_output, index=False)
        # Backup original object
        backup_key = f"{s3_key}.bak-{timestamp}"
        logger.info(f"Creating backup s3://{s3_bucket}/{backup_key}")
        s3_client.copy_object(
            Bucket=s3_bucket,
            CopySource={"Bucket": s3_bucket, "Key": s3_key},
            Key=backup_key,
        )
        # Upload enriched CSV
        logger.info(f"Uploading enriched CSV to s3://{s3_bucket}/{s3_key}")
        s3_client.upload_file(local_output, s3_bucket, s3_key)
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.normpath(os.path.join(base_dir, "..", "data", "TaxRecords.csv"))
        df.to_csv(csv_path, index=False)

    logger.info("Completed enrichment and CSV write")
    return {"statusCode": 200, "body": json.dumps({"rows": len(df)})}

if __name__ == "__main__":
    lambda_handler()