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
import xml.etree.ElementTree as ET
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google import genai
from google.genai import types
from pydantic import BaseModel
from urllib.parse import urlparse
import tempfile
import boto3
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

GENAI_API_KEY = "AIzaSyD1VmH7wuQVqxld5LeKjF79eRq1gqVrNFA"
GENAI_CLIENT = genai.Client(api_key=GENAI_API_KEY) if GENAI_API_KEY else None

# Concurrency and batching configuration (env-configurable)
GENAI_BATCH_SIZE = int(os.getenv("GENAI_BATCH_SIZE", "50"))
ABN_DETAILS_CONCURRENCY = int(os.getenv("ABN_DETAILS_CONCURRENCY", "10"))
BATCH_PAUSE_SECONDS = float(os.getenv("BATCH_PAUSE_SECONDS", "0"))

# --- Models (moved to module scope to avoid redefinition) ---
class ABNDetails(BaseModel):
    Contact: str
    Website: str
    Address: str
    Email: str
    SocialLink: List[str]
    review: str
    Industry: str


class ACNDocumentsDetails(BaseModel):
    documentid: str
    dateofpublication: str
    noticetype: str

# === Tax Records CSV enrichment (Gemini) ===
import shutil

def _ensure_columns(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            # For SocialLink/Documents we prefer list-typed defaults;
            # but CSVs don't preserve lists, so we'll JSON-encode later.
            df[c] = ""
    return df

def _stringify_for_csv(val):
    # CSV can't store Python lists/dicts directly. Use JSON when needed.
    if isinstance(val, (list, dict)):
        try:
            return json.dumps(val, ensure_ascii=False)
        except Exception:
            return str(val)
    return "" if val is None else str(val)

def _chunk_list(items: List[str], chunk_size: int) -> List[List[str]]:
    if chunk_size <= 0:
        return [items]
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]

def _call_gemini_batch(contents: List[str], response_schema, fallback_factory):
    try:
        resp = GENAI_CLIENT.models.generate_content(
            model="gemini-2.0-flash",
            contents=contents,
            config={
                "response_mime_type": "application/json",
                "response_schema": response_schema,
            },
        )
        logger.info("Response array (JSON): %s", json.dumps(resp.parsed, indent=2))
        return resp.parsed
    except Exception as e:
        logger.warning(f"Gemini batch error: {e}")
        return [fallback_factory() for _ in contents]

def enrich_tax_records_csv():
    """
    Reads TaxRecords.csv, fetches enrichment using Google Gemini,
    and writes the results back into the SAME CSV (creates a .bak first).

    Required env: GENAI_API_KEY
    """
    global GENAI_CLIENT
    if GENAI_CLIENT is None:
        api_key = GENAI_API_KEY
        if api_key:
            try:
                GENAI_CLIENT = genai.Client(api_key=api_key)
            except Exception as e:
                logger.warning(f"Failed to init Gemini client: {e}")
        else:
            logger.warning("GENAI_API_KEY not set; enrichment will add empty values.")

    # Read CSV from local path or S3 depending on TAX_CSV_S3_URI
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

    # Add enrichment columns if missing
    enrich_cols = ["Contact", "Website", "Address", "Email", "SocialLink", "Review", "Industry_enriched", "Documents"]
    df = _ensure_columns(df, enrich_cols)

    # Build prompts based on existing columns
    batch_size = GENAI_BATCH_SIZE
    prompts = []
    row_indices = []
    acn_prompts = []
    acn_row_indices = []

    for i, row in df.iterrows():
        # Try to build a company descriptor from available fields
        entity = str(row.get("Business Name") or row.get("Trading Name") or "").strip()
        state = (str(row.get("state") or "")).strip()
        if entity:
            prompts.append(
                f"give me the website, contact number, social media links, total reviews, Industry and address of '{entity}', {state}, Australia. I want review in format of 4/5 like that"
            )
            row_indices.append(i)
        # If ACN exists, ask for ASIC published notices
        acn = str(row.get("acn") or "").strip()
        if acn and acn.lower() != "nan":
            acn_prompts.append(
                "Using https://publishednotices.asic.gov.au/browsesearch-notices fetch all the notices related to ACN " + acn + ". Return JSON with keys documentid, dateofpublication, noticetype (single best match if multiple)."
            )
            acn_row_indices.append(i)

    # Call Gemini for entity enrichment in batches with concurrency
    results = []
    if GENAI_CLIENT and prompts:
        abn_batches = _chunk_list(prompts, batch_size)
        if abn_batches:
            for window_start in range(0, len(abn_batches), ABN_DETAILS_CONCURRENCY):
                window_batches = abn_batches[window_start:window_start + ABN_DETAILS_CONCURRENCY]
                with concurrent.futures.ThreadPoolExecutor(max_workers=ABN_DETAILS_CONCURRENCY) as executor:
                    futures = [
                        executor.submit(
                            _call_gemini_batch,
                            batch,
                            list[ABNDetails],
                            lambda: ABNDetails(Contact="", Website="", Address="", Email="", SocialLink=[], review="", Industry=""),
                        )
                        for batch in window_batches
                    ]
                    for fut in futures:
                        batch_result = fut.result()
                        results.extend(batch_result)
                logger.info("Gemini data1")
                if BATCH_PAUSE_SECONDS > 0 and window_start + ABN_DETAILS_CONCURRENCY < len(abn_batches):
                    time.sleep(BATCH_PAUSE_SECONDS)
    else:
        # No client => blanks
        results = [ABNDetails(Contact="", Website="", Address="", Email="", SocialLink=[], review="", Industry="") for _ in prompts]

    # Call Gemini for ACN notices with concurrency
    acn_results = []
    if GENAI_CLIENT and acn_prompts:
        acn_batches = _chunk_list(acn_prompts, batch_size)
        if acn_batches:
            for window_start in range(0, len(acn_batches), ABN_DETAILS_CONCURRENCY):
                window_batches = acn_batches[window_start:window_start + ABN_DETAILS_CONCURRENCY]
                with concurrent.futures.ThreadPoolExecutor(max_workers=ABN_DETAILS_CONCURRENCY) as executor:
                    futures = [
                        executor.submit(
                            _call_gemini_batch,
                            batch,
                            list[ACNDocumentsDetails],
                            lambda: ACNDocumentsDetails(documentid="", dateofpublication="", noticetype=""),
                        )
                        for batch in window_batches
                    ]
                    for fut in futures:
                        batch_result = fut.result()
                        acn_results.extend(batch_result)
                logger.info("Gemini data2")
                if BATCH_PAUSE_SECONDS > 0 and window_start + ABN_DETAILS_CONCURRENCY < len(acn_batches):
                    time.sleep(BATCH_PAUSE_SECONDS)
    else:
        acn_results = [ACNDocumentsDetails(documentid="", dateofpublication="", noticetype="") for _ in acn_prompts]

    # Merge back into DataFrame
    for offset, ridx in enumerate(row_indices):
        if offset < len(results):
            r = results[offset]
            df.at[ridx, "Contact"] = _stringify_for_csv(r.Contact)
            df.at[ridx, "Website"] = _stringify_for_csv(r.Website)
            df.at[ridx, "Address"] = _stringify_for_csv(r.Address)
            df.at[ridx, "Email"] = _stringify_for_csv(r.Email)
            df.at[ridx, "SocialLink"] = _stringify_for_csv(r.SocialLink)
            df.at[ridx, "Review"] = _stringify_for_csv(r.review)
            df.at[ridx, "Industry_enriched"] = _stringify_for_csv(r.Industry)
            logger.info(f"Merging data")

    for offset, ridx in enumerate(acn_row_indices):
        if offset < len(acn_results):
            doc = acn_results[offset]
            df.at[ridx, "Documents"] = _stringify_for_csv({
                "documentid": doc.documentid,
                "dateofpublication": doc.dateofpublication,
                "noticetype": doc.noticetype,
            })

    # Write back (S3 or local)
    if is_s3:
        assert s3_client is not None
        assert temp_dir is not None
        # Backup original object with timestamped suffix
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup_key = f"{s3_key}.bak-{ts}"
        logger.info(f"Creating backup s3://{s3_bucket}/{backup_key}")
        s3_client.copy_object(Bucket=s3_bucket, CopySource={"Bucket": s3_bucket, "Key": s3_key}, Key=backup_key)

        local_output = os.path.join(temp_dir, "TaxRecords.out.csv")
        df.to_csv(local_output, index=False)
        logger.info(f"Uploading enriched CSV to s3://{s3_bucket}/{s3_key}")
        s3_client.upload_file(local_output, s3_bucket, s3_key)

        try:
            shutil.rmtree(temp_dir)
        except Exception:
            pass
        logger.info(f"Enriched CSV written to s3://{s3_bucket}/{s3_key}; backup at s3://{s3_bucket}/{backup_key}")
        return f"s3://{s3_bucket}/{s3_key}"
    else:
        # Backup then write in-place
        backup_path = str(csv_path) + ".bak"
        shutil.copyfile(csv_path, backup_path)
        df.to_csv(csv_path, index=False)
        logger.info(f"Enriched CSV written to {csv_path}; backup at {backup_path}")
        return csv_path



# Call before the existing main() fallthrough (in Lambda it's harmless)
enrich_tax_records_csv()
