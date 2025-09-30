import json
import time
import os
import logging
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
        if entity and entity.lower() != "nan":
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

    # Call Gemini for entity enrichment in simple sequential batches
    results = []
    if GENAI_CLIENT and prompts:
        for j in range(0, len(prompts), batch_size):
            batch_prompts = prompts[j:j + batch_size]
            try:
                response = GENAI_CLIENT.models.generate_content(
                    model="gemini-2.0-flash",
                    contents=batch_prompts,
                    config={
                        "response_mime_type": "application/json",
                        "response_schema": list[ABNDetails],
                    },
                )
                parsed = getattr(response, "parsed", None) or []
                results.extend(parsed)
            except Exception as e:
                logger.warning(f"Error in Generative AI batch call (ABN): {e}")
                results.extend([
                    ABNDetails(Contact="", Website="", Address="", Email="", SocialLink=[], review="", Industry="")
                    for _ in batch_prompts
                ])
            if BATCH_PAUSE_SECONDS > 0 and j + batch_size < len(prompts):
                time.sleep(BATCH_PAUSE_SECONDS)
    else:
        # No client => blanks
        results = [
            ABNDetails(Contact="", Website="", Address="", Email="", SocialLink=[], review="", Industry="")
            for _ in prompts
        ]

    # Call Gemini for ACN notices in simple sequential batches
    acn_results = []
    if GENAI_CLIENT and acn_prompts:
        for j in range(0, len(acn_prompts), batch_size):
            batch_acn_prompts = acn_prompts[j:j + batch_size]
            try:
                dresponse = GENAI_CLIENT.models.generate_content(
                    model="gemini-2.0-flash",
                    contents=batch_acn_prompts,
                    config={
                        "response_mime_type": "application/json",
                        "response_schema": list[ACNDocumentsDetails],
                    },
                )
                parsed = getattr(dresponse, "parsed", None) or []
                acn_results.extend(parsed)
            except Exception as e:
                logger.warning(f"Error in Generative AI batch call (ACN Docs): {e}")
                acn_results.extend([
                    ACNDocumentsDetails(documentid="", dateofpublication="", noticetype="")
                    for _ in batch_acn_prompts
                ])
            if BATCH_PAUSE_SECONDS > 0 and j + batch_size < len(acn_prompts):
                time.sleep(BATCH_PAUSE_SECONDS)
    else:
        acn_results = [
            ACNDocumentsDetails(documentid="", dateofpublication="", noticetype="")
            for _ in acn_prompts
        ]

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
