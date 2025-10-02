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
from .db_connection import get_connection

# --- DB insert function ---
def insert_batch_to_postgres(batch_df: pd.DataFrame) -> None:
    if batch_df.empty:
        return

    conn = get_connection()
    if not isinstance(conn, psycopg2.extensions.connection):
        # Defensive: get_connection might have returned an error dict
        raise RuntimeError("Database connection is not available")

    cursor = conn.cursor()
    print(batch_df)
    # Ensure only known columns
    header_columns = [
        'Abn', 'AbnStatus', 'AbnStatusEffectiveFrom', 'Acn', 'AddressDate',
        'AddressPostcode', 'AddressState', 'BusinessName', 'EntityName',
        'EntityTypeCode', 'EntityTypeName', 'Gst', 'Message', 'Contact',
        'Website', 'Address', 'Email', 'SocialLink', 'Review', 'Industry',
        'Documents'
    ]
    # # Add any missing expected columns with empty default values
    # for col in header_columns:
    #     if col not in batch_df.columns:
    #         batch_df[col] = ""
    # batch_df = batch_df[header_columns]

    # # Convert lists/dicts to JSON strings for text columns
    # batch_df['SocialLink'] = batch_df['SocialLink'].apply(
    #     lambda x: json.dumps(x) if isinstance(x, (list, dict)) else (x if x is not None else "")
    # )
    # batch_df['Documents'] = batch_df['Documents'].apply(
    #     lambda x: json.dumps(x) if isinstance(x, (list, dict)) else (x if x is not None else "")
    # )

    # values = [tuple(x) for x in batch_df.to_numpy()]

    # insert_query = """
    #     INSERT INTO abn
    #     (Abn, AbnStatus, AbnStatusEffectiveFrom, Acn, AddressDate, AddressPostcode,
    #      AddressState, BusinessName, EntityName, EntityTypeCode, EntityTypeName, Gst,
    #      Message, Contact, Website, Address, Email, SocialLink, Review, Industry, Documents)
    #     VALUES %s
    #     ON CONFLICT (Abn) DO NOTHING
    # """

    # psycopg2.extras.execute_values(
    #     cursor, insert_query, values, template=None, page_size=1000
    # )

    conn.commit()
    cursor.close()
    # Intentionally do not close the connection to enable reuse across invocations
# --- DB insert function end ---


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Reduce noise from third-party libraries leaking INFO logs (e.g., AFC notice)
for noisy_logger_name in ("google", "google.genai", "urllib3", "requests", "httpx"):
    try:
        logging.getLogger(noisy_logger_name).setLevel(logging.WARNING)
    except Exception:
        pass

# --- Configuration via environment variables ---
GENAI_API_KEY = os.getenv("GENAI_API_KEY", "AIzaSyD1VmH7wuQVqxld5LeKjF79eRq1gqVrNFA")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
GENAI_BATCH_SIZE = int(os.getenv("GENAI_BATCH_SIZE", "50"))
ABN_DETAILS_CONCURRENCY = int(os.getenv("ABN_DETAILS_CONCURRENCY", "5"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "10"))
BATCH_PAUSE_SECONDS = float(os.getenv("BATCH_PAUSE_SECONDS", "0"))
RESUME_FROM_DB = os.getenv("RESUME_FROM_DB", "true").lower() in ("1", "true", "yes", "y")

# --- Initialize GenAI client once per container ---
GENAI_CLIENT = genai.Client(api_key=GENAI_API_KEY) if GENAI_API_KEY else None


# --- Models (moved to module scope to avoid redefinition) ---
class ABNDetails(BaseModel):
    Contact: str
    Website: str
    Address: str
    Email: str
    SocialLink: List[str]
    review: str
    Industry: str

def _search_abns():
    conn = get_connection()
    if not isinstance(conn, psycopg2.extensions.connection):
        # Defensive: get_connection might have returned an error dict
        raise RuntimeError("Database connection is not available")

    cursor = conn.cursor()
    cursor.execute("SELECT abn FROM abn WHERE entitytypecode = 'IND' AND businessname IS NOT NULL AND businessname <> '' AND businessname <> '{}'")
    row = cursor.fetchone()
    return row

def _fetch_abn_details(abn: str) -> Dict[str, Any]:
    conn = get_connection()
    if not isinstance(conn, psycopg2.extensions.connection):
        # Defensive: get_connection might have returned an error dict
        raise RuntimeError("Database connection is not available")

    cursor = conn.cursor()
    abn_clean = (abn or "").replace(" ", "")
    cursor.execute("SELECT * FROM abn WHERE abn = %s LIMIT 1", (abn,))
    row = cursor.fetchone()
    return row

def lambda_handler(event, context):
    # --- MAIN LOOP ---
    abns = _search_abns()
    logger.info(f"Found {len(abns)} ABNs")

    # De-duplicate while preserving order
    seen = set()
    abns = [a for a in abns if not (a in seen or seen.add(a))]

    for i in range(0, len(abns), BATCH_SIZE):
        abn_batch = abns[i:i + BATCH_SIZE]
        batch_data = []

        # --- Fetch ABN details concurrently with bounded concurrency ---
        def _safe_fetch(a: str):
            try:
                details = _fetch_abn_details(a)
                return details
            except requests.RequestException as e:
                logger.warning(f"Error fetching ABN {a}: {e}")
                return a, None

        with concurrent.futures.ThreadPoolExecutor(max_workers=ABN_DETAILS_CONCURRENCY) as pool:
            results = list(pool.map(_safe_fetch, abn_batch))

        for details in results:
            if details is None:
                continue
            batch_data.append(details)

        if not batch_data:
            logger.info(f"No ABN details fetched for batch {i // BATCH_SIZE + 1}")
            continue

        # --- Build prompts for GenAI where ACN exists ---
        genai_prompts: List[str] = []
        genai_indices: List[int] = []

        for idx, item in enumerate(batch_data):
            entity_name = item[8]
            state_code = item[6]
            genai_prompts.append(
                f"give me the website, contact number, social media links, total reviews, Industry and address of '{entity_name}', {state_code}, Australia. I want review in format of 4/5 like that"
            )
            genai_indices.append(idx)
            print(genai_indices)
            print(genai_prompts)
        # --- Call Generative AI in batches ---
        genai_results: List[ABNDetails] = []
        if GENAI_CLIENT and genai_prompts:
            for j in range(0, len(genai_prompts), GENAI_BATCH_SIZE):
                batch_prompts = genai_prompts[j:j + GENAI_BATCH_SIZE]
                try:
                    response = GENAI_CLIENT.models.generate_content(
                        model="gemini-2.0-flash",
                        contents=batch_prompts,
                        config={
                            "response_mime_type": "application/json",
                            "response_schema": list[ABNDetails],
                        },
                    )
                    genai_results.extend(response.parsed)
                except Exception as e:
                    logger.warning(f"Error in Generative AI batch call (ABN): {e}")
                    genai_results.extend([
                        ABNDetails(Contact="", Website="", Address="", Email="", SocialLink=[], review="", Industry="")
                        for _ in batch_prompts
                    ])
        else:
            # Fill with empty results if client not configured
            genai_results = [ABNDetails(Contact="", Website="", Address="", Email="", SocialLink=[], review="", Industry="") for _ in genai_prompts]


        # --- Combine results back into batch_data ---
        for offset, idx in enumerate(genai_indices):
            if offset < len(genai_results):
                r = genai_results[offset]
                batch_data[idx]['Contact'] = r.Contact
                batch_data[idx]['Website'] = r.Website
                batch_data[idx]['Address'] = r.Address
                batch_data[idx]['Email'] = r.Email
                batch_data[idx]['SocialLink'] = r.SocialLink
                batch_data[idx]['Review'] = r.review
                batch_data[idx]['Industry'] = r.Industry

        # Ensure defaults for items without ACN or missing fields
        processed_batch_data: List[Dict[str, Any]] = []
        for item in batch_data:
            item.setdefault('Contact', "")
            item.setdefault('Website', "")
            item.setdefault('Address', "")
            item.setdefault('Email', "")
            item.setdefault('SocialLink', [])
            item.setdefault('Review', "")
            item.setdefault('Industry', "")
            processed_batch_data.append(item)

        # --- Insert into DB ---
        batch_df = pd.json_normalize(processed_batch_data)
        if not batch_df.empty:
            insert_batch_to_postgres(batch_df)
            logger.info(f"Inserted batch {i // BATCH_SIZE + 1}")
        else:
            logger.info(f"No data in batch {i // BATCH_SIZE + 1}")

        if BATCH_PAUSE_SECONDS > 0:
            time.sleep(BATCH_PAUSE_SECONDS)

       

def _event_from_env():
    payload = os.getenv("FC_EVENT_JSON")
    if not payload:
        return {}
    try:
        return json.loads(payload)
    except Exception:
        return {}

def main():
    event = _event_from_env()
    lambda_handler(event, None)

if __name__ == "__main__":
    main()
