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

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

GENAI_API_KEY = "AIzaSyD1VmH7wuQVqxld5LeKjF79eRq1gqVrNFA"
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

    # Read CSV
    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.normpath(os.path.join(base_dir, "..", "data", "TaxRecords.csv"))
    df = pd.read_csv(csv_path, low_memory=False)

    # Add enrichment columns if missing
    enrich_cols = ["Contact", "Website", "Address", "Email", "SocialLink", "Review", "Industry_enriched", "Documents"]
    df = _ensure_columns(df, enrich_cols)

    # Build prompts based on existing columns
    batch_size = 60
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
                f"Give me the website, contact number, social media links, total reviews, Industry and address of '{entity}', {state}, Australia. Return JSON with keys Contact, Website, Address, Email, SocialLink (array), review, Industry. Use review format like 4/5."
            )
            row_indices.append(i)
        # If ACN exists, ask for ASIC published notices
        acn = str(row.get("acn") or "").strip()
        if acn and acn.lower() != "nan":
            acn_prompts.append(
                "Using https://publishednotices.asic.gov.au/browsesearch-notices fetch all the notices related to ACN " + acn + ". Return JSON with keys documentid, dateofpublication, noticetype (single best match if multiple)."
            )
            acn_row_indices.append(i)

    # Call Gemini for entity enrichment in batches
    results = []
    if GENAI_CLIENT and prompts:
        for start in range(0, len(prompts), batch_size):
            batch = prompts[start:start+batch_size]
            try:
                resp = GENAI_CLIENT.models.generate_content(
                    model="gemini-2.0-flash",
                    contents=batch,
                    config={
                        "response_mime_type": "application/json",
                        "response_schema": list[ABNDetails],
                    },
                )
                results.extend(resp.parsed)
                logger.info(f"Gemini data1")
            except Exception as e:
                logger.warning(f"Gemini enrichment error: {e}")
                # Fallback to blanks for this batch
                results.extend([
                    ABNDetails(Contact="", Website="", Address="", Email="", SocialLink=[], review="", Industry="")
                    for _ in batch
                ])
    else:
        # No client => blanks
        results = [ABNDetails(Contact="", Website="", Address="", Email="", SocialLink=[], review="", Industry="") for _ in prompts]

    # Call Gemini for ACN notices
    acn_results = []
    if GENAI_CLIENT and acn_prompts:
        for start in range(0, len(acn_prompts), batch_size):
            batch = acn_prompts[start:start+batch_size]
            try:
                dresp = GENAI_CLIENT.models.generate_content(
                    model="gemini-2.0-flash",
                    contents=batch,
                    config={
                        "response_mime_type": "application/json",
                        "response_schema": list[ACNDocumentsDetails],
                    },
                )
                acn_results.extend(dresp.parsed)
                logger.info(f"Gemini data2")
            except Exception as e:
                logger.warning(f"Gemini ACN docs error: {e}")
                acn_results.extend([ACNDocumentsDetails(documentid="", dateofpublication="", noticetype="") for _ in batch])
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

    # Backup then write in-place
    backup_path = str(csv_path) + ".bak"
    shutil.copyfile(csv_path, backup_path)
    df.to_csv(csv_path, index=False)
    logger.info(f"Enriched CSV written to {csv_path}; backup at {backup_path}")
    return csv_path



# Call before the existing main() fallthrough (in Lambda it's harmless)
enrich_tax_records_csv()
