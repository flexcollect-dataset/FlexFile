import os
import psycopg2
import logging

# Set up logging
logger = logging.getLogger(__name__)

DB_HOST = "flexdataset.cluster-cpoeqq6cwu00.ap-southeast-2.rds.amazonaws.com"
DB_NAME = "FlexDataseterMaster"
DB_USER = "FlexUser"
DB_PASS = "Luffy123&&Lucky"
DB_PORT = "5432"

def get_connection():
    logger.info("Creating new database connection")
    try:
        return psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT,
        )
    except Exception as exc:
        logger.error(f"Database connection failed: {exc}")
        raise


