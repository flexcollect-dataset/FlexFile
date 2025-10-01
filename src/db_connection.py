import os
import psycopg2
import logging

# Set up logging
logger = logging.getLogger(__name__)

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = int(os.getenv("DB_PORT", "5432"))

def get_connection():
    if not all([DB_HOST, DB_NAME, DB_USER, DB_PASS]):
        missing = [
            name for name, value in (
                ("DB_HOST", DB_HOST),
                ("DB_NAME", DB_NAME),
                ("DB_USER", DB_USER),
                ("DB_PASS", DB_PASS),
            ) if not value
        ]
        raise ValueError(f"Missing required database settings: {', '.join(missing)}")

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


