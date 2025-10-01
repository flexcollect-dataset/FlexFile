import json
import os
import psycopg2
import logging

# Set up logging
logger = logging.getLogger(__name__)
conn = None

# Use environment variables for sensitive data
# It's a best practice to store credentials in AWS Secrets Manager or Parameter Store
DB_HOST = "flexdataset.cluster-cpoeqq6cwu00.ap-southeast-2.rds.amazonaws.com"
DB_NAME = "FlexDataseterMaster"
DB_USER = "FlexUser"
DB_PASS = "Luffy123&&Lucky"
DB_PORT = "5432"

def get_connection():
    global conn
    try:
        # Establish a connection to the database
        if conn is None or conn.closed: 
            logger.info("Attempting to connect to the database...")
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASS,
                port=DB_PORT
            )
            logger.info("Successfully connected to the database.")
            conn.autocommit = False 
        return conn

    except Exception as e:
        logger.error(f"Error connecting to or querying the database: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }


