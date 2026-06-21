import os
import json
import logging
from dotenv import load_dotenv
from google.cloud import storage
load_dotenv()
logger = logging.getLogger(__name__)
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
def get_gcs_client():
    """
    Creates GCS client using service account
    path from GOOGLE_APPLICATION_CREDENTIALS
    """
    if not BUCKET_NAME:
        raise ValueError("GCS_BUCKET_NAME missing in .env")
    return storage.Client()

def upload_to_bronze(data, gcs_path):
    """
    Upload raw JSON data to bronze layer
    Example:
    roads/2026-01-01/roads.json
    """
    client = get_gcs_client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(
        f"bronze/{gcs_path}"
    )
    json_data = json.dumps( data,indent=2)
    blob.upload_from_string(json_data,content_type="application/json")
    logger.info(f"Uploaded gs://{BUCKET_NAME}/bronze/{gcs_path}")

def download_from_bronze(gcs_path):
    """
    Download JSON from bronze
    """
    client = get_gcs_client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(
        f"bronze/{gcs_path}"
    )
    content = blob.download_as_text()
    return json.loads(content)

def count_bronze_records(gcs_path):
    """
    Used by Airflow validation task
    """
    data = download_from_bronze(gcs_path)
    if isinstance(data, dict) and "elements" in data:
        return len(data["elements"])
    return len(data) if isinstance(data, list) else 0

