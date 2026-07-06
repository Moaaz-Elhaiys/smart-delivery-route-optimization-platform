import os
import json
import logging
from storage.gcs_client import get_gcs_bucket
logger = logging.getLogger(__name__)
from config import GCS_BUCKET_NAME

def upload_to_bronze(data, gcs_path, metadata=None):
    """
    Upload raw JSON data to bronze layer
    Example:
    roads/2026-01-01/roads.json
    """
    bucket = get_gcs_bucket()

    blob = bucket.blob(
        f"bronze/{gcs_path}"
    )
    json_data = json.dumps( data,indent=2)
    blob.upload_from_string(json_data,content_type="application/json")
    logger.info(f"Uploaded gs://{GCS_BUCKET_NAME}/bronze/{gcs_path}")

def download_from_bronze(gcs_path):
    """
    Download JSON from bronze
    """
    bucket = get_gcs_bucket()
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

