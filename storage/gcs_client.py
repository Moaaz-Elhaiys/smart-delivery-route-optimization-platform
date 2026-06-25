# storage/gcs_client.py
import functools
from google.cloud import storage
from config import GCS_BUCKET_NAME, GCS_CREDENTIALS_PATH

@functools.lru_cache(maxsize=1)
def get_gcs_client() -> storage.Client:
    """Singleton GCS client — reuse across the entire process."""
    return storage.Client.from_service_account_json(GCS_CREDENTIALS_PATH)

@functools.lru_cache(maxsize=1)
def get_gcs_bucket() -> storage.Bucket:
    return get_gcs_client().bucket(GCS_BUCKET_NAME)