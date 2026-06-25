# config.py
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Project paths ──
PROJECT_ROOT = Path(__file__).parent.resolve()

# ── GCS ──
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# ── PostGIS ──
DB_CONFIG = {
    "dbname":   os.getenv("DB_NAME", "delivery_platform"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
}

# ── Cairo constants ──
CAIRO_BBOX = (29.9, 31.1, 30.2, 31.5)
CAIRO_CENTER = (30.0444, 31.2357)
CAIRO_CRS_METRIC = "EPSG:32636"  # UTM Zone 36N

# ── Data quality thresholds ──
MAX_DROP_RATE_PCT = float(os.getenv("MAX_DROP_RATE_PCT", 5.0))
MIN_ROAD_ELEMENTS = int(os.getenv("MIN_ROAD_ELEMENTS", 500))
DEFAULT_ORDER_COUNT = int(os.getenv("DEFAULT_ORDER_COUNT", 500))
DEFAULT_DRIVER_COUNT = int(os.getenv("DEFAULT_DRIVER_COUNT", 25))

# ── Validation at import time ──
_required = ["GCS_BUCKET_NAME", "GOOGLE_APPLICATION_CREDENTIALS", "DB_PASSWORD"]
_missing = [k for k in _required if not os.getenv(k)]
if _missing:
    raise EnvironmentError(
        f"Missing required environment variables: {_missing}. "
        f"Check your .env file against .env.example"
    )