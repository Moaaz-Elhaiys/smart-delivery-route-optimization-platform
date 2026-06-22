# Smart Delivery Route Optimization Platform
## Solo Project Plan — Learning Edition

> **Your setup:** Mac M1 Air (8GB) as your daily dev machine · Windows Laptop (16GB) as your heavy-processing node · Free-tier cloud for persistence and CI
> **Goal:** Deep learning across Data Engineering, Geospatial Analytics, Distributed Computing, and Optimization
> **Estimated duration:** 14 weeks (solo, part-time ~10–15 hrs/week)

---

## Table of Contents

1. [Machine Role Assignment](#machine-role-assignment)
2. [Free Cloud Integration](#free-cloud-integration)
3. [Architecture Overview](#architecture-overview)
4. [Environment Setup](#environment-setup)
5. [Phase 0 – Foundation (Week 1–2)](#phase-0--foundation-week-12)
6. [Phase 1 – Data Ingestion (Week 3–4)](#phase-1--data-ingestion-week-34)
7. [Phase 2 – Spark & Sedona Processing (Week 5–7)](#phase-2--spark--sedona-processing-week-57)
8. [Phase 3 – Route Optimization (Week 8–10)](#phase-3--route-optimization-week-810)
9. [Phase 4 – PostGIS & Storage (Week 11–12)](#phase-4--postgis--storage-week-1112)
10. [Phase 5 – Dashboard & Final Integration (Week 13–14)](#phase-5--dashboard--final-integration-week-1314)
11. [Data Governance & Quality Gates](#data-governance--quality-gates)
12. [Testing Strategy](#testing-strategy)
13. [Learning Checkpoints](#learning-checkpoints)
14. [Risk Mitigation](#risk-mitigation)
15. [Folder Structure](#folder-structure)

---

## Machine Role Assignment

This is the most important decision in your setup. You have two machines with very different profiles. Assign roles clearly and connect them over your local network (or via SSH).

### Mac M1 Air — 8GB RAM → "Dev & Orchestration Node"

| Responsibility | Why here |
|---|---|
| VS Code, Git, all code editing | Your daily driver |
| Apache Airflow (orchestrator only) | Airflow scheduler is lightweight (~500MB RAM) |
| Streamlit dashboard | Minimal memory footprint |
| PostGIS database | PostgreSQL + PostGIS runs fine at 8GB with a small dataset |
| Python scripts, OR-Tools optimizer | OR-Tools is single-threaded, no JVM overhead |
| SSH client → connects to Windows for Spark jobs | You trigger jobs from Mac, they run on Windows |

**Do NOT run Spark or Sedona on the Mac.** The JVM alone eats 3–4GB. On 8GB with macOS overhead, it will swap and become unusable.

### Windows Laptop — 16GB RAM → "Processing Node"

| Responsibility | Why here |
|---|---|
| Apache Spark (master + 1 worker) | Needs 6–8GB RAM minimum for Sedona jobs |
| Apache Sedona | Geospatial operations are memory-hungry |
| Heavy Parquet transformations | I/O and CPU intensive |
| Docker Desktop (Windows) | Runs the Spark cluster in containers |

---

### How the Two Machines Communicate

You will SSH from Mac into Windows to submit Spark jobs. Here is the exact setup:

#### Step 1 — Enable SSH on Windows
```
Settings → System → Optional Features → Add "OpenSSH Server"
Services → OpenSSH SSH Server → Set to Automatic → Start
```

#### Step 2 — Find your Windows LAN IP
```cmd
ipconfig
# Note the IPv4 address, e.g. 192.168.1.50
```

#### Step 3 — SSH from Mac to Windows
```bash
# From your Mac terminal
ssh your_windows_username@192.168.1.50

# Add to ~/.ssh/config for convenience
Host winbox
  HostName 192.168.1.50
  User your_windows_username

# Then just use:
ssh winbox
```

#### Step 4 — Mount Windows Spark output folder on Mac (optional but convenient)
```bash
# Install sshfs on Mac
brew install macfuse sshfs

# Mount Windows output directory to Mac
sshfs winbox:/path/to/spark/output ~/win_output

# Now ~/win_output on Mac shows Windows files
```

#### Step 5 — Submit Spark jobs remotely from Mac via Airflow
Your Airflow DAGs on Mac will use `SSHOperator` to trigger Spark jobs on Windows:
```python
from airflow.providers.ssh.operators.ssh import SSHOperator

spark_job = SSHOperator(
    task_id='run_sedona_job',
    ssh_conn_id='windows_spark',  # configured in Airflow Connections
    command='cd /path/to/project && spark-submit jobs/spatial_processing.py',
)
```

---

## Free Cloud Integration

You will use **three free-tier cloud services**, each doing something the local machines cannot reliably do.

### 1. Google Cloud Storage (GCS) — Free Tier
**Purpose:** Persistent Bronze/Silver/Gold data lake storage. Your Parquet files live here so they survive machine restarts and are accessible from both machines.

**Free tier:** 5GB standard storage forever (more than enough for OSM + simulated data)

```bash
# Setup
# 1. Create account at console.cloud.google.com
# 2. Create a project: delivery-optimization
# 3. Create bucket: delivery-data-lake-yourname
# 4. Download service account key JSON

# Install on both machines
pip install google-cloud-storage

# Set credentials
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/key.json"

# In Python
from google.cloud import storage
client = storage.Client()
bucket = client.bucket("delivery-data-lake-yourname")
```

**How it fits in your pipeline:**
- Airflow (Mac) downloads OSM data → uploads raw JSON to GCS `bronze/`
- Spark (Windows) reads from GCS `bronze/` → processes → writes Parquet to GCS `silver/` and `gold/`
- OR-Tools (Mac) reads Gold layer from GCS → runs optimization → writes results back
- PostGIS (Mac) loads from GCS Gold layer

### 2. GitHub Actions — Free CI (2,000 min/month)
**Purpose:** Run your DAG tests, unit tests, and data quality checks automatically on every push. Enforces discipline and proves the project is production-minded.

```yaml
# .github/workflows/test.yml
name: Pipeline Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install uv
        uses: astral-sh/setup-uv@v5
        with:
          enable-cache: true
          version: "latest"

      - name: Set up Python 3.11
        run: uv python install 3.11

      - name: Install Dependencies
        run: uv pip install -r pyproject.toml

      - name: Run Tests
        run: uv run pytest tests/ -v --tb=short

      - name: Validate Schemas
        run: uv run python scripts/validate_schemas.py
```

### 3. Render.com — Free Web Hosting
**Purpose:** Deploy your Streamlit dashboard publicly so you can share it in your portfolio without keeping your Mac running 24/7.

**Free tier:** 750 hours/month (enough for one always-on service)

```bash
# Deploy steps
# 1. Create account at render.com
# 2. Connect GitHub repo
# 3. New Web Service → Python → Build command: pip install -r requirements.txt
# 4. Start command: streamlit run dashboard/app.py --server.port $PORT
```

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        MAC M1 AIR (8GB)                             │
│                                                                     │
│  ┌─────────────┐    ┌──────────────┐    ┌───────────────────────┐  │
│  │   Airflow   │───▶│ OSM Ingestion│───▶│  GCS Bronze Layer     │  │
│  │ (scheduler) │    │   (Python)   │    │  raw JSON / OSM data  │  │
│  └──────┬──────┘    └──────────────┘    └───────────────────────┘  │
│         │ SSHOperator                                               │
│         │                                         ▲                 │
│  ┌──────▼──────┐    ┌──────────────┐    ┌────────┴──────────────┐  │
│  │  OR-Tools   │◀───│  GCS Gold    │    │   GCS Silver Layer    │  │
│  │ (optimizer) │    │   Layer      │    │   cleaned Parquet     │  │
│  └──────┬──────┘    └──────────────┘    └───────────────────────┘  │
│         │                                         ▲                 │
│  ┌──────▼──────┐    ┌──────────────┐             │                 │
│  │   PostGIS   │    │  Streamlit   │             │ SSH / submit    │
│  │ (results)   │    │  Dashboard   │             │                 │
│  └─────────────┘    └──────────────┘             │                 │
└─────────────────────────────────────────────────┼───────────────────┘
                                                  │
┌─────────────────────────────────────────────────▼───────────────────┐
│                      WINDOWS LAPTOP (16GB)                          │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                    Docker Compose                            │   │
│  │  ┌─────────────────┐      ┌──────────────────────────────┐  │   │
│  │  │  Spark Master   │      │       Spark Worker           │  │   │
│  │  │  (2GB)          │      │  + Apache Sedona (6GB)       │  │   │
│  │  └────────┬────────┘      └──────────────────────────────┘  │   │
│  └───────────┼──────────────────────────────────────────────────┘   │
│              │ reads/writes GCS via service account                  │
└──────────────┼──────────────────────────────────────────────────────┘
               │
        ┌──────▼───────┐
        │ Google Cloud │
        │   Storage    │
        │ (free tier)  │
        └──────────────┘
```

---

## Environment Setup

### Mac M1 Air Setup

#### Install core tools
```bash
# Homebrew (if not installed)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"


# PostgreSQL + PostGIS
brew install postgresql@15 postgis
brew services start postgresql@15

# Project tools
brew install git curl wget jq uv
```

#### Create PostGIS database
```bash
psql postgres

CREATE DATABASE delivery_platform;
\c delivery_platform
CREATE EXTENSION postgis;
CREATE EXTENSION postgis_topology;

# Verify
SELECT PostGIS_Version();
```

#### Create Python environment (Mac)
```bash
cd ~/projects/delivery-platform

# Initialize a new uv project (creates a pyproject.toml)
uv init

# Tell uv to pin the project to Python 3.11
uv python pin 3.11

# install your dependencies using Airflow 2.9.0's official constraint file
uv add \
  "apache-airflow==2.9.0" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-3.11.txt"

# Add the rest of your platform's dependencies
uv add \
    apache-airflow-providers-ssh \
    psycopg2-binary \
    ortools \
    streamlit \
    folium \
    streamlit-folium \
    geopandas \
    shapely \
    pyproj \
    requests \
    google-cloud-storage \
    great-expectations \
    pytest \
    python-dotenv
```
#### Add SSH connection to the airflow (Mac)
```bash
uv run airflow connections add 'windows_spark' \
    --conn-type 'ssh' \
    --conn-host '192.168.100.0' \
    --conn-login 'your_windows_username' \
    --conn-password 'your_windows_password' \
    --conn-port 22
```

#### Initialize Airflow
```bash
export AIRFLOW_HOME=~/projects/delivery-platform/airflow

# Initialize the DB using uv run
uv run airflow db init

# Create the user
uv run airflow users create \
  --username admin \
  --password admin \
  --firstname Solo \
  --lastname Dev \
  --role Admin \
  --email admin@local.com

# Start the components (in separate terminals)
uv run airflow webserver --port 8080
uv run airflow scheduler

```

---

### Windows Laptop Setup

#### Install Docker Desktop for Windows
Download from docker.com — enable WSL2 backend during install.

#### docker-compose.yml for Spark cluster
Save this to `C:\projects\delivery-platform\docker-compose.yml`:

```yaml
services:
  spark-master:
    image: bitnamilegacy/spark:3.5
    container_name: delivery-platform-spark-master-1
    environment:
      - SPARK_MODE=master
      - SPARK_MASTER_OPTS=-Dspark.ui.port=8080
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    ports:
      - "7077:7077"   # Spark Master port (used by Airflow & workers)
      - "8081:8080"   # Spark Master Web UI port
    volumes:
      - ./jobs:/opt/bitnami/spark/jobs
      - ./data:/opt/bitnami/spark/data
      - ./jars:/opt/bitnami/spark/custom_jars
      - ${GOOGLE_APPLICATION_CREDENTIALS}:/opt/bitnami/spark/conf/gcs-key.json
    mem_limit: 3g

  spark-worker:
    image: bitnamilegacy/spark:3.5
    container_name: delivery-platform-spark-worker-1
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=10G
      - SPARK_WORKER_CORES=4
      - SPARK_EXTRA_CLASSPATH=/opt/bitnami/spark/custom_jars/sedona-spark-shaded-3.0_2.12-1.5.1.jar:/opt/bitnami/spark/custom_jars/geotools-wrapper-1.5.1-28.2.jar:/opt/bitnami/spark/custom_jars/gcs-connector.jar
    volumes:
      - ./jobs:/opt/bitnami/spark/jobs
      - ./data:/opt/bitnami/spark/data
      - ./jars:/opt/bitnami/spark/custom_jars
      - ${GOOGLE_APPLICATION_CREDENTIALS}:/opt/bitnami/spark/conf/gcs-key.json
    depends_on:
      - spark-master
    mem_limit: 12g

volumes:
  spark_data:
```

#### Download Sedona JARs (run on Windows)
```powershell
# In PowerShell, in your project jars/ folder
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.0_2.12/1.5.1/sedona-spark-shaded-3.0_2.12-1.5.1.jar" -OutFile "jars/sedona-spark-shaded.jar"

Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/1.5.1-28.2/geotools-wrapper-1.5.1-28.2.jar" -OutFile "jars/geotools-wrapper.jar"
```

#### GCS connector for Spark (Windows)
```powershell
Invoke-WebRequest -Uri "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar" -OutFile "jars/gcs-connector.jar"
```

#### Start the cluster
```powershell
cd C:\projects\delivery-platform
docker-compose up -d
# Verify at http://localhost:8081
```

---

## Phase 0 — Foundation (Week 1–2)

**Goal:** Both machines running, talking to each other, talking to GCS. No pipeline yet — just infrastructure confidence.

### Week 1 Checklist

- [ ] Mac: Python env, Airflow running, PostGIS responding
- [ ] Windows: Docker Desktop, Spark cluster up at `localhost:8081`
- [ ] SSH from Mac → Windows working (`ssh winbox`)
- [ ] GCS bucket created, credentials on both machines
- [ ] GitHub repo created, `.env.example` committed, `.gitignore` set

### Week 2 Checklist

- [ ] Write and run a "test_ssh_connection" Airflow DAG (prints date, logs to Airflow UI)
- [ ] Submit a "test_ssh_connection" Spark job from Mac via SSH to Windows
- [ ] Write a test GCS upload/download script from both machines
- [ ] Set up Airflow SSH connection to Windows in Airflow Connections UI
- [ ] Commit folder structure and `docker-compose.yml`

### test_ssh_connection : SSH test from Airflow (mac) to windows
```python
# airflow/dags/test_ssh_connection.py
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

with DAG(
    dag_id='test_ssh_connection',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    # 1. TEST THE BASE CONNECTION FIRST
    test_ssh = SSHOperator(
        task_id='test_basic_ssh',
        ssh_conn_id='windows_spark',
        command='whoami',  # Simple native command. Windows will return "computer_name\username"
    )

    # 2. TEST THE DOCKER HANDSHAKE (Optional verification)
    test_docker = SSHOperator(
        task_id='test_windows_docker_cli',
        ssh_conn_id='windows_spark',
        command='docker ps',  # Verifies OpenSSH has permissions to talk to Docker Desktop
    )

    test_ssh >> test_docker
```

### Learning Goal — Phase 0
Understand **why** this architecture splits work across machines. Read the Airflow architecture docs (scheduler, executor, worker model). Understand Docker networking. Understand what a service account key is and why it's secret.

---

## Phase 1 — Data Ingestion (Week 3–4)

**Goal:** Collect real OSM road network data for Cairo, simulate realistic delivery data, land everything in GCS Bronze layer.

### What you will build

```
ingestion/
  overpass_client.py     # OSM road network fetcher with retry logic
  data_simulator.py      # Generates realistic driver/order data
  bronze_writer.py       # Uploads raw data to GCS bronze/
  schemas.py             # Defines expected data schemas (validated at ingest)

airflow/dags/
  ingestion_dag.py       # Orchestrates all ingestion tasks
```

### Overpass API Client with Retry Logic
```python
# ingestion/overpass_client.py
import requests
import time
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Cairo bounding box: south, west, north, east
CAIRO_BBOX = (29.9, 31.1, 30.2, 31.5)

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

ROAD_QUERY = """
[out:json][timeout:90][bbox:{south},{west},{north},{east}];
(
  way["highway"~"motorway|trunk|primary|secondary|residential|service"];
);
out body geom;
""".strip()
def fetch_roads(bbox=CAIRO_BBOX, max_retries=3, backoff_seconds=10):
    south, west, north, east = bbox
    query = ROAD_QUERY.format(south=south, west=west, north=north, east=east)

    for attempt in range(1, max_retries + 1):
        try:
                        logger.info(f"Fetching roads, attempt {attempt}")
                        response = requests.post(
                            OVERPASS_URL,
                            data={"data": query},
                            headers={
                                "User-Agent": "Smart-Delivery-Route-Optimization-Platform/1.0"
                            },
                            timeout=120,
            )
                        response.raise_for_status()
                        data = response.json()
                        logger.info(f"Fetched {len(data.get('elements', []))} road elements")
                        return data
        except requests.exceptions.HTTPError as e:
            if response.status_code != 429:
                raise
            wait = backoff_seconds * (2 ** attempt)
            logger.warning(f"Rate limited. Waiting {wait}s before retry.")
            time.sleep(wait)
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout on attempt {attempt}. Retrying...")
            time.sleep(backoff_seconds)

    raise RuntimeError("Max retries exceeded fetching OSM data")
# terminal test
if __name__ == "__main__":
    import json
    import logging

    logging.basicConfig(level=logging.INFO)

    roads = fetch_roads()

    print(f"Total roads: {len(roads['elements'])}")

    with open("roads_test.json", "w") as f:
        json.dump(roads, f, indent=2)

    print("Done")
```

### Data Simulator
```python
# ingestion/data_simulator.py
import random
import json
import uuid
from datetime import datetime, timedelta

# Cairo districts bounding boxes (simplified)
DISTRICTS = {
    "Maadi":        {"lat": (29.95, 30.00), "lon": (31.22, 31.28)},
    "Zamalek":      {"lat": (30.05, 30.07), "lon": (31.21, 31.24)},
    "Heliopolis":   {"lat": (30.08, 30.12), "lon": (31.31, 31.36)},
    "Dokki":        {"lat": (30.03, 30.06), "lon": (31.20, 31.22)},
    "Nasr City":    {"lat": (30.05, 30.10), "lon": (31.30, 31.35)},
    "New Cairo":    {"lat": (30.00, 30.05), "lon": (31.40, 31.50)},
}

def random_point_in_district(district_name):
    d = DISTRICTS[district_name]
    lat = random.uniform(*d["lat"])
    lon = random.uniform(*d["lon"])
    return lat, lon

def simulate_orders(n=500, date=None):
    date = date or datetime.utcnow().date()
    orders = []
    for _ in range(n):
        district = random.choice(list(DISTRICTS.keys()))
        lat, lon = random_point_in_district(district)
        created_offset = random.randint(0, 8 * 3600)
        orders.append({
            "order_id": str(uuid.uuid4()),
            "lat": lat,
            "lon": lon,
            "district": district,
            "priority": random.choice(["high", "medium", "low"]),
            "weight_kg": round(random.uniform(0.5, 15.0), 2),
            "created_at": (
                datetime.combine(date, datetime.min.time()) +
                timedelta(seconds=created_offset)
            ).isoformat(),
            "delivery_window_start": "09:00",
            "delivery_window_end": "21:00",
        })
    return orders

def simulate_drivers(n=20):
    drivers = []
    for i in range(n):
        district = random.choice(list(DISTRICTS.keys()))
        lat, lon = random_point_in_district(district)
        drivers.append({
            "driver_id": f"DRV-{i+1:03d}",
            "lat": lat,
            "lon": lon,
            "capacity_kg": random.choice([20.0, 30.0, 50.0]),
            "status": "available",
            "district": district,
        })
    return drivers
```
### Write data to GCS bucket 
```python
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
```
### Schema validation
```python
# ingestion/schemas.py
from datetime import datetime
# -------------------------
# Required fields
# -------------------------
ORDER_REQUIRED_FIELDS = {
    "order_id",
    "lat",
    "lon",
    "district",
    "priority",
    "weight_kg",
    "created_at",
    "delivery_window_start",
    "delivery_window_end",
}
DRIVER_REQUIRED_FIELDS = {
    "driver_id",
    "lat",
    "lon",
    "capacity_kg",
    "status",
    "district",
}
ROAD_REQUIRED_FIELDS = {
    "type",
    "id",
    "geometry",
}
# -------------------------
# Generic validation
# -------------------------
def validate_required_fields(record, required_fields):
    """
    Checks if one dictionary has all required fields
    """
    if missing := required_fields - record.keys():
        raise ValueError(
            f"Missing fields: {missing}"
        )
    return True
# -------------------------
# Orders validation
# -------------------------
def validate_orders(orders):
    """
    Validate simulated orders before Bronze upload
    """
    if not isinstance(orders, list):
        raise TypeError("Orders must be a list")
    for order in orders:
        validate_required_fields(order,ORDER_REQUIRED_FIELDS)

        if not isinstance(order["lat"], float):
            raise TypeError("lat must be float")
        if not isinstance(order["lon"], float):
            raise TypeError("lon must be float")
        if order["priority"] not in ["high","medium","low"]:
            raise ValueError("Invalid priority")
    return True
# -------------------------
# Drivers validation
# -------------------------
def validate_drivers(drivers):
    if not isinstance(drivers, list):
        raise TypeError("Drivers must be list")
    for driver in drivers:
        validate_required_fields(driver,DRIVER_REQUIRED_FIELDS)
        if driver["status"] not in ["available","busy","offline"]:
            raise ValueError("Invalid driver status")
    return True
# -------------------------
# OSM roads validation
# -------------------------
def validate_roads(osm_data):
    """
    Validate Overpass response
    """
    if not isinstance(osm_data, dict):
        raise TypeError("OSM response must be dict")
    if "elements" not in osm_data:
        raise ValueError("Missing elements from OSM response")
    roads = osm_data["elements"]
    if len(roads) == 0:
        raise ValueError("No roads received")
    for road in roads[:10]:
        validate_required_fields(road,ROAD_REQUIRED_FIELDS)
    return True
# -------------------------
# Test locally
# -------------------------
if __name__ == "__main__":

    from data_simulator import (simulate_orders,simulate_drivers)
    orders = simulate_orders(10)
    drivers = simulate_drivers(5)
    validate_orders(orders)
    validate_drivers(drivers)
    print("Schema validation passed")
```
### Airflow Ingestion DAG
```python
# airflow/dags/ingestion_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.insert(0, '/path/to/your/project')

from ingestion.overpass_client import fetch_roads
from ingestion.data_simulator import simulate_orders, simulate_drivers
from ingestion.bronze_writer import upload_to_bronze

def ingest_roads(**context):
    data = fetch_roads()
    upload_to_bronze(data, f"roads/{context['ds']}/roads.json")

def ingest_orders(**context):
    orders = simulate_orders(n=500)
    upload_to_bronze(orders, f"orders/{context['ds']}/orders.json")

def ingest_drivers(**context):
    drivers = simulate_drivers(n=25)
    upload_to_bronze(drivers, f"drivers/{context['ds']}/drivers.json")

def validate_bronze(**context):
    # Basic sanity check — fail the DAG if data is missing
    from ingestion.bronze_writer import count_bronze_records
    roads = count_bronze_records(f"roads/{context['ds']}/roads.json")
    orders = count_bronze_records(f"orders/{context['ds']}/orders.json")
    assert roads > 100, f"Too few road elements: {roads}"
    assert orders == 500, f"Expected 500 orders, got {orders}"
    print(f"Validation passed: {roads} roads, {orders} orders")

with DAG(
    dag_id='ingestion_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['bronze', 'ingestion'],
) as dag:

    t_roads   = PythonOperator(task_id='ingest_roads',   python_callable=ingest_roads)
    t_orders  = PythonOperator(task_id='ingest_orders',  python_callable=ingest_orders)
    t_drivers = PythonOperator(task_id='ingest_drivers', python_callable=ingest_drivers)
    t_validate = PythonOperator(task_id='validate_bronze', python_callable=validate_bronze)

    [t_roads, t_orders, t_drivers] >> t_validate
```

### Learning Goals — Phase 1
- Understand the Overpass Query Language (OverQL) — try different road type filters
- Understand why exponential backoff matters for public APIs
- Understand what a Parquet file is vs JSON — read about columnar storage
- Understand GCS bucket structure and why you partition by date (`orders/2024-01-15/`)

---

## Phase 2 — Spark & Sedona Processing (Week 5–7)

**Goal:** Transform raw Bronze data into clean, enriched, spatially-indexed Silver and Gold layers. All Spark jobs run on Windows, triggered via Airflow SSH from Mac.

### Week 5 — Spark Basics & Silver Layer

Learn Spark fundamentals before adding Sedona complexity.

```python
# jobs/bronze_to_silver.py (runs on Windows Spark cluster)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def create_spark():
    return SparkSession.builder \
        .appName("BronzeToSilver") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                "/opt/bitnami/spark/conf/gcs-key.json") \
        .config("spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .getOrCreate()

def clean_orders(spark, run_date):
    raw_path = f"gs://delivery-data-lake/bronze/orders/{run_date}/"
    df = spark.read.option("multiline", "true").json(raw_path)

    cleaned = df \
        .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull()) \
        .filter((F.col("lat").between(29.5, 30.5)) & (F.col("lon").between(31.0, 31.8))) \
        .filter(F.col("order_id").isNotNull()) \
        .withColumn("lat",  F.col("lat").cast(DoubleType())) \
        .withColumn("lon",  F.col("lon").cast(DoubleType())) \
        .withColumn("created_at", F.to_timestamp("created_at")) \
        .withColumn("date_partition", F.lit(run_date)) \
        .dropDuplicates(["order_id"])

    # Data quality metrics
    total = df.count()
    valid = cleaned.count()
    drop_rate = round((total - valid) / total * 100, 2)
    print(f"Orders: {total} raw → {valid} clean ({drop_rate}% dropped)")
    assert drop_rate < 5.0, f"Drop rate {drop_rate}% exceeds 5% threshold — check data quality"

    silver_path = f"gs://delivery-data-lake/silver/orders/{run_date}/"
    cleaned.write.mode("overwrite").parquet(silver_path)
    print(f"Written to {silver_path}")

if __name__ == "__main__":
    import sys
    run_date = sys.argv[1]  # e.g. "2024-01-15"
    spark = create_spark()
    # ADD THIS LINE: Tells Spark to only log Warnings and Errors
    spark.sparkContext.setLogLevel("WARN")
    clean_orders(spark, run_date)
    spark.stop()
```

### Week 6 — Apache Sedona Spatial Operations

This is the heart of the geospatial learning. Take it slow.

```python
# jobs/spatial_processing.py (runs on Windows Spark cluster)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer

def create_sedona_spark():
    return SparkSession.builder \
        .appName("SpatialProcessing") \
        .config("spark.serializer", KryoSerializer.getName()) \
        .config("spark.kryo.registrator", SedonaKryoRegistrator.getName()) \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                "/opt/bitnami/spark/conf/gcs-key.json") \
        .config("spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .getOrCreate()

def run_spatial_jobs(spark, run_date):
    SedonaRegistrator.registerAll(spark)
    # --- Load silver data ---
    orders_path = f"gs://delivery-data-lake-yourname/silver/orders/{run_date}/"
    orders = spark.read.parquet(orders_path)
    # --- Create geometry column from lat/lon ---
    # IMPORTANT: ST_Point takes (longitude, latitude) — not (lat, lon)
    orders = orders.withColumn(
        "geometry",
        F.expr("ST_Point(CAST(lon AS DECIMAL(24,20)), CAST(lat AS DECIMAL(24,20)))")
    )
    orders.createOrReplaceTempView("orders")
    # --- LEARNING EXERCISE 1: Distance between every order and city center ---
    city_center_lon = 31.2357
    city_center_lat = 30.0444

    orders_with_distance = spark.sql(f"""
        SELECT
            order_id,
            district,
            priority,
            lat, lon,
            ST_Distance(
                ST_Transform(geometry, 'EPSG:4326', 'EPSG:32636'),
                ST_Transform(ST_Point({city_center_lon}, {city_center_lat}), 'EPSG:4326', 'EPSG:32636')
            ) / 1000 AS dist_from_center_km
        FROM orders
    """)
    # Why EPSG:32636? It's UTM Zone 36N — the correct metric projection for Cairo.
    # ST_Distance on EPSG:4326 gives degrees, not meters.

    # --- LEARNING EXERCISE 2: Delivery hotspot detection using ST_Buffer ---
    hotspot_query = """
        SELECT
            district,
            COUNT(*) as order_count,
            ST_AsText(ST_Buffer(
                ST_Transform(ST_Centroid(ST_Collect(geometry)), 'EPSG:4326', 'EPSG:32636'),
                1000  -- 1km buffer around district centroid
            )) as coverage_area_wkt
        FROM orders
        GROUP BY district
    """
    hotspots = spark.sql(hotspot_query)
    hotspots.show()

    # --- LEARNING EXERCISE 3: Spatial join — assign orders to service zones ---
    # (In a real project, zones come from a GeoJSON polygon file)
    # For now, cluster by a grid
    orders_gridded = orders.withColumn(
        "grid_cell",
        F.concat(
            F.round(F.col("lat") * 10).cast("int").cast("string"),
            F.lit("_"),
            F.round(F.col("lon") * 10).cast("int").cast("string")
        )
    )

    # --- Write Gold layer ---
    gold_path = f"gs://delivery-data-lake-yourname/gold/orders_spatial/{run_date}/"
    orders_with_distance.write.mode("overwrite").parquet(gold_path)
    hotspots.write.mode("overwrite").parquet(
        f"gs://delivery-data-lake-yourname/gold/hotspots/{run_date}/"
    )

    print("Spatial processing complete")

if __name__ == "__main__":
    import sys
    run_date = sys.argv[1]
    spark = create_sedona_spark()
    run_spatial_jobs(spark, run_date)
    spark.stop()
```

### Week 7 — Road Network Processing

```python
# jobs/road_network_processing.py (Windows Spark)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sedona.register import SedonaRegistrator

def process_roads(spark, run_date):
    SedonaRegistrator.registerAll(spark)

    raw_path = f"gs://delivery-data-lake-yourname/bronze/roads/{run_date}/"
    roads_raw = spark.read.json(raw_path)

    # Explode OSM elements array
    elements = roads_raw.select(F.explode("elements").alias("elem"))

    # Extract road attributes
    roads = elements.select(
        F.col("elem.id").alias("road_id"),
        F.col("elem.tags.highway").alias("road_type"),
        F.col("elem.tags.maxspeed").alias("maxspeed_raw"),
        F.col("elem.tags.name").alias("road_name"),
        F.col("elem.tags.oneway").alias("is_oneway"),
        F.col("elem.geometry").alias("geometry_nodes"),
    ).filter(F.col("road_type").isNotNull())

    # Parse speed: OSM maxspeed is a string like "60" or "60 mph"
    roads = roads.withColumn(
        "speed_kmh",
        F.when(
            F.col("maxspeed_raw").rlike("^[0-9]+$"),
            F.col("maxspeed_raw").cast("int")
        ).when(
            F.col("maxspeed_raw").rlike("mph"),
            (F.regexp_extract("maxspeed_raw", r"(\d+)", 1).cast("int") * 1.609).cast("int")
        ).otherwise(
            # Default speeds by road type (Cairo approximations)
            F.when(F.col("road_type") == "motorway", 100)
             .when(F.col("road_type") == "trunk", 80)
             .when(F.col("road_type") == "primary", 60)
             .when(F.col("road_type") == "secondary", 50)
             .otherwise(30)
        )
    )

    silver_path = f"gs://delivery-data-lake-yourname/silver/roads/{run_date}/"
    roads.write.mode("overwrite").parquet(silver_path)
    print(f"Roads written: {roads.count()} segments")

if __name__ == "__main__":
    import sys
    run_date = sys.argv[1]
    spark = create_spark()
    process_roads(spark, run_date)
    spark.stop()
```

### Airflow DAG for Spark Jobs (on Mac, triggers Windows)
```python
# airflow/dags/spark_processing_dag.py
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

SPARK_SUBMIT = "docker exec delivery-platform-spark-master-1 spark-submit \
    --master spark://spark-master:7077 \
    --jars /opt/bitnami/spark/jars/sedona-spark-shaded.jar,/opt/bitnami/spark/jars/geotools-wrapper.jar,/opt/bitnami/spark/jars/gcs-connector.jar \
    /opt/bitnami/spark/jobs/{script} {run_date}"

with DAG(
    dag_id='spark_processing',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['silver', 'gold', 'spark'],
) as dag:

    clean_orders = SSHOperator(
        task_id='clean_orders',
        ssh_conn_id='windows_spark',
        command=SPARK_SUBMIT.format(script="bronze_to_silver.py", run_date="{{ ds }}"),
    )

    clean_roads = SSHOperator(
        task_id='clean_roads',
        ssh_conn_id='windows_spark',
        command=SPARK_SUBMIT.format(script="road_network_processing.py", run_date="{{ ds }}"),
    )

    spatial_processing = SSHOperator(
        task_id='spatial_processing',
        ssh_conn_id='windows_spark',
        command=SPARK_SUBMIT.format(script="spatial_processing.py", run_date="{{ ds }}"),
    )

    [clean_orders, clean_roads] >> spatial_processing
```

### Learning Goals — Phase 2
- Understand what a DataFrame is vs RDD — read "Spark: The Definitive Guide" ch.1–4 (free preview)
- Understand CRS (Coordinate Reference Systems) — **EPSG:4326 vs EPSG:32636** for Egypt
- Understand why `ST_Distance` needs a metric projection to return meters
- Practice each Sedona function in isolation in a Jupyter notebook before putting them in jobs

---

## Phase 3 — Route Optimization (Week 8–10)

**Goal:** Implement a real Vehicle Routing Problem solver using OR-Tools. This phase is the algorithmic heart of the project.

### What is VRP?

The Vehicle Routing Problem asks: given N delivery locations and M drivers (vehicles), find the optimal set of routes that minimizes total distance while respecting constraints (capacity, time windows, max route duration).

This is NP-hard — OR-Tools uses constraint programming and local search heuristics, not brute force.

### Week 8 — VRP Fundamentals

Start simple: no time windows, no capacity. Just minimize distance.

```python
# optimization/vrp_solver.py
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
import math
import json

def haversine_meters(lat1, lon1, lat2, lon2):
    """Compute great-circle distance in meters between two coordinates."""
    R = 6_371_000  # Earth radius in meters
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2 * R * math.asin(math.sqrt(a))

def build_distance_matrix(locations):
    """
    locations: list of (lat, lon) tuples
    Returns NxN matrix of integer distances in meters
    """
    n = len(locations)
    matrix = [[0] * n for _ in range(n)]
    for i in range(n):
        for j in range(n):
            if i != j:
                matrix[i][j] = int(haversine_meters(
                    locations[i][0], locations[i][1],
                    locations[j][0], locations[j][1]
                ))
    return matrix

def solve_vrp(orders, drivers, max_route_distance_m=50_000):
    """
    orders:  list of dicts with lat, lon, order_id
    drivers: list of dicts with lat, lon, driver_id, capacity_kg
    Returns: list of routes (one per driver)
    """
    # Node 0 is the depot (city center / warehouse)
    depot_lat, depot_lon = 30.0444, 31.2357  # Cairo center
    depot = {"lat": depot_lat, "lon": depot_lon, "order_id": "DEPOT"}

    # All locations: depot first, then orders
    all_nodes = [depot] + orders
    locations  = [(n["lat"], n["lon"]) for n in all_nodes]
    n_nodes    = len(locations)
    n_vehicles = len(drivers)

    distance_matrix = build_distance_matrix(locations)

    # OR-Tools setup
    manager = pywrapcp.RoutingIndexManager(
        n_nodes,       # number of locations
        n_vehicles,    # number of drivers
        0              # depot index (always 0)
    )
    routing = pywrapcp.RoutingModel(manager)

    # Distance callback
    def distance_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node   = manager.IndexToNode(to_index)
        return distance_matrix[from_node][to_node]

    transit_callback_index = routing.RegisterTransitCallback(distance_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

    # Add distance constraint per vehicle
    dimension_name = "Distance"
    routing.AddDimension(
        transit_callback_index,
        0,                       # no slack
        max_route_distance_m,    # max distance per vehicle
        True,                    # start cumul at zero
        dimension_name,
    )
    distance_dimension = routing.GetDimensionOrDie(dimension_name)
    distance_dimension.SetGlobalSpanCostCoefficient(100)

    # Search parameters
    search_params = pywrapcp.DefaultRoutingSearchParameters()
    search_params.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    )
    search_params.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    )
    search_params.time_limit.seconds = 30  # max optimization time

    solution = routing.SolveWithParameters(search_params)

    if not solution:
        raise RuntimeError("No VRP solution found — check distance constraints")

    # Extract routes
    routes = []
    total_distance = 0

    for vehicle_id in range(n_vehicles):
        driver  = drivers[vehicle_id]
        index   = routing.Start(vehicle_id)
        route   = {"driver_id": driver["driver_id"], "stops": [], "total_distance_m": 0}

        while not routing.IsEnd(index):
            node_index = manager.IndexToNode(index)
            if node_index != 0:  # skip depot
                route["stops"].append({
                    "order_id": all_nodes[node_index]["order_id"],
                    "lat":      all_nodes[node_index]["lat"],
                    "lon":      all_nodes[node_index]["lon"],
                    "sequence": len(route["stops"]) + 1,
                })
            previous_index = index
            index = solution.Value(routing.NextVar(index))
            route["total_distance_m"] += routing.GetArcCostForVehicle(
                previous_index, index, vehicle_id
            )

        if route["stops"]:  # only include drivers with deliveries
            route["total_distance_km"] = round(route["total_distance_m"] / 1000, 2)
            routes.append(route)
            total_distance += route["total_distance_m"]

    print(f"Solved: {len(routes)} routes, {total_distance/1000:.1f} km total")
    return routes
```

### Week 9 — Add Capacity and Time Windows

```python
# optimization/vrp_constrained.py
# Extends vrp_solver.py with capacity and time window constraints

def solve_vrp_with_constraints(orders, drivers, depot_lat=30.0444, depot_lon=31.2357):
    depot = {"lat": depot_lat, "lon": depot_lon, "weight_kg": 0,
             "window_start_min": 0, "window_end_min": 24*60}

    all_nodes  = [depot] + orders
    locations  = [(n["lat"], n["lon"]) for n in all_nodes]
    demands    = [0] + [int(o.get("weight_kg", 5) * 10) for o in orders]  # decagrams
    capacities = [int(d["capacity_kg"] * 10) for d in drivers]

    # Time windows (minutes from midnight)
    def parse_time(t):
        h, m = map(int, t.split(":"))
        return h * 60 + m

    time_windows = [(0, 24*60)]  # depot: all day
    for o in orders:
        start = parse_time(o.get("delivery_window_start", "09:00"))
        end   = parse_time(o.get("delivery_window_end",   "21:00"))
        time_windows.append((start, end))

    manager = pywrapcp.RoutingIndexManager(len(locations), len(drivers), 0)
    routing = pywrapcp.RoutingModel(manager)

    # Distance/time callback (assume 40 km/h avg in Cairo)
    distance_matrix = build_distance_matrix(locations)
    avg_speed_m_per_min = (40 * 1000) / 60

    def time_callback(from_idx, to_idx):
        from_node = manager.IndexToNode(from_idx)
        to_node   = manager.IndexToNode(to_idx)
        travel_m  = distance_matrix[from_node][to_node]
        service_min = 5  # 5 minutes per delivery stop
        return int(travel_m / avg_speed_m_per_min) + service_min

    transit_idx = routing.RegisterTransitCallback(time_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_idx)

    # Capacity constraint
    def demand_callback(from_idx):
        return demands[manager.IndexToNode(from_idx)]

    demand_idx = routing.RegisterUnaryTransitCallback(demand_callback)
    routing.AddDimensionWithVehicleCapacity(
        demand_idx, 0, capacities, True, "Capacity"
    )

    # Time window constraint
    routing.AddDimension(transit_idx, 30, 24*60, False, "Time")
    time_dim = routing.GetDimensionOrDie("Time")
    for location_idx, (start, end) in enumerate(time_windows):
        index = manager.NodeToIndex(location_idx)
        time_dim.CumulVar(index).SetRange(start, end)

    # Penalize dropped orders (soft constraint — solver can skip if impossible)
    penalty = 100_000
    for node in range(1, len(all_nodes)):
        routing.AddDisjunction([manager.NodeToIndex(node)], penalty)

    search_params = pywrapcp.DefaultRoutingSearchParameters()
    search_params.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    )
    search_params.time_limit.seconds = 60

    return routing.SolveWithParameters(search_params)
```

### Week 10 — Wire Optimizer into Airflow

```python
# airflow/dags/optimization_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def run_optimization(**context):
    from google.cloud import storage
    import json, sys
    sys.path.insert(0, '/path/to/project')
    from optimization.vrp_constrained import solve_vrp_with_constraints

    run_date = context['ds']
    client   = storage.Client()
    bucket   = client.bucket("delivery-data-lake-yourname")

    # Load gold layer data
    orders_blob  = bucket.blob(f"gold/orders_spatial/{run_date}/part-00000.parquet")
    drivers_blob = bucket.blob(f"bronze/drivers/{run_date}/drivers.json")

    # (In practice, read parquet with pandas or pyarrow)
    import pandas as pd, io
    orders_df  = pd.read_parquet(io.BytesIO(orders_blob.download_as_bytes()))
    drivers    = json.loads(drivers_blob.download_as_text())

    orders = orders_df.to_dict("records")
    routes = solve_vrp_with_constraints(orders, drivers)

    # Save results
    result_blob = bucket.blob(f"gold/routes/{run_date}/routes.json")
    result_blob.upload_from_string(json.dumps(routes))
    print(f"Saved {len(routes)} routes to GCS")

with DAG(
    dag_id='route_optimization',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['gold', 'optimization'],
) as dag:

    optimize = PythonOperator(
        task_id='run_vrp_solver',
        python_callable=run_optimization,
    )
```

### Learning Goals — Phase 3
- Read the OR-Tools VRP documentation: https://developers.google.com/optimization/routing
- Understand why VRP is NP-hard and what heuristics mean (guided local search, simulated annealing)
- Understand the difference between hard constraints (capacity, must satisfy) and soft constraints (time windows, penalized if violated)
- Experiment: remove time limit and see how solution quality improves

---

## Phase 4 — PostGIS & Storage (Week 11–12)

**Goal:** Load optimized routes and spatial analytics into PostGIS for efficient querying. Learn spatial SQL.

### Schema Design

```sql
-- Run on Mac (psql delivery_platform)

-- Coordinate Reference System note:
-- All geometries stored in EPSG:4326 (WGS84 — standard GPS coordinates)
-- Spatial indexes in GIST for fast queries

CREATE TABLE drivers (
    driver_id        VARCHAR(20) PRIMARY KEY,
    location         GEOGRAPHY(POINT, 4326) NOT NULL,
    home_district    VARCHAR(50),
    capacity_kg      NUMERIC(6,2),
    status           VARCHAR(20) DEFAULT 'available',
    last_updated_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_drivers_location ON drivers USING GIST(location);
CREATE INDEX idx_drivers_status   ON drivers(status);

CREATE TABLE orders (
    order_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    location         GEOGRAPHY(POINT, 4326) NOT NULL,
    district         VARCHAR(50),
    priority         VARCHAR(10) CHECK (priority IN ('high','medium','low')),
    weight_kg        NUMERIC(6,2),
    delivery_window  TSTZRANGE,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    run_date         DATE NOT NULL
);

CREATE INDEX idx_orders_location ON orders USING GIST(location);
CREATE INDEX idx_orders_run_date ON orders(run_date);
CREATE INDEX idx_orders_priority ON orders(priority);

CREATE TABLE routes (
    route_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    driver_id        VARCHAR(20) REFERENCES drivers(driver_id),
    route_geometry   GEOGRAPHY(LINESTRING, 4326),
    stop_sequence    JSONB,          -- ordered array of {order_id, lat, lon, sequence}
    total_distance_m NUMERIC(10,2),
    total_duration_m NUMERIC(10,2),
    run_date         DATE NOT NULL,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_routes_driver   ON routes(driver_id);
CREATE INDEX idx_routes_run_date ON routes(run_date);
CREATE INDEX idx_routes_geometry ON routes USING GIST(route_geometry);

-- Materialized view for KPI dashboard
CREATE MATERIALIZED VIEW daily_kpis AS
SELECT
    run_date,
    COUNT(DISTINCT o.order_id)          AS total_orders,
    COUNT(DISTINCT r.driver_id)         AS active_drivers,
    ROUND(AVG(r.total_distance_m)/1000, 2) AS avg_route_km,
    ROUND(SUM(r.total_distance_m)/1000, 2) AS total_km_driven,
    ROUND(COUNT(DISTINCT o.order_id)::numeric /
          NULLIF(COUNT(DISTINCT r.driver_id),0), 1) AS avg_orders_per_driver
FROM orders o
LEFT JOIN routes r ON r.run_date = o.run_date
GROUP BY run_date
ORDER BY run_date DESC;

CREATE UNIQUE INDEX ON daily_kpis(run_date);
```

### PostGIS Spatial Query Learning Exercises

```sql
-- EXERCISE 1: Find all orders within 5km of Cairo's city center
SELECT
    order_id,
    district,
    ST_Distance(location, ST_GeogFromText('POINT(31.2357 30.0444)')) / 1000 AS km_from_center
FROM orders
WHERE ST_DWithin(
    location,
    ST_GeogFromText('POINT(31.2357 30.0444)'),
    5000  -- meters
)
ORDER BY km_from_center;

-- EXERCISE 2: Nearest available driver to a given order
SELECT
    d.driver_id,
    ST_Distance(d.location, o.location) / 1000 AS distance_km
FROM drivers d, orders o
WHERE o.order_id = 'your-order-id'
  AND d.status = 'available'
ORDER BY d.location <-> o.location  -- uses GiST index — fast!
LIMIT 5;

-- EXERCISE 3: Delivery density heatmap by 500m grid
SELECT
    ST_SnapToGrid(ST_Centroid(location::geometry), 0.005) AS grid_cell,
    COUNT(*) AS delivery_count
FROM orders
WHERE run_date = '2024-01-15'
GROUP BY grid_cell
ORDER BY delivery_count DESC;

-- EXERCISE 4: Which routes pass within 2km of a given hotspot?
SELECT r.route_id, r.driver_id
FROM routes r
WHERE ST_DWithin(
    r.route_geometry,
    ST_GeogFromText('POINT(31.24 30.06)'),  -- Dokki hotspot
    2000
);
```

### Python PostGIS Loader
```python
# storage/postgis_loader.py
import psycopg2
import json
from google.cloud import storage

DB_CONFIG = {
    "dbname":   "delivery_platform",
    "user":     "postgres",
    "password": "your_password",
    "host":     "localhost",
    "port":     5432,
}

def load_routes(run_date):
    gcs = storage.Client()
    bucket = gcs.bucket("delivery-data-lake-yourname")
    blob   = bucket.blob(f"gold/routes/{run_date}/routes.json")
    routes = json.loads(blob.download_as_text())

    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()

    for route in routes:
        # Build WKT LineString from stop sequence
        coords = " ".join(
            f"{s['lon']} {s['lat']}" for s in route["stops"]
        )
        linestring_wkt = f"LINESTRING({coords})" if len(route["stops"]) > 1 else None

        cur.execute("""
            INSERT INTO routes (driver_id, route_geometry, stop_sequence,
                                total_distance_m, run_date)
            VALUES (%s,
                    ST_GeogFromText(%s),
                    %s::jsonb,
                    %s,
                    %s)
            ON CONFLICT DO NOTHING
        """, (
            route["driver_id"],
            linestring_wkt,
            json.dumps(route["stops"]),
            route["total_distance_m"],
            run_date,
        ))

    # Refresh KPI materialized view
    cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY daily_kpis;")

    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {len(routes)} routes for {run_date}")
```

### Learning Goals — Phase 4
- Understand the difference between `GEOMETRY` and `GEOGRAPHY` in PostGIS
  - `GEOMETRY`: flat-earth calculations, fast, in projected units (meters if right CRS)
  - `GEOGRAPHY`: spherical calculations, slightly slower, always in meters, better for large areas
- Understand GIST indexes — why `<->` operator uses an index but `ST_Distance(...) < X` in WHERE does not always
- Practice all 4 SQL exercises above, then modify them

---

## Phase 5 — Dashboard & Final Integration (Week 13–14)

**Goal:** Build the Streamlit + Folium dashboard. Wire the full end-to-end pipeline. Deploy to Render.com.

### Dashboard Structure

```
dashboard/
  app.py              # Main Streamlit entry point
  pages/
    01_overview.py    # KPI cards + daily summary
    02_routes_map.py  # Interactive Folium route map
    03_hotspots.py    # Delivery density heatmap
    04_drivers.py     # Per-driver performance
  db.py               # PostGIS query helpers
  gcs.py              # GCS data loader
```

### Main Dashboard App
```python
# dashboard/app.py
import streamlit as st
import folium
from streamlit_folium import st_folium
import psycopg2
import pandas as pd

st.set_page_config(
    page_title="Delivery Route Optimizer",
    page_icon="🗺️",
    layout="wide",
)

st.title("Smart Delivery Route Optimization Platform")
st.caption("Cairo — Powered by Spark · Sedona · OR-Tools · PostGIS")

# --- KPI Cards ---
conn = psycopg2.connect(dbname="delivery_platform", user="postgres",
                        password="your_password", host="localhost")

kpis = pd.read_sql("SELECT * FROM daily_kpis ORDER BY run_date DESC LIMIT 1", conn)

if not kpis.empty:
    row = kpis.iloc[0]
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Orders",         int(row["total_orders"]))
    col2.metric("Active Drivers",       int(row["active_drivers"]))
    col3.metric("Avg Route (km)",       f"{row['avg_route_km']:.1f}")
    col4.metric("Total km Driven",      f"{row['total_km_driven']:.0f}")
    col5.metric("Orders per Driver",    f"{row['avg_orders_per_driver']:.1f}")

# --- Route Map ---
st.subheader("Optimized Routes")
run_date = st.date_input("Select date")

routes_df = pd.read_sql(
    "SELECT driver_id, stop_sequence, total_distance_m FROM routes WHERE run_date = %s",
    conn, params=[str(run_date)]
)

# Cairo center
m = folium.Map(location=[30.0444, 31.2357], zoom_start=11, tiles="CartoDB positron")

colors = ["red","blue","green","purple","orange","darkred","lightred","beige",
          "darkblue","darkgreen","cadetblue","darkpurple","white","pink","lightblue",
          "lightgreen","gray","black","lightgray"]

for idx, row in routes_df.iterrows():
    stops   = row["stop_sequence"]
    color   = colors[idx % len(colors)]
    coords  = [[s["lat"], s["lon"]] for s in stops]
    if len(coords) >= 2:
        folium.PolyLine(coords, color=color, weight=3, opacity=0.8,
                        tooltip=f"Driver: {row['driver_id']} | {row['total_distance_m']/1000:.1f} km").add_to(m)
    for i, s in enumerate(stops):
        folium.CircleMarker(
            [s["lat"], s["lon"]],
            radius=6, color=color, fill=True,
            popup=f"Stop {i+1} — Order {s['order_id'][:8]}"
        ).add_to(m)

st_folium(m, width=None, height=550)
conn.close()
```

### Full End-to-End Master DAG
```python
# airflow/dags/master_pipeline_dag.py
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id='master_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['master'],
) as dag:

    ingest = TriggerDagRunOperator(
        task_id='trigger_ingestion',
        trigger_dag_id='ingestion_pipeline',
        wait_for_completion=True,
        conf={"run_date": "{{ ds }}"},
    )

    process = TriggerDagRunOperator(
        task_id='trigger_spark_processing',
        trigger_dag_id='spark_processing',
        wait_for_completion=True,
        conf={"run_date": "{{ ds }}"},
    )

    optimize = TriggerDagRunOperator(
        task_id='trigger_route_optimization',
        trigger_dag_id='route_optimization',
        wait_for_completion=True,
        conf={"run_date": "{{ ds }}"},
    )

    ingest >> process >> optimize
```

---

## Data Governance & Quality Gates

These are not optional. Apply them from Week 3 onward.

### CRS Contract
All spatial data in this project uses **EPSG:4326 (WGS84)**. Any projection for distance calculations uses **EPSG:32636 (UTM Zone 36N)** for the Cairo area. Document this in every Spark job file header.

### Data Quality Rules (enforced in Airflow)

| Layer | Check | Threshold | Action on failure |
|---|---|---|---|
| Bronze | Road elements count > 500 | Hard | Fail DAG |
| Bronze | Order count = expected | Hard | Fail DAG |
| Silver | Null lat/lon rate < 1% | Hard | Fail DAG |
| Silver | Coordinate bounds within Cairo bbox | Hard | Fail DAG |
| Silver | Drop rate < 5% vs Bronze | Hard | Fail DAG |
| Gold | All orders assigned to at least 1 driver | Soft | Log warning |
| Gold | Route distance < 200 km per driver | Soft | Log warning |

### PII Handling
Simulated data has no real PII, but treat it as if it does to learn correct habits:

```python
# Apply in Silver layer transformation
import hashlib

def pseudonymize_order_id(order_id: str, salt: str = "delivery_salt_2024") -> str:
    """Replace real order ID with deterministic hash for analytics layers."""
    return hashlib.sha256(f"{salt}:{order_id}".encode()).hexdigest()[:16]
```

---

## Testing Strategy

Every phase adds tests. Run them locally and via GitHub Actions.

```
tests/
  test_ingestion.py       # Overpass client, retry logic, schema validation
  test_simulator.py       # Data simulator output shape and value ranges
  test_vrp_solver.py      # VRP solver returns valid routes
  test_spatial.py         # Coordinate validation, distance calculations
  test_postgis_loader.py  # DB insert/query round-trips (uses test DB)
  test_dag_integrity.py   # Airflow DAG imports without errors
```

### Example Tests
```python
# tests/test_vrp_solver.py
import pytest
from optimization.vrp_solver import solve_vrp, build_distance_matrix

SAMPLE_ORDERS = [
    {"order_id": "O1", "lat": 30.05, "lon": 31.25},
    {"order_id": "O2", "lat": 30.06, "lon": 31.22},
    {"order_id": "O3", "lat": 30.04, "lon": 31.28},
]
SAMPLE_DRIVERS = [
    {"driver_id": "D1", "lat": 30.04, "lon": 31.23, "capacity_kg": 30},
    {"driver_id": "D2", "lat": 30.07, "lon": 31.27, "capacity_kg": 30},
]

def test_vrp_returns_routes():
    routes = solve_vrp(SAMPLE_ORDERS, SAMPLE_DRIVERS)
    assert len(routes) > 0

def test_all_orders_assigned():
    routes = solve_vrp(SAMPLE_ORDERS, SAMPLE_DRIVERS)
    assigned = {s["order_id"] for r in routes for s in r["stops"]}
    expected = {o["order_id"] for o in SAMPLE_ORDERS}
    assert assigned == expected

def test_route_distance_positive():
    routes = solve_vrp(SAMPLE_ORDERS, SAMPLE_DRIVERS)
    for r in routes:
        assert r["total_distance_m"] > 0

def test_distance_matrix_symmetry():
    locs = [(30.0, 31.0), (30.1, 31.1), (30.2, 31.2)]
    matrix = build_distance_matrix(locs)
    for i in range(len(locs)):
        for j in range(len(locs)):
            assert abs(matrix[i][j] - matrix[j][i]) < 1  # within 1 meter

# tests/test_dag_integrity.py
def test_all_dags_load():
    """Ensures DAG files parse without import errors."""
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder='airflow/dags', include_examples=False)
    assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"
```

---

## Learning Checkpoints

Use these to confirm you actually understand each phase before moving on. If you cannot answer these without looking at your code, go back.

### After Phase 0
- [ ] Explain what Airflow's scheduler does vs the webserver
- [ ] Explain what Docker Compose `depends_on` does
- [ ] Explain why Spark needs more RAM than your Python scripts

### After Phase 1
- [ ] Explain the difference between Bronze, Silver, and Gold layers in one sentence each
- [ ] Write an Overpass query for just traffic lights in Cairo from memory
- [ ] Explain what GCS object paths are and why you partition by date

### After Phase 2
- [ ] Explain why `ST_Distance` on `EPSG:4326` returns degrees, not meters
- [ ] Explain what a spatial join is and give an example from your code
- [ ] Explain what Kryo serialization is and why Sedona needs it
- [ ] Explain the difference between a Spark transformation and an action

### After Phase 3
- [ ] Explain in plain English what makes VRP NP-hard
- [ ] Explain the difference between a hard constraint and a soft constraint in OR-Tools
- [ ] Explain what `PATH_CHEAPEST_ARC` does as a first solution strategy
- [ ] Change the time limit to 120 seconds and observe the solution quality difference

### After Phase 4
- [ ] Explain the difference between `GEOMETRY` and `GEOGRAPHY` types in PostGIS
- [ ] Explain why the `<->` operator in PostGIS is faster than `ST_Distance(...) < X`
- [ ] Write the nearest-driver query from memory and explain the index it uses

### After Phase 5
- [ ] Explain what a Materialized View is and when you'd refresh it
- [ ] Walk someone through the full pipeline end-to-end in 3 minutes

---

## Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| OSM data quality gaps in Cairo suburbs | High | Medium | Stick to inner-city bounding box (Maadi to Heliopolis); document coverage gaps |
| Mac 8GB memory pressure from PostGIS + Airflow | Medium | High | Set `work_mem=32MB` and `shared_buffers=256MB` in PostgreSQL config; restart Mac services if needed |
| SSH connection drops between Mac and Windows | Medium | Medium | Add `ServerAliveInterval 60` to Mac `~/.ssh/config`; Airflow retry=2 on SSH tasks |
| GCS free tier quota (5GB) exceeded | Low | Low | Parquet compression reduces road data to ~50MB; orders ~10MB/day; well within limits |
| OR-Tools finds no solution for constrained VRP | Medium | High | Start with relaxed constraints; add 30-min time window slack before tightening |
| Sedona CRS mismatch causing wrong distances | High | High | Unit test every spatial function with known distances; assert distances on a reference route |
| Windows laptop unavailable | Low | High | Write all Spark jobs to also run locally on Mac with `local[*]` master on small data subset |

---

## Folder Structure

```
delivery-platform/
│
├── .github/
│   └── workflows/
│       └── test.yml
│
├── airflow/
│   ├── dags/
│   │   ├── ingestion_dag.py
│   │   ├── spark_processing_dag.py
│   │   ├── optimization_dag.py
│   │   ├── master_pipeline_dag.py
│   │   └── hello_world_dag.py
│   └── plugins/
│
├── ingestion/
│   ├── overpass_client.py
│   ├── data_simulator.py
│   ├── bronze_writer.py
│   └── schemas.py
│
├── jobs/                          # Spark jobs (synced to Windows)
│   ├── bronze_to_silver.py
│   ├── road_network_processing.py
│   └── spatial_processing.py
│
├── optimization/
│   ├── vrp_solver.py
│   └── vrp_constrained.py
│
├── storage/
│   └── postgis_loader.py
│
├── dashboard/
│   ├── app.py
│   ├── pages/
│   │   ├── 01_overview.py
│   │   ├── 02_routes_map.py
│   │   ├── 03_hotspots.py
│   │   └── 04_drivers.py
│   ├── db.py
│   └── gcs.py
│
├── tests/
│   ├── test_ingestion.py
│   ├── test_simulator.py
│   ├── test_vrp_solver.py
│   ├── test_spatial.py
│   ├── test_postgis_loader.py
│   └── test_dag_integrity.py
│
├── sql/
│   └── schema.sql
│
├── docker-compose.yml             # Lives on Windows, synced via Git
├── requirements.txt               # Mac dependencies
├── requirements-test.txt          # CI/testing dependencies
├── .env.example                   # Template (never commit real .env)
├── .gitignore
└── README.md
```

### .gitignore
```
.env
venv/
__pycache__/
*.pyc
airflow/logs/
airflow/db/
conf/gcs-key.json          # NEVER commit service account key
data/
*.parquet
.DS_Store
```

---

## Weekly Summary Timeline

| Week | Focus | Machine | Deliverable |
|---|---|---|---|
| 1 | Mac env, Airflow, PostGIS setup | Mac | Airflow running, PostGIS responding |
| 2 | Windows Spark cluster, SSH bridge, GCS | Both | End-to-end "Hello World" pipeline |
| 3 | Overpass client, data simulator | Mac | Bronze layer in GCS |
| 4 | Ingestion DAG, validation, tests | Mac | Passing ingestion DAG + tests |
| 5 | Spark basics, Bronze→Silver cleaning | Windows | Silver Parquet in GCS |
| 6 | Apache Sedona spatial operations | Windows | Spatial Gold layer in GCS |
| 7 | Road network processing, full Spark DAG | Both | Complete Spark DAG in Airflow |
| 8 | VRP basics, haversine matrix, OR-Tools | Mac | Simple VRP running on sample data |
| 9 | Capacity + time window constraints | Mac | Constrained VRP with real data |
| 10 | Optimization DAG, GCS output | Mac | Routes in GCS Gold layer |
| 11 | PostGIS schema, spatial queries | Mac | Routes loaded, queries working |
| 12 | PostGIS loader, materialized views | Mac | KPI view populated |
| 13 | Streamlit dashboard, route maps | Mac | Dashboard running locally |
| 14 | Full integration, Render deploy, docs | Both | Live demo URL + README |

---

*Built solo. Learned deeply. Production-minded from day one.*
