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
│  ┌─────────────┐    ┌──────────────┐    ┌───────────────────────┐   │
│  │   Airflow   │───▶│ OSM Ingestion│───▶│  GCS Bronze Layer     │   │
│  │ (scheduler) │    │   (Python)   │    │  raw JSON / OSM data  │   │
│  └──────┬──────┘    └──────────────┘    └───────────────────────┘   │
│         │ SSHOperator                                               │
│         │                                        ▲                  │
│  ┌──────▼──────┐    ┌──────────────┐    ┌────────┴──────────────┐   │
│  │  OR-Tools   │◀───│  GCS Gold    │    │   GCS Silver Layer    │   │
│  │ (optimizer) │    │   Layer      │    │   cleaned Parquet     │   │
│  └──────┬──────┘    └──────────────┘    └───────────────────────┘   │
│         │                                        ▲                  │
│  ┌──────▼──────┐    ┌──────────────┐             │                  │
│  │   PostGIS   │    │  Streamlit   │             │ SSH / submit     │
│  │ (results)   │    │  Dashboard   │             │                  │
│  └─────────────┘    └──────────────┘             │                  │
└──────────────────────────────────────────────────┼──────────────────┘
                                                   │
┌──────────────────────────────────────────────────▼──────────────────┐
│                      WINDOWS LAPTOP (16GB)                          │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                    Docker Compose                            │   │
│  │  ┌─────────────────┐      ┌──────────────────────────────┐   │   │
│  │  │  Spark Master   │      │       Spark Worker           │   │   │
│  │  │  (2GB)          │      │  + Apache Sedona (6GB)       │   │   │
│  │  └────────┬────────┘      └──────────────────────────────┘   │   │
│  └───────────┼──────────────────────────────────────────────────┘   │
│              │ reads/writes GCS via service account                 │
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
    pydantic \
    gcsfs \
    pyarrow \
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

# Initialize the DB using uv run
#### Initialize Airflow
```bash
uv run airflow db init
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

```bash
# Create the user
uv run airflow users create \
  --username admin \
  --password admin \
  --firstname Solo \
  --lastname Dev \
  --role Admin \
  --email admin@local.com


export AIRFLOW_HOME=~/projects/delivery-platform/airflow
export PYTHONPATH="${AIRFLOW_HOME}:$PYTHONPATH"

kill -9 $(lsof -i :8080)
kill -9 $(lsof -i :8793)



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
    dag_id='test_ssh_connection',  # Updated to reflect the test purpose
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
  __init__.py
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
# ingestion/overpass_client.py — fixed and improved
import requests
import time
import logging
from config import CAIRO_BBOX

logger = logging.getLogger(__name__)

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

ROAD_QUERY = """
[out:json][timeout:300][bbox:{south},{west},{north},{east}];
(
    way["highway"~"motorway|trunk|primary|secondary|residential|service"];
);
out body geom;
""".strip()


def fetch_roads(
    bbox: tuple = CAIRO_BBOX,
    max_retries: int = 3,
    backoff_seconds: float = 10,
) -> dict:
    """Fetch road network from Overpass API with exponential backoff."""
    south, west, north, east = bbox
    query = ROAD_QUERY.format(south=south, west=west, north=north, east=east)

    for attempt in range(1, max_retries + 1):
        try:
            logger.info("Fetching roads, attempt %d/%d", attempt, max_retries)
            response = requests.post(
                OVERPASS_URL,
                data={"data": query},
                headers={
                    "User-Agent": "Smart-Delivery-Route-Optimization-Platform/1.0"
                },
                timeout=350,
            )
            response.raise_for_status()
            data = response.json()
            n_elements = len(data.get("elements", []))
            logger.info("Fetched %d road elements", n_elements)
            return data

        except requests.exceptions.HTTPError as e:
            if response.status_code != 429:
                raise
            wait = backoff_seconds * (2 ** attempt)
            logger.warning("Rate limited (429). Waiting %.0fs before retry.", wait)
            time.sleep(wait)

        except requests.exceptions.Timeout:
            logger.warning("Timeout on attempt %d/%d", attempt, max_retries)
            if attempt == max_retries:
                raise
            time.sleep(backoff_seconds)

        except requests.exceptions.ConnectionError as e:
            logger.warning("Connection error on attempt %d: %s", attempt, e)
            if attempt == max_retries:
                raise
            time.sleep(backoff_seconds * (2 ** attempt))

    raise RuntimeError(f"Max retries ({max_retries}) exceeded fetching OSM data")
```

### Data Simulator
```python
# ingestion/data_simulator.py
import random
import json
import uuid
from datetime import datetime, timedelta
from config import DEFAULT_ORDER_COUNT , DEFAULT_DRIVER_COUNT
import random
from typing import Optional

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

def simulate_orders(n: int = 500,date=None,seed: Optional[int] = None,) -> list[dict]:
    """Generate simulated delivery orders.
    Args:
        seed: If provided, results are reproducible (for testing).
    """
    if seed is not None:
        random.seed(seed)
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

def simulate_drivers(n=DEFAULT_DRIVER_COUNT):
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

# terminal test
if __name__ == "__main__":
    import json
    import logging

    logging.basicConfig(level=logging.INFO)

    orders = simulate_orders()
    drivers = simulate_drivers()

    print(f"Total orders: {len(orders)}")
    print(f"Total drivers: {len(drivers)}")

    with open("orders_test.json", "w") as f:
        json.dump(orders, f, indent=2)
    with open("drivers_test.json", "w") as f:
        json.dump(drivers, f, indent=2)

    print("Done")
```
### Schema validation
```python
# ingestion/schemas.py
from __future__ import annotations
from typing import Literal
from pydantic import BaseModel, Field, field_validator
from datetime import datetime

# ── Order Contract ──
class Order(BaseModel):
    order_id: str
    lat: float = Field(ge=29.5, le=30.5, description="Latitude within Cairo bbox")
    lon: float = Field(ge=31.0, le=31.8, description="Longitude within Cairo bbox")
    district: str
    priority: Literal["high", "medium", "low"]
    weight_kg: float = Field(gt=0, le=100)
    created_at: datetime
    delivery_window_start: str
    delivery_window_end: str

    @field_validator("delivery_window_start", "delivery_window_end")
    @classmethod
    def validate_time_format(cls, v: str) -> str:
        try:
            datetime.strptime(v, "%H:%M")
        except ValueError:
            raise ValueError(f"Invalid time format: {v}. Expected HH:MM")
        return v

# ── Driver Contract ──
class Driver(BaseModel):
    driver_id: str
    lat: float = Field(ge=29.5, le=30.5)
    lon: float = Field(ge=31.0, le=31.8)
    capacity_kg: float = Field(gt=0)
    status: Literal["available", "busy", "offline"]
    district: str

# ── Bulk validators ──
def validate_orders(orders: list[dict]) -> list[Order]:
    """Validate all orders. Raises ValidationError with details on failure."""
    return [Order(**o) for o in orders]

def validate_drivers(drivers: list[dict]) -> list[Driver]:
    return [Driver(**d) for d in drivers]

def validate_roads(osm_data: dict) -> dict:
    """Lightweight OSM validation — Pydantic is overkill for the full OSM schema."""
    if not isinstance(osm_data, dict):
        raise TypeError("OSM response must be dict")
    if "elements" not in osm_data:
        raise ValueError("Missing 'elements' from OSM response")
    if len(osm_data["elements"]) == 0:
        raise ValueError("No road elements received")
    for road in osm_data["elements"][:20]:  # sample check
        if "id" not in road:
            raise ValueError(f"Road element missing 'id': {road}")
    return osm_data
```
### Airflow Ingestion DAG
```python
# airflow/dags/ingestion_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import logging


# Default args with retry logic
default_args = {
    "owner": "delivery-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

def ingest_roads(**context):
    from ingestion.overpass_client import fetch_roads
    from ingestion.bronze_writer import upload_to_bronze
    
    data = fetch_roads()
    upload_to_bronze(
        data,
        f"roads/{context['ds']}/roads.json",
        metadata={"source": "overpass_api", "record_count": len(data.get("elements", []))}
    )

def ingest_orders(**context):
    from ingestion.data_simulator import simulate_orders
    from ingestion.bronze_writer import upload_to_bronze
    
    orders = simulate_orders(n=500)
    upload_to_bronze(
        orders,
        f"orders/{context['ds']}/orders.json",
        metadata={"source": "simulator", "record_count": len(orders)}
    )

def ingest_drivers(**context):
    from ingestion.data_simulator import simulate_drivers
    from ingestion.bronze_writer import upload_to_bronze
    
    drivers = simulate_drivers(n=25)
    upload_to_bronze(
        drivers,
        f"drivers/{context['ds']}/drivers.json",
        metadata={"source": "simulator", "record_count": len(drivers)}
    )

def validate_bronze(**context):
    from ingestion.bronze_writer import count_bronze_records
    from config import MIN_ROAD_ELEMENTS, DEFAULT_ORDER_COUNT
    
    roads = count_bronze_records(f"roads/{context['ds']}/roads.json")
    orders = count_bronze_records(f"orders/{context['ds']}/orders.json")
    
    assert roads > MIN_ROAD_ELEMENTS, f"Too few road elements: {roads} (min: {MIN_ROAD_ELEMENTS})"
    assert orders == DEFAULT_ORDER_COUNT, f"Expected {DEFAULT_ORDER_COUNT} orders, got {orders}"
    
    logging.info("Validation passed: %d roads, %d orders", roads, orders)

with DAG(
    dag_id="ingestion_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["bronze", "ingestion"],
    doc_md="""## Ingestion Pipeline
    Fetches OSM road data and generates simulated delivery orders/drivers.
    Uploads all data to the GCS Bronze layer.
    """,
) as dag:

    start = EmptyOperator(task_id="start")
    
    t_roads = PythonOperator(task_id="ingest_roads", python_callable=ingest_roads)
    t_orders = PythonOperator(task_id="ingest_orders", python_callable=ingest_orders)
    t_drivers = PythonOperator(task_id="ingest_drivers", python_callable=ingest_drivers)
    t_validate = PythonOperator(task_id="validate_bronze", python_callable=validate_bronze)
    end = EmptyOperator(task_id="end")

    start >> [t_roads, t_orders, t_drivers] >> t_validate >> end
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
import sys
import logging
from spark_utils import create_spark_session
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from config import GCS_BUCKET_NAME

logger = logging.getLogger(__name__)


def clean_orders(spark, run_date: str) -> None:
    raw_path = f"gs://{GCS_BUCKET_NAME}/bronze/orders/{run_date}/"
    df = spark.read.option("multiline", "true").json(raw_path)

    cleaned = (
        df
        .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
        .filter((F.col("lat").between(29.5, 30.5)) & (F.col("lon").between(31.0, 31.8)))
        .filter(F.col("order_id").isNotNull())
        .withColumn("lat", F.col("lat").cast(DoubleType()))
        .withColumn("lon", F.col("lon").cast(DoubleType()))
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("date_partition", F.lit(run_date))
        .dropDuplicates(["order_id"])
    )

    total = df.count()
    valid = cleaned.count()
    drop_rate = round((total - valid) / total * 100, 2) if total > 0 else 0
    logger.info("Orders: %d raw → %d clean (%.2f%% dropped)", total, valid, drop_rate)

    if drop_rate > 5.0:
        raise ValueError(f"Drop rate {drop_rate}% exceeds 5% threshold")

    silver_path = f"gs://{GCS_BUCKET}/silver/orders/{run_date}/"
    cleaned.write.mode("overwrite").parquet(silver_path)
    logger.info("Written to %s", silver_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_date = sys.argv[1]
    spark = create_spark_session("BronzeToSilver")
    try:
        clean_orders(spark, run_date)
    finally:
        spark.stop()
```
#### Test it inside docker container
```bash
docker exec -u 0 -it delivery-platform-spark-master-1 bash

spark-submit --master spark://spark-master:7077 --jars /opt/bitnami/spark/custom_jars/sedona-spark-shaded.jar,/opt/bitnami/spark/custom_jars/geotools-wrapper.jar,/opt/bitnami/spark/custom_jars/gcs-connector.jar /opt/bitnami/spark/jobs/spatial_processing.py 2026-06-22
```
### Week 6 — Apache Sedona Spatial Operations

This is the heart of the geospatial learning. Take it slow.
#### Install sedona inside docker container
```bash
docker exec --user root delivery-platform-spark-master-1 bash -c "apt-get update && apt-get install -y gcc python3-dev && pip install apache-sedona==1.5.1 shapely"

docker exec --user root delivery-platform-spark-worker-1 bash -c "apt-get update && apt-get install -y gcc python3-dev && pip install apache-sedona==1.5.1 shapely"
```
#### spark & sedona pocessing

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
    spark.sparkContext.setLogLevel("WARN")
    run_spatial_jobs(spark, run_date)
    spark.stop()
```
#### Test it inside docker container
```bash
docker exec -u 0 -it delivery-platform-spark-master-1 bash

spark-submit --master spark://spark-master:7077 --jars /opt/bitnami/spark/custom_jars/sedona-spark-shaded.jar,/opt/bitnami/spark/custom_jars/geotools-wrapper.jar,/opt/bitnami/spark/custom_jars/gcs-connector.jar /opt/bitnami/spark/jobs/spatial_processing.py 2026-06-22
```

### Week 7 — Road Network Processing

```python
# jobs/spatial_processing.py (runs on Windows Spark cluster)
import sys
from pyspark.sql import functions as F
from spark_utils import create_spark_session

def run_spatial_jobs(spark, run_date):
    orders_path = f"gs://delivery-data-lake/silver/orders/{run_date}/"
    orders = spark.read.parquet(orders_path)
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
                    ST_Transform(ST_Centroid(ST_Union_Aggr(geometry)), 'EPSG:4326', 'EPSG:32636'),
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
    gold_path = f"gs://delivery-data-lake/gold/orders_spatial/{run_date}/"
    orders_with_distance.write.mode("overwrite").parquet(gold_path)
    hotspots.write.mode("overwrite").parquet(f"gs://delivery-data-lake/gold/hotspots/{run_date}/")

    print("Spatial processing complete")

if __name__ == "__main__":
    run_date = sys.argv[1]
    # Use the shared utility to create the session, ensuring Sedona is enabled
    spark = create_spark_session(app_name="SpatialProcessing", sedona=True)
    run_spatial_jobs(spark, run_date)
    spark.stop()
```
#### Test it inside docker container

```bash
docker exec -u 0 -it delivery-platform-spark-master-1 bash

spark-submit   --master spark://spark-master:7077   --driver-memory 2G   --executor-memory 8G   --jars /opt/bitnami/spark/custom_jars/sedona-spark-shaded.jar,/opt/bitnami/spark/custom_jars/geotools-wrapper.jar,/opt/bitnami/spark/custom_jars/gcs-connector.jar   /opt/bitnami/spark/jobs/road_network_processing.py 2026-06-22
```
### Airflow DAG for Spark Jobs (on Mac, triggers Windows)
```python
# airflow/dags/spark_processing_dag.py
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

# Explicitly invoke PowerShell and wrap the Docker command in double quotes
SPARK_SUBMIT = (
    'powershell.exe -NonInteractive -NoProfile -Command '
    '"docker exec -u 0 delivery-platform-spark-master-1 spark-submit '
    '--master spark://spark-master:7077 '
    '--driver-memory 2G '
    '--executor-memory 8G '
    '--jars /opt/bitnami/spark/custom_jars/sedona-spark-shaded.jar,/opt/bitnami/spark/custom_jars/geotools-wrapper.jar,/opt/bitnami/spark/custom_jars/gcs-connector.jar '
    '/opt/bitnami/spark/jobs/{script} {run_date}"'
)

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
        cmd_timeout=600, # Good practice to extend timeout for heavy Spark jobs
    )

    clean_roads = SSHOperator(
        task_id='clean_roads',
        ssh_conn_id='windows_spark',
        command=SPARK_SUBMIT.format(script="road_network_processing.py", run_date="{{ ds }}"),
        cmd_timeout=600,
    )

    spatial_processing = SSHOperator(
        task_id='spatial_processing',
        ssh_conn_id='windows_spark',
        command=SPARK_SUBMIT.format(script="spatial_processing.py", run_date="{{ ds }}"),
        cmd_timeout=600,
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
    # ── Input validation ──
    if not orders:
        raise ValueError("No orders provided to VRP solver")
    if not drivers:
        raise ValueError("No drivers provided to VRP solver")
    if len(drivers) > len(orders):
        logger.warning(
            "More drivers (%d) than orders (%d) — some drivers will be idle",
            len(drivers), len(orders)
        )

    for o in orders:
        if not all(k in o for k in ("lat", "lon", "order_id")):
            raise ValueError(f"Order missing required fields: {o}")
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

    solution = routing.SolveWithParameters(search_params)

    if not solution:
        # Check if the penalty for dropping orders was too high, or time windows too strict
        raise RuntimeError("No VRP solution found — constraints are too tight.")

    routes = []
    total_distance_m = 0

    for vehicle_id in range(len(drivers)):
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
            
            # Get real distance from matrix since ArcCost is now calculating time
            prev_node = manager.IndexToNode(previous_index)
            curr_node = manager.IndexToNode(index)
            route["total_distance_m"] += distance_matrix[prev_node][curr_node]

        if route["stops"]:  # only include drivers with deliveries
            route["total_distance_km"] = round(route["total_distance_m"] / 1000, 2)
            routes.append(route)
            total_distance_m += route["total_distance_m"]

    print(f"Solved constrained VRP: {len(routes)} routes, {total_distance_m/1000:.1f} km total")
    return routes
```

### Week 10 — Wire Optimizer into Airflow

```python
# airflow/dags/optimization_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def run_optimization(**context):
    import os, json, sys
    from storage.gcs_client import get_gcs_bucket
    from config import GCS_BUCKET_NAME
    from optimization.vrp_constrained import solve_vrp_with_constraints
    run_date = context['ds']
    bucket = get_gcs_bucket()
    # Load gold layer data
    gcs_orders_path  = f"gs://{GCS_BUCKET_NAME}/gold/orders_spatial/{run_date}/"
    drivers_blob = bucket.blob(f"bronze/drivers/{run_date}/drivers.json")

    # (In practice, read parquet with pandas or pyarrow)
    import pandas as pd, io
    orders_df  = pd.read_parquet(gcs_orders_path)
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
### Spatial SQL Functions Reference

#### 1. Geometry Constructors (Creation)
| Function | Description |
| :--- | :--- |
| `ST_Point(lon, lat)` | Creates a 2D Point geometry from longitude and latitude coordinates. |
| `ST_GeomFromText('WKT')` | Constructs a Geometry object from a Well-Known Text (WKT) string (e.g., `'POINT(31.2 30.0)'`). |
| `ST_GeogFromText('WKT')` | Constructs a Geography object (spherical Earth model) from a WKT string. |
| `ST_MakeLine(geom1, geom2)` | Creates a LineString from Point or LineString geometries. |
| `ST_MakePolygon(linestring)` | Creates a Polygon formed by a closed LineString. |

#### 2. Geometry Outputs (Conversion)
| Function | Description |
| :--- | :--- |
| `ST_AsText(geom)` | Returns the Well-Known Text (WKT) string representation of a geometry. |
| `ST_AsGeoJSON(geom)` | Returns the geometry as a GeoJSON string, useful for web mapping APIs. |
| `ST_AsBinary(geom)` | Returns the Well-Known Binary (WKB) representation of the geometry. |

#### 3. Measurements & Analytics
| Function | Description |
| :--- | :--- |
| `ST_Distance(geom1, geom2)` | Calculates the shortest distance between two geometries. *Note: Units depend on the Spatial Reference System (SRS). Use projected coordinates for meters.* |
| `ST_Area(geom)` | Returns the 2D surface area of a polygonal geometry. |
| `ST_Length(geom)` | Returns the 2D length of a linear geometry (LineString or MultiLineString). |
| `ST_Perimeter(geom)` | Returns the 2D perimeter of a polygonal geometry. |
| `<->` (Operator) | PostGIS-specific spatial operator for index-accelerated Nearest Neighbor (KNN) distance calculations. |

#### 4. Processing & Transformations
| Function | Description |
| :--- | :--- |
| `ST_Transform(geom, to_srid)` | Reprojects a geometry from its current Coordinate Reference System (CRS) to a new one (e.g., EPSG:4326 to EPSG:32636). |
| `ST_Buffer(geom, distance)` | Creates a polygon representing all points whose distance from the geometry is less than or equal to the specified distance. |
| `ST_Centroid(geom)` | Returns the geometric center point of a geometry. |
| `ST_Intersection(geom1, geom2)` | Returns a geometry representing the shared (overlapping) portion of two geometries. |
| `ST_Union(geom1, geom2)` | Combines two geometries into a single geometry, dissolving shared boundaries. |
| `ST_SnapToGrid(geom, size)` | Snaps all vertices of a geometry to a regular grid defined by the given cell size. Useful for data clustering/heatmaps. |

#### 5. Spatial Predicates (Relationships)
*These functions return a boolean (True/False) and are highly optimized for use in `WHERE` clauses with spatial indexes.*

| Function | Description |
| :--- | :--- |
| `ST_Intersects(geom1, geom2)` | Returns TRUE if the two geometries share any portion of space (they overlap or touch). |
| `ST_Contains(geomA, geomB)` | Returns TRUE if no points of geometry B lie in the exterior of geometry A, and at least one point of the interior of B lies in the interior of A. |
| `ST_Within(geomA, geomB)` | Returns TRUE if geometry A is completely inside geometry B (the inverse of `ST_Contains`). |
| `ST_DWithin(geom1, geom2, d)` | Returns TRUE if the geometries are within the specified distance `d` of one another. Much faster than using `ST_Distance(...) <= d`. |
| `ST_Touches(geom1, geom2)` | Returns TRUE if the geometries have at least one point in common, but their interiors do not intersect. |

#### 6. Aggregations (GROUP BY)
| Function | Description |
| :--- | :--- |
| `ST_Collect(geom)` | Aggregates a set of geometries into a `GeometryCollection` or `MultiGeometry`. (In Apache Sedona, this is often a scalar function, not an aggregate). |
| `ST_Union_Aggr(geom)` | Aggregates a set of geometries by dissolving their boundaries into a single continuous geometry (Native to Apache Sedona). |

### Python PostGIS Loader
```python
# storage/postgis_loader.py
import psycopg2
import json
import io
import os
import pandas as pd
from google.cloud import storage
from config import DB_CONFIG, GCS_CREDENTIALS_PATH, GCS_BUCKET_NAME

BATCH_SIZE = 500

def load_drivers(cur, bucket, run_date: str) -> int:
    blob = bucket.blob(f"bronze/drivers/{run_date}/drivers.json")
    if not blob.exists():
        logger.warning("No drivers found for %s", run_date)
        return 0

    drivers = json.loads(blob.download_as_text())

    # ✅ Use execute_values for batch insert
    from psycopg2.extras import execute_values

    rows = [
        (
            d["driver_id"],
            f"POINT({d['lon']} {d['lat']})",
            d.get("district"),
            d.get("capacity_kg"),
            d.get("status", "available"),
        )
        for d in drivers
    ]

    execute_values(
        cur,
        """
        INSERT INTO drivers (driver_id, location, home_district, capacity_kg, status)
        VALUES %s
        ON CONFLICT (driver_id) DO UPDATE
        SET location = EXCLUDED.location,
            status = EXCLUDED.status,
            last_updated_at = NOW()
        """,
        rows,
        template="(%s, ST_GeogFromText(%s), %s, %s, %s)",
        page_size=BATCH_SIZE,
    )
    logger.info("Loaded/updated %d drivers", len(drivers))
    return len(drivers)


def load_orders(cur, bucket, run_date: str) -> int:
    prefix = f"gold/orders_spatial/{run_date}/"
    blobs = list(bucket.list_blobs(prefix=prefix))
    parquet_blobs = [b for b in blobs if b.name.endswith(".parquet")]

    if not parquet_blobs:
        logger.warning("No order parquet files found for %s", run_date)
        return 0

    from psycopg2.extras import execute_values

    total_loaded = 0
    for blob in parquet_blobs:
        orders_df = pd.read_parquet(io.BytesIO(blob.download_as_bytes()))

        rows = [
            (
                o["order_id"],
                f"POINT({o['lon']} {o['lat']})",
                o.get("district"),
                o.get("priority"),
                o.get("weight_kg"),
                run_date,
            )
            for o in orders_df.to_dict("records")
        ]

        execute_values(
            cur,
            """
            INSERT INTO orders (order_id, location, district, priority, weight_kg, run_date)
            VALUES %s
            ON CONFLICT (order_id) DO NOTHING
            """,
            rows,
            template="(%s, ST_GeogFromText(%s), %s, %s, %s, %s)",
            page_size=BATCH_SIZE,
        )
        total_loaded += len(rows)

    logger.info("Loaded %d orders", total_loaded)
    return total_loaded


def load_routes(cur, bucket, run_date: str) -> int:
    blob = bucket.blob(f"gold/routes/{run_date}/routes.json")
    if not blob.exists():
        logger.warning("No routes found for %s", run_date)
        return 0

    routes = json.loads(blob.download_as_text())

    from psycopg2.extras import execute_values

    rows = []
    for route in routes:
        coords = ", ".join(f"{s['lon']} {s['lat']}" for s in route["stops"])
        linestring_wkt = f"LINESTRING({coords})" if len(route["stops"]) > 1 else None

        rows.append((
            route["driver_id"],
            linestring_wkt,
            json.dumps(route["stops"]),
            route.get("total_distance_m"),
            run_date,
        ))

    execute_values(
        cur,
        """
        INSERT INTO routes (driver_id, route_geometry, stop_sequence,
                            total_distance_m, run_date)
        VALUES %s
        ON CONFLICT DO NOTHING
        """,
        rows,
        template="(%s, ST_GeogFromText(%s), %s::jsonb, %s, %s)",
        page_size=BATCH_SIZE,
    )
    logger.info("Loaded %d routes", len(routes))
    return len(routes)


def run_postgis_ingestion(run_date: str) -> dict:
    """Returns a summary dict for Airflow XCom or logging."""
    client = storage.Client.from_service_account_json(GCS_CREDENTIALS_PATH)
    bucket = client.bucket(GCS_BUCKET_NAME)

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        n_drivers = load_drivers(cur, bucket, run_date)
        n_orders  = load_orders(cur, bucket, run_date)
        n_routes  = load_routes(cur, bucket, run_date)

        logger.info("Refreshing materialized view...")
        cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY daily_kpis;")

        conn.commit()
        summary = {
            "run_date": run_date,
            "drivers_loaded": n_drivers,
            "orders_loaded": n_orders,
            "routes_loaded": n_routes,
        }
        logger.info("✅ PostGIS ingestion complete: %s", summary)
        return summary

    except Exception:
        conn.rollback()
        logger.exception("❌ Failed to load data into PostGIS")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    date = sys.argv[1] if len(sys.argv) > 1 else "2026-06-23"
    run_postgis_ingestion(date)
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
```

### Main Dashboard App
```python
# dashboard/app.py
import streamlit as st

st.set_page_config(
    page_title="Delivery Route Optimizer",
    page_icon="🗺️",
    layout="wide",
)

st.title("Smart Delivery Route Optimization Platform")
st.caption("Cairo — Powered by Spark · Sedona · OR-Tools · PostGIS")

st.markdown("""
### Welcome to the Operations Command Center
Use the sidebar to navigate through the operational layers:
* **01 Overview:** Executive KPI summary and high-level platform health.
* **02 Routes Map:** Deep dive into OR-Tools generated geometries.
* **03 Hotspots:** Spatial density analysis of incoming orders.
* **04 Drivers:** Individual fleet performance and capacity metrics.
""")
```

### Full End-to-End DAG delivery_platform_pipeline
```python
# airflow/dags/delivery_platform_pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

# 1. Bring in your configurations & functions
from ingestion_dag import ingest_roads, ingest_orders, ingest_drivers, validate_bronze
from optimization_dag import run_optimization

SPARK_SUBMIT = (
    'powershell.exe -NonInteractive -NoProfile -Command '
    '\"docker exec -u 0 delivery-platform-spark-master-1 spark-submit '
    '--master spark://spark-master:7077 '
    '--driver-memory 2G '
    '--executor-memory 8G '
    '--jars /opt/bitnami/spark/custom_jars/sedona-spark-shaded.jar,/opt/bitnami/spark/custom_jars/geotools-wrapper.jar,/opt/bitnami/spark/custom_jars/gcs-connector.jar '
    '/opt/bitnami/spark/jobs/{script} {run_date}\"'
)

default_args = {
    "owner": "delivery-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# 2. Define the single unified pipeline
with DAG(
    dag_id="delivery_platform_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["production", "unified"],
) as dag:

    # --- PHASE 1: INGESTION ---
    start = EmptyOperator(task_id="start_pipeline")
    t_roads = PythonOperator(task_id="ingest_roads", python_callable=ingest_roads)
    t_orders = PythonOperator(task_id="ingest_orders", python_callable=ingest_orders)
    t_drivers = PythonOperator(task_id="ingest_drivers", python_callable=ingest_drivers)
    v_bronze = PythonOperator(task_id="validate_bronze", python_callable=validate_bronze)

    # --- PHASE 2: SPARK PROCESSING ---
    clean_orders = SSHOperator(
        task_id='clean_orders',
        ssh_conn_id='windows_spark',
        command=SPARK_SUBMIT.format(script="bronze_to_silver.py", run_date="{{ ds }}"),
        cmd_timeout=600,
    )
    clean_roads = SSHOperator(
        task_id='clean_roads',
        ssh_conn_id='windows_spark',
        command=SPARK_SUBMIT.format(script="road_network_processing.py", run_date="{{ ds }}"),
        cmd_timeout=600,
    )
    spatial_join = SSHOperator(
        task_id='spatial_join_gold',
        ssh_conn_id='windows_spark',
        command=SPARK_SUBMIT.format(script="spatial_processing.py", run_date="{{ ds }}"),
        cmd_timeout=600,
    )

    # --- PHASE 3: OPTIMIZATION ---
    optimize = PythonOperator(
        task_id="run_route_optimization",
        python_callable=run_optimization
    )
    end = EmptyOperator(task_id="end_pipeline")

    # --- CLEAR, SEQUENTIAL DEPENDENCIES ---
    start >> [t_roads, t_orders, t_drivers] >> v_bronze
    v_bronze >> [clean_orders, clean_roads] >> spatial_join
    spatial_join >> optimize >> end
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
│   │   └── test_ssh_connection.py
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
│   
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
