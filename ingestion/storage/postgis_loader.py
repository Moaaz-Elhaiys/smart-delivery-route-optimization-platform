# storage/postgis_loader.py
import psycopg2
import json
import io
import os
import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "dbname":   "delivery_platform",
    "user":     "postgres",
    "password": os.getenv("DB_PASSWORD"), # Update this or use os.getenv("DB_PASSWORD")
    "host":     "localhost",
    "port":     5432,
}

def load_drivers(cur, bucket, run_date):
    """Load drivers from Bronze JSON to satisfy the Foreign Key constraint."""
    blob = bucket.blob(f"bronze/drivers/{run_date}/drivers.json")
    if not blob.exists():
        print("No drivers found for this date. Skipping driver load.")
        return
        
    drivers = json.loads(blob.download_as_text())
    
    for driver in drivers:
        cur.execute("""
            INSERT INTO drivers (driver_id, location, home_district, capacity_kg, status)
            VALUES (%s, ST_GeogFromText(%s), %s, %s, %s)
            ON CONFLICT (driver_id) DO UPDATE 
            SET location = EXCLUDED.location,
                status = EXCLUDED.status,
                last_updated_at = NOW()
        """, (
            driver["driver_id"],
            f"POINT({driver['lon']} {driver['lat']})",
            driver.get("district"),
            driver.get("capacity_kg"),
            driver.get("status", "available")
        ))
    print(f"Loaded/Updated {len(drivers)} drivers.")

def load_orders(cur, bucket, run_date):
    """Load spatially-enriched orders from Gold Parquet."""
    prefix = f"gold/orders_spatial/{run_date}/"
    blobs = list(bucket.list_blobs(prefix=prefix))
    parquet_blob = next((b for b in blobs if b.name.endswith(".parquet")), None)
    
    if not parquet_blob:
        print("No order parquet files found. Skipping order load.")
        return

    orders_df = pd.read_parquet(io.BytesIO(parquet_blob.download_as_bytes()))
    orders = orders_df.to_dict("records")
    
    for order in orders:
        cur.execute("""
            INSERT INTO orders (order_id, location, district, priority, weight_kg, run_date)
            VALUES (%s, ST_GeogFromText(%s), %s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING
        """, (
            order["order_id"],
            f"POINT({order['lon']} {order['lat']})",
            order.get("district"),
            order.get("priority"),
            order.get("weight_kg"),
            run_date
        ))
    print(f"Loaded {len(orders)} orders.")

def load_routes(cur, bucket, run_date):
    """Load optimized routes from Gold JSON."""
    blob = bucket.blob(f"gold/routes/{run_date}/routes.json")
    if not blob.exists():
        print("No routes found. Skipping route load.")
        return
        
    routes = json.loads(blob.download_as_text())
        
    for route in routes:
        # Build WKT LineString from stop sequence
        coords = ", ".join(f"{s['lon']} {s['lat']}" for s in route["stops"])
        linestring_wkt = f"LINESTRING({coords})" if len(route["stops"]) > 1 else None

        cur.execute("""
            INSERT INTO routes (driver_id, route_geometry, stop_sequence,
                                total_distance_m, run_date)
            VALUES (%s, ST_GeogFromText(%s), %s::jsonb, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            route["driver_id"],
            linestring_wkt,
            json.dumps(route["stops"]),
            route.get("total_distance_m"),
            run_date,
        ))
    print(f"Loaded {len(routes)} routes.")

def run_postgis_ingestion(run_date):
    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    client = storage.Client.from_service_account_json(key_path)
    bucket = client.bucket("delivery-data-lake")

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        # 1. Load Drivers FIRST (Satisfies Foreign Keys)
        load_drivers(cur, bucket, run_date)
        
        # 2. Load Orders (Needed for KPIs)
        load_orders(cur, bucket, run_date)
        
        # 3. Load Routes
        load_routes(cur, bucket, run_date)

        # 4. Refresh KPI materialized view
        print("Refreshing Materialized View...")
        cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY daily_kpis;")

        conn.commit()
        print(f"✅ Successfully ingested all PostGIS data for {run_date}")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Failed to load data into PostGIS: {e}")
        raise e
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    import sys
    # For local testing: python storage/postgis_loader.py 2026-06-23
    test_date = sys.argv if len(sys.argv) > 1 else "2026-06-23"
    run_postgis_ingestion(test_date)