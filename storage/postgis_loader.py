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