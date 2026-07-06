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