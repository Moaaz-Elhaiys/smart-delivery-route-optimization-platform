# airflow/dags/optimization_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def run_optimization(**context):
    import os, json, sys
    from google.cloud import storage
    from dotenv import load_dotenv
    load_dotenv()
    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not key_path:
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS is not set in the .env file!")

    sys.path.insert(0, '/Users/elhaiys/Desktop/Desktop/vs projects/smart-delivery-route-optimization-platform')
    from optimization.vrp_constrained import solve_vrp_with_constraints
    run_date = context['ds']
    client   = storage.Client.from_service_account_json(key_path)
    bucket   = client.bucket("delivery-data-lake")

    # Load gold layer data
    orders_blob  = bucket.blob(f"gold/orders_spatial/{run_date}/part-00000-b1cc003a-338c-45a5-93cf-39a5312c4e29-c000.snappy.parquet")
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