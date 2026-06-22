from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.insert(0,"/Users/elhaiys/Desktop/Desktop/vs projects/Smart-Delivery-Route-Optimization-Platform")

from ingestion.overpass_client import fetch_roads
from ingestion.data_simulator import simulate_orders, simulate_drivers
from ingestion.bronze_writer import upload_to_bronze
from ingestion.schemas import (validate_roads,validate_orders,validate_drivers)
def ingest_roads(**context):
    data = fetch_roads()
    validate_roads(data) 
    upload_to_bronze(data, f"roads/{context['ds']}/roads.json")

def ingest_orders(**context):
    orders = simulate_orders(n=500)
    validate_orders(orders)
    upload_to_bronze(orders, f"orders/{context['ds']}/orders.json")

def ingest_drivers(**context):
    drivers = simulate_drivers(n=25)
    validate_drivers(drivers)
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