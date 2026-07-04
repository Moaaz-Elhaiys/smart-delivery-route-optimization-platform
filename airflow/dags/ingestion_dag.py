# airflow/dags/ingestion_dag.py — refactored
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# Default args with retry logic
default_args = {
    "owner": "delivery-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
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