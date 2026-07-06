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