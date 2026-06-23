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