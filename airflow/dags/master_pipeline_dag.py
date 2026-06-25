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