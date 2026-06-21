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