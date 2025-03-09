from datetime import datetime

from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator


@dag(start_date=datetime(2022, 8, 1), schedule=None, catchup=False)
def process_sales_data():
    process_raw_sales_data = DockerOperator(
        task_id="process_raw_sales_data",
        image="process-sales-data:latest",
        command=[
            "--csv",
            "/app/generate_sales_data.csv",
            "--table",
            "sales_data_raw",
            "--host",
            "postgres-sales",
            "--port",
            "5432",
            "--replace_table",
        ],
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        auto_remove="success",
        container_name="process_raw_sales_data_{{ ts_nodash }}",
        network_mode="hostaway_de_assignment_airflow-net",
    )
    
    dbt_sales_data = DockerOperator(
        task_id="dbt_sales_data",
        image="dbt-process-sales-data:latest",
        command=[
            "build",
            "--profiles-dir", ".dbt",
            "--target", "prod", "--full-refresh"
        ],
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        auto_remove="success",
        container_name="dbt_sales_data_{{ ts_nodash }}",
        network_mode="hostaway_de_assignment_airflow-net",
    ) 
    
    process_raw_sales_data >> dbt_sales_data


process_sales_data()
