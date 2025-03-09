from datetime import datetime

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator


@dag(start_date=datetime(2022, 8, 1), schedule=None, catchup=False)
def airflow_docker_operator():
    test_docker_operator = DockerOperator(
        task_id="test_docker_operator",
        image="busybox:latest",
        command=["sleep", "120"],
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        auto_remove="success",
        container_name="test-docker-{{ ts_nodash }}",
        ### environment={
        ###   'CLICKHOUSE_HOST': 'clickhousedb',
        ###   'CLICKHOUSE_USER': <username>,
        ###   'CLICKHOUSE_PASSWORD': <password>,
        ###   'MONGO_HOST': 'mongodb',
        ###   'MONGO_USER': <username>,
        ###   'MONGO_PASSWORD': <password>
        ### }
    )
    empty_task = EmptyOperator(task_id="empty_task")
    test_docker_operator >> empty_task


airflow_docker_operator()
