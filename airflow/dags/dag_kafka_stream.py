from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
import os
from src.kafka_client.kafka_streaming import stream


client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
user_agent = os.getenv("USER_AGENT")
start_date = datetime.today() - timedelta(days=1)


default_args = {
    "owner": "airflow",
    "start_date": start_date,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}


with DAG(
    dag_id="kafka_spark_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    kafka_stream_task = PythonOperator(
        task_id="kafka_data_stream",
        python_callable=stream,
    )

    spark_stream_task = DockerOperator(
        task_id="pyspark_consumer",
        image="reddit-posts/spark:latest",
        api_version="auto",
        auto_remove=True,
        command="""
            /opt/bitnami/spark/bin/spark-submit \
            --master local[*] \
            --packages org.postgresql:postgresql:42.5.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
            --py-files ./configs \
            ./spark_streaming.py
        """,
        docker_url='tcp://docker-proxy:2375',
        environment={'SPARK_LOCAL_HOSTNAME': 'localhost',
                     'PG_PASSWORD': os.getenv("PG_PASSWORD")
        },
        network_mode="airflow-kafka",
        mount_tmp_dir=False,
    )


    kafka_stream_task >> spark_stream_task