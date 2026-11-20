"""Retrain the hack-data models when new attacks have arrived in HDFS.

check_new_data -> train -> register -> remember_size

Every hour. If /hack/attacks has not grown since the last training, the run stops
after check_new_data and the three other tasks show as "skipped".
The senior analyst still approves by hand: this DAG only creates @candidate.
"""
import os
from datetime import datetime

import requests
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import Variable, dag, task

HDFS_SUMMARY = "http://namenode:9870/webhdfs/v1/hack/attacks?op=GETCONTENTSUMMARY"
LAST_SIZE = "hack_last_trained_size"

# Same settings as the "trainer" service in docker-compose.yml
TRAINER = dict(
    image="hack-data-mlops:v1",
    network_mode="streaming-hack-data_default",   # the network of your compose project
    environment={
        "MLFLOW_TRACKING_URI": "http://mlflow:5000",
        "SPARK_MASTER": "local[2]",
        "PYSPARK_SUBMIT_ARGS": "--driver-memory 512m pyspark-shell",   # the data is tiny
        # MinIO login, so the models can be saved. Read from the Airflow
        # container's environment (filled from .env), never written here.
        "MLFLOW_S3_ENDPOINT_URL": "http://minio:9000",
        "AWS_ACCESS_KEY_ID": os.environ.get("MINIO_USER", ""),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("MINIO_PASSWORD", ""),
    },
    mem_limit="1g",           # with Airflow running, more than this freezes Docker on an 8 GB PC
    mount_tmp_dir=False,      # required with Docker Desktop
    auto_remove="success",    # delete the container when it succeeds
)


@dag(schedule="@hourly", start_date=datetime(2026, 11, 1), catchup=False,
     max_active_runs=1, tags=["hack-data"], doc_md=__doc__)
def hack_data_retrain():

    @task.short_circuit
    def check_new_data(**context) -> bool:
        reply = requests.get(HDFS_SUMMARY, timeout=10)
        reply.raise_for_status()
        size = reply.json()["ContentSummary"]["length"]
        last = int(Variable.get(LAST_SIZE, default="0"))
        print(f"/hack/attacks is {size} bytes; the last training saw {last} bytes")
        context["ti"].xcom_push(key="size", value=size)
        return size > last        # False: skip train, register and remember_size

    medallion = DockerOperator(        # bronze -> silver (+ gold with the @staging model)
        task_id="medallion",
        command="python medallion.py",
        **TRAINER,
    )

    train = DockerOperator(
        task_id="train",
        command="python run_pipeline.py --data hdfs://namenode:9000/hack/silver/attacks "
                "--experiment hack-data-stream",
        **TRAINER,
    )

    register = DockerOperator(
        task_id="register",
        command="python register_best_model.py --experiment hack-data-stream",
        **TRAINER,
    )

    @task
    def remember_size(**context):
        size = context["ti"].xcom_pull(task_ids="check_new_data", key="size")
        Variable.set(LAST_SIZE, str(size))
        print(f"Saved {size} bytes as the size of the last training")

    check_new_data() >> medallion >> train >> register >> remember_size()


hack_data_retrain()