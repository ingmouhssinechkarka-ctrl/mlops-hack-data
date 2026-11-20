"""Copy the approved models (@staging, @preprod) to their deployment folders in MinIO.

Every 10 minutes. Light: no Spark, it only copies files. When no alias moved,
it prints "already deployed" and does nothing.
"""
import os
from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag


@dag(schedule="*/10 * * * *", start_date=datetime(2026, 11, 1), catchup=False,
     max_active_runs=1, tags=["hack-data"], doc_md=__doc__)
def hack_data_deploy():
    DockerOperator(
        task_id="prepare_deployments",
        command="python prepare_deployments.py",
        image="hack-data-mlops:v1",
        network_mode="streaming-hack-data_default",
        environment={
            "MLFLOW_TRACKING_URI": "http://mlflow:5000",
            "MLFLOW_S3_ENDPOINT_URL": "http://minio:9000",
            "AWS_ACCESS_KEY_ID": os.environ.get("MINIO_USER", ""),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("MINIO_PASSWORD", ""),
        },
        mem_limit="512m",
        mount_tmp_dir=False,
        auto_remove="success",
    )


hack_data_deploy()
