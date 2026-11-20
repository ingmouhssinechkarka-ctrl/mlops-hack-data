"""Prepare deployments: copy each approved model to its environment folder in MinIO.

The senior analyst moves the aliases in the MLflow UI:
    @staging  -> s3://mlflow/deployments/staging/
    @preprod  -> s3://mlflow/deployments/preprod/
Each folder holds the model files + deployment.json (which version, when).
A folder is only rewritten when its alias points to a new version.

Usage: python prepare_deployments.py [--name hack-data-clustering]
"""
import argparse
import json
import os
import tempfile
from datetime import datetime, timezone

import boto3
import mlflow
from mlflow import MlflowClient

BUCKET = "mlflow"
ENVIRONMENTS = ["staging", "preprod"]   # alias name = environment name


def deployed_version(s3, env):
    """Version currently in deployments/<env>/, or None."""
    try:
        body = s3.get_object(Bucket=BUCKET, Key=f"deployments/{env}/deployment.json")["Body"]
        return json.load(body)["version"]
    except s3.exceptions.NoSuchKey:
        return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--name", default="hack-data-clustering")
    args = p.parse_args()

    client = MlflowClient()
    s3 = boto3.client("s3", endpoint_url=os.environ["MLFLOW_S3_ENDPOINT_URL"])

    for env in ENVIRONMENTS:
        try:
            version = client.get_model_version_by_alias(args.name, env)
        except mlflow.exceptions.MlflowException:
            print(f"{env}: no @{env} alias yet, nothing to deploy")
            continue

        if deployed_version(s3, env) == version.version:
            print(f"{env}: version {version.version} already deployed")
            continue

        with tempfile.TemporaryDirectory() as tmp:
            local = mlflow.artifacts.download_artifacts(f"models:/{args.name}@{env}", dst_path=tmp)
            # Replace the old model files with the new ones
            old = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"deployments/{env}/").get("Contents", [])
            for obj in old:
                s3.delete_object(Bucket=BUCKET, Key=obj["Key"])
            for root, _, files in os.walk(local):
                for f in files:
                    path = os.path.join(root, f)
                    key = f"deployments/{env}/model/" + os.path.relpath(path, local).replace(os.sep, "/")
                    s3.upload_file(path, BUCKET, key)

        info = {"model": args.name, "version": version.version, "run_id": version.run_id,
                "deployed_at": datetime.now(timezone.utc).isoformat()}
        s3.put_object(Bucket=BUCKET, Key=f"deployments/{env}/deployment.json",
                      Body=json.dumps(info, indent=2).encode())
        print(f"{env}: deployed version {version.version} to s3://{BUCKET}/deployments/{env}/")


if __name__ == "__main__":
    main()
