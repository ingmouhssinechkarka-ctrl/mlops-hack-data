# 1. Create the bucket if it does not exist (waits for MinIO to be ready)
python -c '
import os, time, boto3
s3 = boto3.client("s3", endpoint_url=os.environ["MLFLOW_S3_ENDPOINT_URL"])
for attempt in range(30):
    try:
        names = [b["Name"] for b in s3.list_buckets()["Buckets"]]
        break
    except Exception:
        print("MinIO not ready yet..."); time.sleep(2)
if "mlflow" not in names:
    s3.create_bucket(Bucket="mlflow"); print("Bucket mlflow created")
'

# 2. Start the MLflow server
exec mlflow server --host 0.0.0.0 --port 5000 --workers 1 \
  --backend-store-uri "$DB_URI" \
  --artifacts-destination s3://mlflow \
  --allowed-hosts mlflow,mlflow:5000,localhost,localhost:5000,127.0.0.1:5000