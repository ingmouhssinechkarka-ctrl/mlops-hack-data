# streaming-hack-data

The 334 attacks in `data/hack_data.csv` go through **Kafka**, get processed by **Spark Structured Streaming**, and end up in **HDFS**.

```
producer.py ──> Kafka topic "hack-attacks" ──> spark/stream_job.py ──> HDFS /hack/attacks
                                                                   ├─> HDFS /hack/batch_stats
                                                                   └─> console (running totals)
```

## Settings

`.env` holds the image versions, ports and topic name. `docker compose` reads it automatically.

## Commands

```powershell
# 0. Python virtual environment for the producer (once)
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# 1. Start Kafka, HDFS and Spark (wait about 30 s for HDFS to be ready)
docker compose up -d

# 1b. Create the topic (Spark won't auto-create it; safe to re-run)
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 `
  --create --if-not-exists --topic hack-attacks --partitions 3 --replication-factor 1

# 2. Start the streaming job (leave this terminal open)
docker compose exec spark /opt/spark/bin/spark-submit `
  --conf spark.jars.ivy=/tmp/.ivy2 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 `
  /opt/jobs/stream_job.py

# 3. In a second terminal (venv activated): send the attacks
python producer/producer.py            # --delay 0 for maximum speed, --loops 3 to replay

# 4. Stop the job (Ctrl+C), then count what landed in HDFS
docker compose exec spark /opt/spark/bin/spark-submit /opt/jobs/check_hdfs.py
```

To process the data already in Kafka and then stop (no need for Ctrl+C), add `--available-now` after `stream_job.py`.

## Useful

| What | Where |
|---|---|
| HDFS web UI (Utilities > Browse) | http://localhost:9870 |
| Spark UI (while a job runs) | http://localhost:4040 |
| List HDFS files | `docker compose exec namenode hdfs dfs -ls -R /hack` |
| Read the topic | `docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic hack-attacks --from-beginning` |
| Start over (deletes HDFS data) | `docker compose exec namenode hdfs dfs -rm -r -f /hack` |
| Stop everything | `docker compose down` (add `-v` to also remove the HDFS volumes) |

The job keeps its Kafka offsets in `/hack/_checkpoints`. If you restart it, it picks up where it stopped and does not write duplicates. If you delete `/hack`, it reads the topic from the beginning again.
