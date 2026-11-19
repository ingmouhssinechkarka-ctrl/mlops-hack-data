"""Read attacks from Kafka, aggregate them, write the results to HDFS.

Outputs (in HDFS):
  /hack/attacks       every attack, parsed (Parquet)
  /hack/batch_stats   one row of statistics per micro-batch (Parquet)
  console             running totals per Kali_Trace_Used (complete mode)

Run inside the spark container (see README.md):
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/jobs/stream_job.py
Add --available-now to process what is already in Kafka and then stop.
"""
import argparse
import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType)

HDFS = "hdfs://namenode:9000"

SCHEMA = StructType([
    StructField("attack_id", LongType()),
    StructField("Session_Connection_Time", DoubleType()),
    StructField("Bytes Transferred", DoubleType()),
    StructField("Kali_Trace_Used", IntegerType()),
    StructField("Servers_Corrupted", DoubleType()),
    StructField("Pages_Corrupted", DoubleType()),
    StructField("Location", StringType()),
    StructField("WPM_Typing_Speed", DoubleType()),
])


def write_batch_stats(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    stats = batch_df.agg(
        F.count("*").alias("attacks"),
        F.round(F.avg("bytes_transferred"), 2).alias("avg_bytes"),
        F.round(F.avg("servers_corrupted"), 2).alias("avg_servers_corrupted"),
        F.round(F.avg("wpm_typing_speed"), 2).alias("avg_wpm"),
        F.sum("kali_trace_used").alias("kali_used"),
        F.countDistinct("location").alias("locations"),
        F.min("kafka_ts").alias("first_ts"),
        F.max("kafka_ts").alias("last_ts"),
    ).withColumn("batch_id", F.lit(batch_id))
    stats.write.mode("append").parquet(f"{HDFS}/hack/batch_stats")
    stats.show(truncate=False)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    p.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", "hack-attacks"))
    p.add_argument("--available-now", action="store_true",
                   help="process the data already in Kafka, then stop")
    args = p.parse_args()

    spark = (SparkSession.builder.appName("hack-stream")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.hadoop.dfs.replication", "1")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", args.bootstrap)
           .option("subscribe", args.topic)
           .option("startingOffsets", "earliest")
           .option("failOnDataLoss", "false")
           .load())

    attacks = (raw
               .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("a"),
                       F.col("timestamp").alias("kafka_ts"))
               .select("a.*", "kafka_ts")
               .withColumnRenamed("Bytes Transferred", "bytes_transferred")
               .toDF(*[c.lower() for c in
                       ["attack_id", "session_connection_time", "bytes_transferred",
                        "kali_trace_used", "servers_corrupted", "pages_corrupted",
                        "location", "wpm_typing_speed", "kafka_ts"]]))

    def trigger(writer):
        return (writer.trigger(availableNow=True) if args.available_now
                else writer.trigger(processingTime="5 seconds"))

    # 1. Every attack -> HDFS
    q_raw = trigger(attacks.writeStream
                    .format("parquet")
                    .option("path", f"{HDFS}/hack/attacks")
                    .option("checkpointLocation", f"{HDFS}/hack/_checkpoints/attacks")
                    .outputMode("append")).start()

    # 2. Statistics of each micro-batch -> HDFS
    q_stats = trigger(attacks.writeStream
                      .foreachBatch(write_batch_stats)
                      .option("checkpointLocation", f"{HDFS}/hack/_checkpoints/batch_stats")).start()

    # 3. Running totals -> console
    totals = (attacks.groupBy("kali_trace_used")
              .agg(F.count("*").alias("attacks"),
                   F.round(F.avg("bytes_transferred"), 2).alias("avg_bytes"),
                   F.round(F.avg("wpm_typing_speed"), 2).alias("avg_wpm")))
    q_console = trigger(totals.writeStream
                        .format("console")
                        .outputMode("complete")
                        .option("truncate", "false")
                        .option("checkpointLocation", f"{HDFS}/hack/_checkpoints/console")).start()

    if args.available_now:
        for q in (q_raw, q_stats, q_console):
            q.awaitTermination()
    else:
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
