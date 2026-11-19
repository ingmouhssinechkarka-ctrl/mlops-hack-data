"""Count what the streaming job wrote to HDFS.

Run inside the spark container (see README.md):
  spark-submit /opt/jobs/check_hdfs.py
"""
from pyspark.sql import SparkSession, functions as F

HDFS = "hdfs://namenode:9000"
EXPECTED = 335  # data rows in hack_data.csv (one producer run)


def main():
    spark = SparkSession.builder.appName("check-hdfs").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    attacks = spark.read.parquet(f"{HDFS}/hack/attacks")
    total = attacks.count()
    distinct = attacks.select("attack_id").distinct().count()
    print(f"\n/hack/attacks       : {total} rows ({distinct} distinct attack_id), "
          f"expected {EXPECTED} per producer run")

    print("\nAttacks per Kali_Trace_Used:")
    attacks.groupBy("kali_trace_used").count().orderBy("kali_trace_used").show()

    print("Top 10 locations:")
    attacks.groupBy("location").count().orderBy(F.desc("count")).show(10, truncate=False)

    stats = spark.read.parquet(f"{HDFS}/hack/batch_stats")
    print(f"/hack/batch_stats   : {stats.count()} micro-batches, "
          f"{stats.agg(F.sum('attacks')).first()[0]} attacks in total")
    stats.orderBy("batch_id").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
