"""Bronze -> Silver -> Gold in HDFS.

Bronze  /hack/attacks                    raw attacks written by the streaming job (part 1)
Silver  /hack/silver/attacks             same attacks, renamed, typed, no duplicates, no gaps
Gold    /hack/gold/attacks_per_hacker    the answer: attacks per hacker, using the @staging model

Usage: python medallion.py [--model hack-data-clustering@staging]
Gold is skipped (with a message) while no model has the @staging alias.
"""
import argparse

import mlflow
import mlflow.spark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline.ingestion import load_parquet
from pipeline.preprocessing import FEATURES, clean

HDFS = "hdfs://namenode:9000/hack"
BRONZE = f"{HDFS}/attacks"
SILVER = f"{HDFS}/silver/attacks"
GOLD = f"{HDFS}/gold/attacks_per_hacker"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--model", default="hack-data-clustering@staging")
    args = p.parse_args()

    spark = (SparkSession.builder.appName("medallion").master("local[2]")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    # Bronze -> Silver: rename + type (load_parquet), then drop duplicates and gaps (clean)
    bronze = spark.read.parquet(BRONZE)
    silver = clean(load_parquet(spark, BRONZE))
    silver.write.mode("overwrite").parquet(SILVER)
    silver = spark.read.parquet(SILVER)
    print(f"Bronze: {bronze.count()} rows -> Silver: {silver.count()} rows  ({SILVER})")

    # Silver -> Gold: which hacker made each attack, then one row per hacker
    try:
        model = mlflow.spark.load_model(f"models:/{args.model}")
    except Exception as e:
        print(f"Gold skipped: cannot load models:/{args.model} ({type(e).__name__})")
        spark.stop()
        return

    gold = (model.transform(silver)
            .groupBy(F.col("cluster").alias("hacker"))
            .agg(F.count("*").alias("attacks"),
                 *[F.round(F.avg(c), 2).alias(f"avg_{c.replace(' ', '_')}") for c in FEATURES])
            .withColumn("model", F.lit(args.model))
            .withColumn("computed_at", F.current_timestamp())
            .orderBy("hacker"))
    gold.write.mode("overwrite").parquet(GOLD)
    print(f"Gold ({GOLD}):")
    spark.read.parquet(GOLD).drop("model", "computed_at").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
