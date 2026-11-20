"""Check: load the model version an alias points to and classify new attacks.

Usage: python predict_with_approved.py [--alias staging] [--name hack-data-clustering]
"""
import argparse

import mlflow
import mlflow.spark
from mlflow import MlflowClient
from pyspark.sql import SparkSession

from pipeline.preprocessing import FEATURES


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--name", default="hack-data-clustering")
    p.add_argument("--alias", default="staging")
    args = p.parse_args()

    spark = (SparkSession.builder.master("local[*]").appName("predict-approved")
             .config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    version = MlflowClient().get_model_version_by_alias(args.name, args.alias)
    uri = f"models:/{args.name}@{args.alias}"
    model = mlflow.spark.load_model(uri)
    print(f"Loaded {uri} = version {version.version} "
          f"(status: {version.tags.get('validation_status', 'unknown')})")

    # Two invented attacks, same columns as the CSV
    new_attacks = spark.createDataFrame([
        (8.0, 390.0, 1.0, 3.0, 7.0, "France", 72.0),
        (35.0, 900.0, 1.0, 8.0, 12.0, "Maroc", 57.0),
    ], FEATURES[:5] + ["Location", "WPM_Typing_Speed"])
    model.transform(new_attacks).select(*FEATURES, "cluster").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()