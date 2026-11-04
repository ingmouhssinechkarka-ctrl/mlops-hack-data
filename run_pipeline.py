"""Pipeline MLOps : ingestion CSV -> traitement Spark ML -> 3 modèles de clustering -> MLflow.

Question : 2 ou 3 hackers ont-ils attaqué ? Indice : chaque hacker a fait
à peu près le même nombre d'attaques, donc les bons groupes ont la même taille.

Usage : python run_pipeline.py [--data data/hack_data.csv] [--experiment hack-data] [--no-model]
"""
import argparse

import mlflow
from pyspark.sql import SparkSession

from pipeline.ingestion import load_csv
from pipeline.preprocessing import clean
from pipeline.train import K_VALUES, MODELS, train_and_log


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="data/hack_data.csv")
    parser.add_argument("--experiment", default="hack-data")
    parser.add_argument("--no-model", action="store_true",
                        help="ne sauvegarde pas le modèle Spark dans MLflow")
    args = parser.parse_args()

    spark = (SparkSession.builder.appName("mlops-hack-data").master("local[*]")
             .config("spark.sql.shuffle.partitions", "4")      # petit fichier -> peu de partitions
             .config("spark.ui.showConsoleProgress", "false")  # pas de barres de progression
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Ingestion
    raw = load_csv(spark, args.data)
    n_raw = raw.count()

    # 2. Traitement
    df = clean(raw).cache()
    data_info = {"data_path": args.data, "rows_raw": n_raw, "rows_clean": df.count()}
    print("Données :", data_info)

    # 3. Entraînement + tracking
    mlflow.set_experiment(args.experiment)
    print(f"\n{'run':24s} {'silhouette':>10s} {'équilibre':>10s}  tailles des groupes")
    for name in MODELS:
        for k in K_VALUES:
            m, sizes = train_and_log(name, k, df, data_info, save_model=not args.no_model)
            print(f"{name + '_k' + str(k):24s} {m['silhouette']:10.4f} "
                  f"{m['balance_min_max']:10.2f}  {sizes}")

    print("\nLecture : si les groupes sont de même taille (équilibre ~1.00) avec k=2 "
          "mais pas avec k=3, il y a 2 hackers.")
    print("Lance `mlflow ui` puis ouvre http://127.0.0.1:5000 pour comparer les runs.")
    spark.stop()


if __name__ == "__main__":
    main()
