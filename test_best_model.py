"""Choisit le meilleur modèle dans MLflow puis le teste.

1. Choix : on lit les runs MLflow et on prend celui avec les groupes les plus
   équilibrés (balance_min_max), puis la meilleure silhouette.
2. Chargement : on charge le modèle sauvegardé dans le run. S'il n'existe pas
   (souvent sous Windows : winutils manquant), on le réentraîne avec les mêmes
   paramètres que ceux enregistrés dans MLflow.
3. Tests :
   a. les métriques retrouvées = celles enregistrées dans MLflow (reproductible) ;
   b. les groupes ont la même taille (l'indice de l'énoncé) ;
   c. le modèle classe de nouvelles attaques dans un des k groupes ;
   d. la même attaque donne toujours le même groupe.

Usage : python test_best_model.py [--data data/hack_data.csv] [--experiment hack-data]
"""
import argparse
import os
import sys

import mlflow
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import SparkSession

from pipeline.ingestion import load_csv
from pipeline.preprocessing import FEATURES, clean, feature_stages
from pipeline.train import MODELS

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


def choose_best_run(experiment):
    runs = mlflow.search_runs(experiment_names=[experiment],
                              filter_string="metrics.silhouette > -1",
                              order_by=["start_time DESC"])
    if runs.empty:
        sys.exit("Aucun run avec des métriques : lance d'abord python run_pipeline.py")
    runs = runs.drop_duplicates("tags.mlflow.runName")  # garde le run le plus récent de chaque type
    runs["balance"] = runs["metrics.balance_min_max"].round(2)
    runs["silhouette"] = runs["metrics.silhouette"].round(4)
    best = runs.sort_values(["balance", "silhouette"], ascending=False).iloc[0]

    print("Classement des runs :")
    print(runs.sort_values(["balance", "silhouette"], ascending=False)
          [["tags.mlflow.runName", "silhouette", "balance", "params.cluster_sizes"]]
          .to_string(index=False))
    print(f"\n==> Modèle choisi : {best['tags.mlflow.runName']} (run {best['run_id'][:8]})\n")
    return best


def get_model(best, df):
    try:
        model = mlflow.spark.load_model(f"runs:/{best['run_id']}/model")
        print("Modèle chargé depuis MLflow.")
        return model
    except Exception:
        print("Pas de modèle sauvegardé dans ce run -> réentraînement avec les paramètres MLflow.")
    name = best["tags.model_type"]
    k = int(best["params.k"])
    clf = MODELS[name](k).setFeaturesCol("features").setPredictionCol("cluster")
    return Pipeline(stages=feature_stages() + [clf]).fit(df)


def check(label, ok):
    print(f"  [{'OK' if ok else 'ÉCHEC'}] {label}")
    return ok


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="data/hack_data.csv")
    parser.add_argument("--experiment", default="hack-data")
    args = parser.parse_args()

    spark = (SparkSession.builder.appName("test-best-model").master("local[*]")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    best = choose_best_run(args.experiment)
    k = int(best["params.k"])
    df = clean(load_csv(spark, args.data)).cache()
    model = get_model(best, df)
    preds = model.transform(df)

    print("\nTests :")
    results = []

    # a. Reproductible : on retrouve la silhouette enregistrée dans MLflow
    silhouette = ClusteringEvaluator(featuresCol="features",
                                     predictionCol="cluster").evaluate(preds)
    results.append(check(f"silhouette {silhouette:.4f} = MLflow {best['metrics.silhouette']:.4f}",
                         abs(silhouette - best["metrics.silhouette"]) < 1e-3))

    # b. Les hackers ont fait le même nombre d'attaques
    sizes = sorted(r["count"] for r in preds.groupBy("cluster").count().collect())
    results.append(check(f"{k} groupes de même taille : {sizes}",
                         len(sizes) == k and min(sizes) / max(sizes) > 0.9))

    # c. Nouvelles attaques (inventées) : chacune doit tomber dans un groupe 0..k-1
    new_attacks = spark.createDataFrame([
        (8.0, 390.0, 1.0, 3.0, 7.0, "France", 72.0),
        (35.0, 900.0, 1.0, 8.0, 12.0, "Maroc", 57.0),
    ], FEATURES[:5] + ["Location", "WPM_Typing_Speed"])
    new_preds = model.transform(new_attacks).select(*FEATURES, "cluster").collect()
    for row in new_preds:
        print(f"     attaque {dict(list(row.asDict().items())[:6])} -> hacker n°{row['cluster']}")
    results.append(check("nouvelles attaques classées dans un groupe valide",
                         all(0 <= r["cluster"] < k for r in new_preds)))

    # d. Stable : prédire deux fois la même chose donne le même résultat
    again = model.transform(new_attacks).select("cluster").collect()
    results.append(check("même attaque -> même groupe",
                         [r["cluster"] for r in new_preds] == [r["cluster"] for r in again]))

    spark.stop()
    print(f"\n{sum(results)}/{len(results)} tests réussis.")
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()
