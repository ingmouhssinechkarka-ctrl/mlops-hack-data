"""Étape 3 : 3 modèles de clustering Spark ML, testés avec k = 2 et k = 3 hackers.
Chaque combinaison (modèle, k) = 1 run MLflow."""
import json

import mlflow
import mlflow.spark
from pyspark.ml import Pipeline
from pyspark.ml.clustering import BisectingKMeans, GaussianMixture, KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

from pipeline.preprocessing import feature_stages

MODELS = {
    "kmeans": lambda k: KMeans(k=k, seed=42),
    "bisecting_kmeans": lambda k: BisectingKMeans(k=k, seed=42),
    "gaussian_mixture": lambda k: GaussianMixture(k=k, seed=42),
}
K_VALUES = [2, 3]


def train_and_log(name, k, df, data_info, save_model=True):
    clf = MODELS[name](k).setFeaturesCol("features").setPredictionCol("cluster")
    pipeline = Pipeline(stages=feature_stages() + [clf])

    with mlflow.start_run(run_name=f"{name}_k{k}"):
        mlflow.set_tag("model_type", name)
        mlflow.log_params(data_info)
        mlflow.log_params({p.name: v for p, v in clf.extractParamMap().items()
                           if clf.isSet(p) and p.name not in ("featuresCol", "predictionCol")})

        model = pipeline.fit(df)
        preds = model.transform(df)

        # Qualité des groupes : silhouette (entre -1 et 1, plus c'est haut mieux c'est)
        # Indice clé de l'énoncé : les hackers se partagent les attaques à parts égales
        sizes = {int(r["cluster"]): r["count"]
                 for r in preds.groupBy("cluster").count().collect()}
        if len(sizes) > 1:
            silhouette = ClusteringEvaluator(featuresCol="features",
                                             predictionCol="cluster").evaluate(preds)
        else:  # le modèle a tout mis dans un seul groupe
            silhouette = -1.0
        sizes = [sizes.get(i, 0) for i in range(k)]
        balance = min(sizes) / max(sizes)

        metrics = {"silhouette": silhouette, "balance_min_max": balance}
        mlflow.log_metrics(metrics)
        mlflow.log_param("cluster_sizes", str(sizes))
        mlflow.log_text(json.dumps({"k": k, "cluster_sizes": sizes}, indent=2), "clusters.json")

        if save_model:
            try:
                mlflow.spark.log_model(model, "model")
            except Exception as e:  # souvent winutils manquant sous Windows
                print(f"  (modèle non sauvegardé : {type(e).__name__}. "
                      "Relance avec --no-model si ça bloque)")
    return metrics, sizes
