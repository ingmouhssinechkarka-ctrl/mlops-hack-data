"""Analyst step: register the best run of the latest training as a new model version.

1. Reads the runs of the experiment and keeps the latest run of each type.
2. Picks the best: most balanced groups first, then best silhouette.
3. Quality gate: refuses a model whose groups are unbalanced or badly separated.
4. Registers it, writes its numbers as tags, and gives it the alias @candidate.

Usage: python register_best_model.py [--experiment hack-data-stream] [--name hack-data-clustering]
"""
import argparse
import sys

import mlflow
from mlflow import MlflowClient

MIN_BALANCE = 0.9      # the hint: each hacker made about the same number of attacks
MIN_SILHOUETTE = 0.7   # the groups must be clearly separated


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--experiment", default="hack-data-stream")
    p.add_argument("--name", default="hack-data-clustering")
    args = p.parse_args()

    runs = mlflow.search_runs(experiment_names=[args.experiment],
                              filter_string="attributes.status = 'FINISHED'",
                              order_by=["start_time DESC"])
    if runs.empty:
        sys.exit(f"No finished run in '{args.experiment}': run the trainer first.")

    latest = runs.drop_duplicates("tags.mlflow.runName").copy()   # latest run of each type
    latest["balance"] = latest["metrics.balance_min_max"].round(2)
    best = latest.sort_values(["balance", "metrics.silhouette"], ascending=False).iloc[0]

    run_name = best["tags.mlflow.runName"]
    balance, silhouette = best["metrics.balance_min_max"], best["metrics.silhouette"]
    print(f"Best run: {run_name}  balance={balance:.2f}  silhouette={silhouette:.4f}")

    if balance < MIN_BALANCE or silhouette < MIN_SILHOUETTE:
        sys.exit("Quality gate failed: the model is NOT registered.")

    version = mlflow.register_model(f"runs:/{best['run_id']}/model", args.name)

    client = MlflowClient()
    tags = {
        "run_name": run_name,
        "silhouette": f"{silhouette:.4f}",
        "balance_min_max": f"{balance:.2f}",
        "cluster_sizes": best["params.cluster_sizes"],
        "rows_clean": best["params.rows_clean"],
        "validation_status": "pending",
    }
    for key, value in tags.items():
        client.set_model_version_tag(args.name, version.version, key, value)
    client.set_registered_model_alias(args.name, "candidate", version.version)

    print(f"Registered {args.name} version {version.version} as @candidate.")
    print("Next: the senior analyst reviews it in the MLflow UI (Model registry).")


if __name__ == "__main__":
    main()