import sys

import mlflow
import pytest

from pipeline.ingestion import SCHEMA, load_csv
from pipeline.preprocessing import FEATURES, clean
from pipeline.train import train_and_log

DATA = "data/hack_data.csv"

# Sous Windows, le LAPACK natif de Spark fait planter la JVM (toute la session de
# tests tomberait) : GaussianMixture n'est donc testé que sous Linux, donc dans la CI.
gaussian_mixture = pytest.param(
    "gaussian_mixture",
    marks=pytest.mark.skipif(sys.platform == "win32", reason="crash LAPACK natif sous Windows"),
)


@pytest.fixture(scope="module")
def data(spark):
    return clean(load_csv(spark, DATA)).cache()


@pytest.fixture
def tracking(tmp_path):
    previous = mlflow.get_tracking_uri()
    mlflow.set_tracking_uri(f"sqlite:///{(tmp_path / 'mlflow.db').as_posix()}")
    mlflow.set_experiment("tests")
    yield
    mlflow.set_tracking_uri(previous)


def test_ingestion_lit_toutes_les_colonnes(spark):
    df = load_csv(spark, DATA)
    assert df.schema == SCHEMA
    assert df.count() == 334


def test_clean_enleve_doublons_et_lignes_incompletes(spark):
    complete = (8.0, 391.09, 1.0, 2.96, 7.0, "Slovenia", 72.37)
    incomplete = (None, 391.09, 1.0, 2.96, 7.0, "Slovenia", 72.37)
    df = spark.createDataFrame([complete, complete, incomplete], SCHEMA)
    assert clean(df).count() == 1


def test_location_nest_pas_une_feature():
    assert "Location" not in FEATURES


@pytest.mark.parametrize("name", ["kmeans", "bisecting_kmeans", gaussian_mixture])
def test_k2_donne_deux_groupes_egaux(name, data, tracking):
    """Le résultat métier : avec k=2, chaque hacker a le même nombre d'attaques."""
    metrics, sizes = train_and_log(name, 2, data, {"rows_clean": 334}, save_model=False)
    assert sizes == [167, 167]
    assert metrics["balance_min_max"] == 1.0
    assert metrics["silhouette"] > 0.8


def test_k3_ne_donne_pas_trois_groupes_egaux(data, tracking):
    metrics, _ = train_and_log("kmeans", 3, data, {"rows_clean": 334}, save_model=False)
    assert metrics["balance_min_max"] < 0.9


def test_run_enregistre_dans_mlflow(data, tracking):
    train_and_log("kmeans", 2, data, {"rows_clean": 334}, save_model=False)
    run = mlflow.search_runs(output_format="list")[0]
    assert run.info.run_name == "kmeans_k2"
    assert run.info.status == "FINISHED"
    assert {"silhouette", "balance_min_max"} <= set(run.data.metrics)
