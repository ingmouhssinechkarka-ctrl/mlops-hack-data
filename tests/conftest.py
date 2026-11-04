import os
import sys

import pytest
from pyspark.sql import SparkSession

# Les workers Spark doivent utiliser le même Python que les tests.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder.appName("tests").master("local[2]")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
