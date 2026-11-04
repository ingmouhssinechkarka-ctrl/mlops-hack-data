"""Étape 1 : ingestion. Lit hack_data.csv avec un schéma explicite."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

SCHEMA = StructType([
    StructField("Session_Connection_Time", DoubleType()),
    StructField("Bytes Transferred", DoubleType()),
    StructField("Kali_Trace_Used", DoubleType()),
    StructField("Servers_Corrupted", DoubleType()),
    StructField("Pages_Corrupted", DoubleType()),
    StructField("Location", StringType()),
    StructField("WPM_Typing_Speed", DoubleType()),
])


def load_csv(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.csv(path, header=True, schema=SCHEMA)
