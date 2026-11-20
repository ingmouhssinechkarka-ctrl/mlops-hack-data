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
from pyspark.sql import functions as F

# Names written by the streaming job (part 1) -> names used by this pipeline
STREAM_COLUMNS = {
    "session_connection_time": "Session_Connection_Time",
    "bytes_transferred": "Bytes Transferred",
    "kali_trace_used": "Kali_Trace_Used",
    "servers_corrupted": "Servers_Corrupted",
    "pages_corrupted": "Pages_Corrupted",
    "location": "Location",
    "wpm_typing_speed": "WPM_Typing_Speed",
}


def load_parquet(spark: SparkSession, path: str) -> DataFrame:
    """Reads attacks stored in HDFS, with the CSV's column names.
    Bronze (streaming job) has lowercase names; silver already has the CSV names."""
    df = spark.read.parquet(path)
    if "Session_Connection_Time" in df.columns:          # silver: already renamed
        return df.select(*SCHEMA.fieldNames())
    return df.select([F.col(src).cast(SCHEMA[dst].dataType).alias(dst)
                      for src, dst in STREAM_COLUMNS.items()])


def load_data(spark: SparkSession, path: str) -> DataFrame:
    """A .csv file -> load_csv; anything else (an HDFS folder) -> load_parquet."""
    return load_csv(spark, path) if path.endswith(".csv") else load_parquet(spark, path)