"""Étape 2 : traitement des données avec Spark (nettoyage) et Spark ML (features).

La colonne Location est ignorée : les hackers utilisent des VPN, donc le pays
ne dit rien sur qui a fait l'attaque."""
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame

FEATURES = [
    "Session_Connection_Time",
    "Bytes Transferred",
    "Kali_Trace_Used",
    "Servers_Corrupted",
    "Pages_Corrupted",
    "WPM_Typing_Speed",
]


def clean(df: DataFrame) -> DataFrame:
    """Supprime les doublons et les lignes incomplètes."""
    return df.dropDuplicates().dropna(subset=FEATURES)


def feature_stages():
    """Étapes Spark ML placées devant chaque modèle dans le Pipeline :
    on regroupe les colonnes dans un vecteur, puis on les met à la même échelle."""
    return [
        VectorAssembler(inputCols=FEATURES, outputCol="raw_features"),
        StandardScaler(inputCol="raw_features", outputCol="features"),
    ]
