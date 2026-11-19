from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ChurnAnalysis").getOrCreate()
df = spark.read.csv("hdfs://namenode:9000/data/churn.csv", header=True)
print(df.rdd.getNumPartitions())   # en combien de morceaux ?
df = df.repartition(8)             # le redécouper en 8 morceaux