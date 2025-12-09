from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("test") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()

df = spark.read.text("hdfs://namenode:8020/")
df.show()
