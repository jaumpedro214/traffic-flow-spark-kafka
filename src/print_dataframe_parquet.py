from pyspark.sql import SparkSession

spark = SparkSession.builder\
.appName('joinParquet')\
.getOrCreate()

spark.read.parquet('/data/AGOSTO_2022_PARQUET_FINAL/')\
.show(20)

spark.stop()