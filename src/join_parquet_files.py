from pyspark.sql import SparkSession
import os

spark = SparkSession.builder\
.appName('joinParquet')\
.getOrCreate()

BASE_PATH = '/data/AGOSTO_2022_PARQUET/'

# Get all the files from the base path
files = [os.path.join(BASE_PATH, f) for f in os.listdir(BASE_PATH)]

spark.read.parquet(*files)\
.write\
.mode('overwrite')\
.parquet(
    '/data/AGOSTO_2022_PARQUET_FINAL/'
)

spark.stop()
