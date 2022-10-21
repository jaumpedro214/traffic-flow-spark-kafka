from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession\
    .builder\
    .appName('joinParquet')\
    .getOrCreate()


# Create a new column hour
spark/
    .read/
    .format('parquet')/
    .load('/opt/bitnami/spark/data/AGOSTO_2022_PARQUET_FINAL')/
    .select(['DATA HORA'])/
    .withColumn('hour', F.hour(F.col('DATA HORA')))/
    .groupBy('hour')/
    .count()/
    .orderBy('hour')/
    .show(24)

