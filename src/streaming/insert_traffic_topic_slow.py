from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType

import time

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "traffic_sensor"
FILE_PATH = "/data/AGOSTO_2022_PARQUET_FINAL/"

# "ID EQP" -> INT 64

SCHEMA = StructType([
    StructField("ID EQP", LongType()),
    StructField("DATA HORA", TimestampType()),
    StructField("MILESEGUNDO", LongType()),
    StructField("CLASSIFICAÇÃO", StringType()),
    StructField("FAIXA", LongType()),
    StructField("ID DE ENDEREÇO", LongType()),
    StructField("VELOCIDADE DA VIA", StringType()),
    StructField("VELOCIDADE AFERIDA", StringType()),
    StructField("TAMANHO", StringType()),
    StructField("NUMERO DE SÉRIE", LongType()),
    StructField("LATITUDE", StringType()),
    StructField("LONGITUDE", StringType()),
    StructField("ENDEREÇO", StringType()),
    StructField("SENTIDO", StringType())
])

spark = SparkSession.builder.appName("write_traffic_sensor_topic").getOrCreate()
spark.sparkContext.setLogLevel("WARN") # Reduce logging verbosity

# Read the parquet file write it to the topic
# We need to specify the schema in the stream
# and also convert the entries to the format key, value
df_traffic_stream = spark.read.format("parquet")\
    .schema(SCHEMA)\
    .load(FILE_PATH)\
    .withColumn("value", F.to_json( F.struct(F.col("*")) ) )\
    .withColumn("key", F.lit("key"))\
    .withColumn("value", F.encode(F.col("value"), "iso-8859-1").cast("binary"))\
    .withColumn("key", F.encode(F.col("key"), "iso-8859-1").cast("binary"))\
    .limit(50000)

# Write one row at a time to the topic

for row in df_traffic_stream.collect():

    # transform row to dataframe
    df_row = spark.createDataFrame([row.asDict()])

    # write to topic
    df_row.write\
        .format("kafka")\
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)\
        .option("topic", KAFKA_TOPIC)\
        .save()

    print(f"Row written to topic {KAFKA_TOPIC}")
    time.sleep(2.5)
