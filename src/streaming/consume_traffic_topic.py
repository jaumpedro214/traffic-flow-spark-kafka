from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "traffic_sensor"

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


spark = SparkSession.builder.appName("read_traffic_sensor_topic").getOrCreate()

# Reduce logging verbosity
spark.sparkContext.setLogLevel("WARN")

df_traffic_stream = spark\
    .readStream.format("kafka")\
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)\
    .option("subscribe", KAFKA_TOPIC)\
    .option("startingOffsets", "earliest")\
    .load()

df_traffic_stream.select(
    # Convert the value to a string
    F.from_json(
        F.col("value").cast("string"), SCHEMA
    ).alias("value")
)\
.select("value.*")\
.writeStream\
.outputMode("append")\
.format("console")\
.start()\
.awaitTermination()

