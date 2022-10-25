from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "traffic_sensor"

SCHEMA = StructType([
    StructField("ID EQP", StringType()),
    StructField("DATA HORA", TimestampType()),
    StructField("MILESEGUNDO", IntegerType()),
    StructField("FAIXA", IntegerType()),
    StructField("ID DE ENDEREÇO", IntegerType()),
    StructField("VELOCIDADE DA VIA", IntegerType()),
    StructField("VELOCIDADE AFERIDA", IntegerType()),
    StructField("TAMANHO", StringType()),
    StructField("LATITUDE", StringType()),
    StructField("LONGITUDE", StringType()),
    StructField("ENDEREÇO", StringType()),
    StructField("SENTIDO", StringType())
])


spark = SparkSession.builder.appName("read_traffic_sensor_topic").getOrCreate()

