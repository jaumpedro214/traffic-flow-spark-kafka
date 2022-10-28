from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

import os

spark = SparkSession.builder\
.appName('joinParquet')\
.getOrCreate()

BASE_PATH = '/data/AGOSTO_2022/20220801'
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


files = list( os.listdir(BASE_PATH) )
files = [ os.path.join(BASE_PATH, file) for file in files ]

print(files)

spark\
    .read\
    .option("multiline", "true")\
    .json(files, schema=SCHEMA)\
    .show(20)