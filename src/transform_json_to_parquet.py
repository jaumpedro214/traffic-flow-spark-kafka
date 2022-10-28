
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

import os

spark = SparkSession.builder\
.appName('transformParquet')\
.getOrCreate()

# Get all JSON files from the path
# /data/AGOSTO_2022/20220801/20220801_00.json
BASE_PATH = '/data/AGOSTO_2022/'

WRITE_PATH = '/data/AGOSTO_2022_PARQUET/'

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

# Get all the folders from the base path
folders = [os.path.join(BASE_PATH, f) for f in os.listdir(BASE_PATH)]
print(folders)

# Get all json files from the folders
json_files = {
    folder: [os.path.join(folder, f) for f in os.listdir(folder)]
    for folder in folders
}

print(json_files)
print(len(json_files))

# Transform all files into parquet

for folder, files in json_files.items():
    
    print(
        f"""
        
        Transforming files from {folder}
        Total files: {len(files)}

        """
    )
    
    spark\
    .read\
    .option("multiline", "true")\
    .json(files, schema=SCHEMA)\
    .write\
    .mode('overwrite')\
    .parquet(os.path.join(WRITE_PATH, folder.split('/')[-1]))

spark.stop()

# Command to run the script
# spark-submit --deploy-mode client --master spark://spark:7077 --driver-memory 2G --executor-memory 2G transform_json_to_parquet.py

# Unprotect the folder
# sudo chmod -R 777 data/ 