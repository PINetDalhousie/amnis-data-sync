#to run with kafka: ~/.local/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 <file_name>


import imp
from os import truncate
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import sys
import logging

nodeName = sys.argv[1]
nodeID = nodeName[1:]
host = "10.0.0."+nodeID
port = int(sys.argv[2])

spark = SparkSession.builder.appName("maritimeSpark")\
    .master("local[4]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

logging.basicConfig(filename="extra.log",\
            format='%(asctime)s %(levelname)s:%(message)s',\
            level=logging.INFO)

streamStart = spark\
    .readStream.format('socket')\
        .option('host', host).option('port', port)\
            .load()#\
                # .selectExpr("CAST (value as STRING)")

vesselSchema = StructType( [StructField('MMSI', IntegerType(), True),\
    StructField('BaseDateTime', TimestampType(), True),\
        StructField('LAT', DoubleType(), True),\
            StructField('LON', DoubleType(), True)])

def parse_data_from_kafka_message(sdf, schema):
    from pyspark.sql.functions import split
    assert sdf.isStreaming == True, "DataFrame doesn't receive treaming data"
    col = split(sdf['value'], ',') #split attributes to nested array in one Column
    #now expand col to multiple top-level columns
    for idx, field in enumerate(schema): 
        sdf = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return sdf.select([field.name for field in schema])

vesselDf = parse_data_from_kafka_message(streamStart, vesselSchema)

query = vesselDf.select("LAT").where("MMSI > 18")


ds_file_sink = query.writeStream\
    .format("csv") \
        .option("path", "/home/ubuntu/Documents/fileSinkOutputDir")\
            .option("checkpointLocation", "/home/ubuntu/Documents/maritimeCheckpoint")\
    .start()