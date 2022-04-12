#!/usr/bin/python3

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import expr
from pyspark.sql.functions import avg
from pyspark.sql.functions import window

import sys
import logging

try:
    # nodeName = "h1" 
    # nodeID = "1" 
    # host = "10.0.0."+nodeID
    # port = 12345 

    nodeName = sys.argv[1]
    nodeID = nodeName[1:]
    host = "10.0.0."+nodeID
    port = int(sys.argv[2])

    logging.basicConfig(filename="logs/kafka/"+"nodes:1_mSize:fixed,10_mRate:1.0_topics:1_replication:1"+"/cons/client-"+nodeID+".log",\
            format='%(asctime)s %(levelname)s:%(message)s',\
            level=logging.INFO)
    logger = logging.getLogger(__name__)

    logging.info("node is: "+nodeID)
    logging.info("host: "+host)
    logging.info("port: "+str(port))

    logger.setLevel(logging.DEBUG)
    logger.debug("1 - DEBUG - Print the message")

    
    spark = SparkSession.builder \
        .appName("Spark Structured Streaming from Kafka") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')
    sdfRides = spark\
        .readStream\
        .format('socket')\
        .option('host', host)\
        .option('port', port)\
        .load()\
        .selectExpr("CAST(value AS STRING)")
    logging.info(sdfRides)

        
    taxiRidesSchema = StructType([ \
        StructField("rideId", LongType()), StructField("isStart", StringType()), \
        StructField("endTime", TimestampType()), StructField("startTime", TimestampType()), \
        StructField("startLon", FloatType()), StructField("startLat", FloatType()), \
        StructField("endLon", FloatType()), StructField("endLat", FloatType()), \
        StructField("passengerCnt", ShortType()), StructField("taxiId", LongType()), \
        StructField("driverId", LongType())])


    def parse_data_from_kafka_message(sdf, schema):
        from pyspark.sql.functions import split
        assert sdf.isStreaming == True, "DataFrame doesn't receive treaming data"
        col = split(sdf['value'], ',') #split attributes to nested array in one Column
        #now expand col to multiple top-level columns
        for idx, field in enumerate(schema): 
            sdf = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))
        return sdf.select([field.name for field in schema])

    sdfRides = parse_data_from_kafka_message(sdfRides, taxiRidesSchema)

    query = sdfRides.groupBy("driverId").count()

    # writing the aggregated spark dataframe
    # query.writeStream \
    #     .outputMode("complete") \
    #     .format("console") \
    #     .option("truncate", False) \
    #     .start() \
    #     .awaitTermination()

    # writing the dataframe in a csv file
    sdfRides.writeStream \
    .format("csv") \
    .option("path", "/tmp/filesink_output") \
    .option("checkpointLocation", "/tmp/checkpoint/filesink_checkpoint") \
    .start() \
    .awaitTermination()

except Exception as e:
	logging.error(e)
	sys.exit(1)