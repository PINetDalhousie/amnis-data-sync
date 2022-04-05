#!/usr/bin/python3
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys
import logging

try:
    # nodeName = sys.argv[1]
    nodeID = "1" #nodeName[1:]
    host = "10.0.0."+nodeID

    port = 12345  #int(sys.argv[2])

    logging.basicConfig(filename="logs/kafka/new12-"+nodeID+".log",\
                            format='%(asctime)s %(levelname)s:%(message)s',\
                            level=logging.INFO)    
    logging.info("node is: "+nodeID)
    logging.info("host: "+host)
    logging.info("port: "+str(port))

    # spark = SparkSession.builder \
    #     .appName("Spark Structured Streaming from Kafka") \
    #     .getOrCreate()

    # spark.sparkContext.setLogLevel('ERROR')
    # sdfRides = spark\
    #     .readStream\
    #     .format('socket')\
    #     .option('host', host)\
    #     .option('port', port)\
    #     .load()\
    #     .selectExpr("CAST(value AS STRING)")

        
    # taxiRidesSchema = StructType([ \
    #     StructField("rideId", LongType()), StructField("isStart", StringType()), \
    #     StructField("endTime", TimestampType()), StructField("startTime", TimestampType()), \
    #     StructField("startLon", FloatType()), StructField("startLat", FloatType()), \
    #     StructField("endLon", FloatType()), StructField("endLat", FloatType()), \
    #     StructField("passengerCnt", ShortType()), StructField("taxiId", LongType()), \
    #     StructField("driverId", LongType())])


    # def parse_data_from_kafka_message(sdf, schema):
    #     from pyspark.sql.functions import split
    #     assert sdf.isStreaming == True, "DataFrame doesn't receive treaming data"
    #     col = split(sdf['value'], ',') #split attributes to nested array in one Column
    #     #now expand col to multiple top-level columns
    #     for idx, field in enumerate(schema): 
    #         sdf = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    #     return sdf.select([field.name for field in schema])

    # sdfRides = parse_data_from_kafka_message(sdfRides, taxiRidesSchema)


    # query = sdfRides.groupBy("driverId").count()


    # # query.writeStream \
    # #     .outputMode("complete") \
    # #     .format("console") \
    # #     .option("truncate", False) \
    # #     .start() \
    # #     .awaitTermination()

    # logging.info(query.printSchema())
    # logging.info("Streaming DataFrame : " + query.isStreaming)

    # query.writeStream.outputMode("append").format("csv").option("path", "home/monzurul/Desktop/sparkOutput").start().awaitTermination()

except Exception as e:
	logging.error(e)
	sys.exit(1)