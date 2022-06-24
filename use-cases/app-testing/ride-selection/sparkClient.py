
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import expr
from pyspark.sql.functions import avg
from pyspark.sql.functions import window

import sys

# data parsing according to schema
def parse_data_from_kafka_message(sdf, schema):
    from pyspark.sql.functions import split
    assert sdf.isStreaming == True, "DataFrame doesn't receive treaming data"
    col = split(sdf['value'], ',') #split attributes to nested array in one Column
    #now expand col to multiple top-level columns
    for idx, field in enumerate(schema): 
        sdf = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return sdf.select([field.name for field in schema])

try:
    # spark hostnode
    nodeName = sys.argv[1]
    nodeID = nodeName[1:]
    host = "10.0.0."+nodeID
    kafkaNode = host + ":9092"
    
    # creating a spark session
    spark = SparkSession.builder \
        .appName("Spark Structured Streaming from Kafka") \
        .getOrCreate()
    
    # to ignore logs other than the error ones
    spark.sparkContext.setLogLevel('ERROR')
    
    # creating dataframe to receive data from Kafka topic
    sdfRides = spark\
        .readStream\
        .format('kafka')\
        .option('kafka.bootstrap.servers', kafkaNode) \
        .option('subscribe', 'topic-0')\
        .load().selectExpr("CAST(value AS STRING)")

    # schema to structure data    
    taxiRidesSchema = StructType([ \
        StructField("rideId", LongType()), StructField("isStart", StringType()), \
        StructField("endTime", TimestampType()), StructField("startTime", TimestampType()), \
        StructField("startLon", FloatType()), StructField("startLat", FloatType()), \
        StructField("endLon", FloatType()), StructField("endLat", FloatType()), \
        StructField("passengerCnt", ShortType()), StructField("taxiId", LongType()), \
        StructField("driverId", LongType())])
 
    sdfRides = parse_data_from_kafka_message(sdfRides, taxiRidesSchema)

    # query = sdfRides.groupBy("driverId").count()

    # starting the query, storing output in csv file
    query = sdfRides.writeStream.queryName("uber_ride_query")\
        .outputMode("append").format("csv")\
        .option("path", "../../logs/output/uber_output")\
        .option("checkpointLocation", "../../logs/output/uber_checkpoint")\
        .trigger(processingTime='60 seconds').start()
    query.awaitTermination(30)
    query.stop()

except Exception as e:
	sys.exit(1)