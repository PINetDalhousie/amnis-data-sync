# This file is going to be our maritime application. At this point, this file reads AIS data from a kafka topic,
# does some processing, then stores the result in a kafka topic with a particular json format.

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from time import sleep
from json import dumps
from kafka import KafkaProducer
import sys
import logging

try:
    nodeName = sys.argv[1]
    sparkOutputTo = sys.argv[2]

    nodeID = nodeName[1:]
    host = "10.0.0."+nodeID
    kafkaNode = host + ":9092"

    sparkInputFrom = "maritimeInput"

    logging.basicConfig(filename="logs/output/maritimeSpark.log",\
        format='%(asctime)s %(levelname)s:%(message)s',\
        level=logging.INFO)
    logging.info("node: "+nodeName)
    logging.info("input: "+sparkInputFrom)
    logging.info("output: "+sparkOutputTo)

    
    # Some basic spark setup
    spark = SparkSession.builder \
            .appName("Spark Structured Streaming for Maritime") \
            .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    # Now, we read in the AIS data from the kafka topic

    vessel = spark\
            .readStream\
            .format('kafka')\
            .option('kafka.bootstrap.servers', kafkaNode)\
            .option('subscribe', sparkInputFrom)\
            .option("startingOffsets", "earliest")\
            .load()\
            .selectExpr("CAST(value AS STRING)")

    # Since the data we read is in json format, we extract the fields we want and use those to make new columns

    vessel = vessel.select(json_tuple(col("value"),"mmsi", "lat", "lon")) \
            .toDF("mmsi", "lat", "lon")

    
    # Here we determine the largest and smallest reported longitude of each ship
    
    query = vessel.groupBy("mmsi")\
            .agg( max("lon"), min("lon") )


    # Here we get the dataframe columns, which we will use in creating our json string

    columns = []

    for column in query.columns:
        columns.append(column)

    # Convert the result of our query to json format

    query = query.select( to_json( struct("*")).alias("value") )

    
    # Here we create our required json schema

    def construct_json_string(columns):

        jsonString = '{"schema":{"type":"struct","optional":false,"version":1,"fields":['

        i = 0

        for column in columns:

            jsonString += '{"field":"' + column + '","type":"string","optional":true}'

            if i < len(columns) - 1:
                
                jsonString += ','
                i+=1
        
        jsonString += ']},"payload":'

        return jsonString

    # Getting our json schema string

    jsonString = construct_json_string(columns)
    
    i = 0

    # Here we send each of our dataframe row to the output kafka topic. Each row is first formatted as follows
    #
    # Schema followed by the row data
    #
    # Each formatted row is then sent to the kafka topic

    class RowPrinter:

        # This part is just left as the default

        def open(self, partition_id, epoch_id):
            return True

        # This is where we format and send each row to the kafka topic

        def process(self, row):

            global i
            global jsonString

            if i == 0:
                producer = KafkaProducer(bootstrap_servers=[kafkaNode])#,\
                        # value_serializer=lambda x:\
                        # dumps(x).encode('utf-8'))
                i+= 10
                jsonString += str(row[0]) + "}"
                jsonByte = bytes(jsonString, 'utf-8')

                producer.send(sparkOutputTo, jsonByte)
        
        # This part is also left as the default

        def close(self, error):
            print("Closed with error: %s" % str(error))

    # Sending the dataframe to the kafka topic row by row, in the desired json format

    output =query\
        .writeStream\
        .outputMode("complete")\
        .foreach(RowPrinter())\
        .start()\
        .awaitTermination()
    
    output.awaitTermination(30)
    output.stop()

except Exception as e:
	logging.error(e)
	sys.exit(1)