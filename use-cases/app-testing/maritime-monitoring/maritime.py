# This file is going to be our maritime application. At this point, this file reads AIS data from a kafka topic,
# does some processing, then stores the result in a kafka topic with a particular json format.

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from kafka import KafkaProducer

import sys
import json
import logging

try:
    # nodeName = sys.argv[1]
    # sparkOutputTo = sys.argv[2]

    # nodeID = nodeName[1:]

    sparkInputFrom = "maritimeInput"
    sparkOutputTo = "maritimeOutput"

    nodeID = "1"
    host = "10.0.0."+nodeID
    kafkaNode = host + ":9092"

    logging.basicConfig(filename="logs/output/maritimeSpark.log",\
        format='%(asctime)s %(levelname)s:%(message)s',\
        level=logging.INFO)
    logging.info("node: "+nodeID)
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
            .option("failOnDataLoss", False)\
            .load()\
            .selectExpr("CAST(value AS STRING)")
    logging.info("Streaming: "+str(vessel.isStreaming))
    logging.info("Schema: "+str(vessel.printSchema))

    # Since the data we read is in json format, we extract all the fields and use them to make new columns

    vessel = vessel.select(json_tuple(col("value"),"class", "device", "repeat", "mmsi", "type", "lat", "lon",\
            "scaled", "status", "status_text", "turn", "speed", "accuracy", "course", "heading", "second", \
            "manuever", "raim", "radio", "reserved", "regional", "cs", "display", "dsc", "band", "msg22",\
            "imo", "ais_version", "callsign", "shipname", "shiptype", "shiptype_text", "to_bow", "to_stern", \
            "to_port", "to_starboard", "epfd", "epfd_text", "eta", "draught", "destination", "dte", \
            "aid_type", "aid_type_text", "name", "off_position", "virtual_aid"))\
            \
            .toDF("class", "device", "repeat", "mmsi", "type", "lat", "lon",\
            "scaled", "status", "status_text", "turn", "speed", "accuracy", "course", "heading", "second", \
            "manuever", "raim", "radio", "reserved", "regional", "cs", "display", "dsc", "band", "msg22",\
            "imo", "ais_version", "callsign", "shipname", "shiptype", "shiptype_text", "to_bow", "to_stern", \
            "to_port", "to_starboard", "epfd", "epfd_text", "eta", "draught", "destination", "dte", \
            "aid_type", "aid_type_text", "name", "off_position", "virtual_aid")

    vessel = vessel.withColumn("timestamp", current_timestamp())

    # Getting the columns with our required information. We use messages of type 5 as other messages types have null values in these columns
    vessel = vessel.filter( col("type") == 5).select("timestamp", "destination", "shiptype","shiptype_text", "mmsi")

    # This is our query
    # We get the types of ships at each destination each minute, along with the number and names of ships
    query = vessel.groupBy(window("timestamp", "1 minute"), "destination", "shiptype")\
            .agg(approx_count_distinct("mmsi").alias("numberOfShips"),\
            collect_set("mmsi").alias("shipIDs"))

    logging.info("Query Streaming: "+str(query.isStreaming))
    logging.info("Query Schema: "+str(query.printSchema))


    # Here we get the dataframe columns, which we will use in creating our json string
    columns = []

    for column in query.columns:
        columns.append(column)

    # Convert the result of our query to json format
    query = query.select( to_json( struct("*")).alias("value") )

    
    #Here we create our required json schema
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
    kafkaNode2 = "10.0.0.2:9092"
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
                producer = KafkaProducer(bootstrap_servers=[kafkaNode2])#,\
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
    # Our choice of output mode is worth elaborating
    # We have 3 choices for output mode: complete, update and append. Complete does not work with this query
    # as we send data row by row, and there are no streaming aggregations on the row. 
    # Update and append both work. However, we chose append as it aligns well with our logic, in that we want
    # to send data row by row as we generate each new transformed row. Also, we wanted to avoid any possible issues
    # by using update, such as a row repeating in part of our output, but not being sent due to matching a previous
    # row
    output = query\
            .writeStream\
            .outputMode("complete")\
            .foreach(RowPrinter())\
            .start()\
            .awaitTermination()
    
    # # output.stop()

except Exception as e:
	logging.error(e)
	sys.exit(1)