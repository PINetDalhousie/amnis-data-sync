# command to run this script: sudo ~/.local/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 sparkApp1.py

import sys
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

try:
    nodeName = sys.argv[1]
    sparkOutputTo = sys.argv[2]

    sparkInputFrom = "inTopic"

    logging.basicConfig(filename="logs/output/spark1.log",\
		format='%(asctime)s %(levelname)s:%(message)s',\
		level=logging.INFO)
    logging.info("node: "+nodeName)
    logging.info("input: "+sparkInputFrom)
    logging.info("output: "+sparkOutputTo)
    
    nodeID = "2" #nodeName[1:]
    host = "10.0.0."+nodeID

    spark = SparkSession\
        .builder\
        .appName("StructuredNetworkWordCount"+nodeID)\
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    # kafkaNode = host + ":9092"
    kafkaNode = "10.0.0.2:9092"
    logging.info("Connected at Broker: "+kafkaNode)
    # kafkaNode = "10.0.0.1:9092,10.0.0.2:9092"

    # Create DataFrame representing the stream of input lines from connection to host:port
    lines = spark\
        .readStream\
        .format('kafka')\
        .option('kafka.bootstrap.servers', kafkaNode) \
        .option("startingOffsets", "earliest")\
        .option("failOnDataLoss", False)\
        .option('subscribe', sparkInputFrom)\
        .load().selectExpr("CAST(value AS STRING)")

    #Use of Kafka topic as a sink
    output = lines.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaNode) \
    .option("topic", sparkOutputTo) \
    .option("checkpointLocation", "logs/output/wordcount_checkpoint_intermediate") \
    .start()
    output.awaitTermination()
    
    # Generate running word count
    # wordCounts = words.groupBy('word').count()

    # output = words.writeStream \
    #     .format("csv") \
    #     .option("path", sparkOutputTo+"wordcount_output") \
    #     .option("checkpointLocation", sparkOutputTo+"wordcount_checkpoint") \
    #     .start()

    
    # output.awaitTermination(30)
    # output.stop()

except Exception as e:
	logging.error(e)
	sys.exit(1)


