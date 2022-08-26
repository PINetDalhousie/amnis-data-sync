# to run this script: sudo ~/.local/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 sparkApp2.py

import sys
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


try:
    nodeName = sys.argv[1]
    sparkOutputTo = sys.argv[2]

    sparkInputFrom = "outTopic"
    
    logging.basicConfig(filename="logs/output/spark2.log",\
		format='%(asctime)s %(levelname)s:%(message)s',\
		level=logging.INFO)
    logging.info("node: "+nodeName)
    logging.info("input: "+sparkInputFrom)
    logging.info("output: "+sparkOutputTo)

    
    nodeID = "2" #nodeName[1:]
    host = "10.0.0."+nodeID
    kafkaNode = host + ":9092"
    # kafkaNode = "10.0.0.1:9092,10.0.0.2:9092"

    spark = SparkSession\
        .builder\
        .appName("StructuredNetworkWordCount"+nodeID)\
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    # Create DataFrame representing the stream of input lines from connection to host:port
    lines = spark\
        .readStream\
        .format('kafka')\
        .option('kafka.bootstrap.servers', kafkaNode) \
        .option("startingOffsets", "earliest")\
        .option("failOnDataLoss", False)\
        .option('subscribe', sparkInputFrom)\
        .load()\
        .selectExpr("CAST(value AS STRING)")

    logging.info("dataframe: ")
    logging.info(lines.isStreaming)
    logging.info("schema: ")
    logging.info(lines.printSchema())

    splitBy = split('value', 'rrrr')
    lines = lines.select(splitBy.getItem(0).alias('value'),\
        splitBy.getItem(1).alias('fileID'))

    # #Split the lines into words
    # # explode turns each item in an array into a separate row
    words = lines.select(
         explode(
             split(lines.value, ' ')
         ).alias('word'), "fileID"
     )

    # # # Generate running word count
    words = words.groupBy('word', 'fileID').count()

    words = words.select( concat(lit('word: '), 'word', lit("  count: "), 'count' ,\
          lit("  File: "), 'fileID').alias("value") )
    
    # output to csv file
    # output = words.writeStream \
    #     .outputMode("complete")\
    #     .format("csv") \
    #     .option("path", sparkOutputTo+"/wordcount_output") \
    #     .option("checkpointLocation", sparkOutputTo+"/wordcount_checkpoint") \
    #     .start()

    output = words.writeStream \
    .outputMode("complete")\
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaNode) \
    .option("topic", sparkOutputTo) \
    .option("checkpointLocation", "logs/output/wordcount_checkpoint_final") \
    .start()

    output.awaitTermination()
    # output.awaitTermination(80)
    # output.stop()

except Exception as e:
    logging.error(e)
    sys.exit(1)