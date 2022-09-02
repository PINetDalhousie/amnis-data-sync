# command to run this script: sudo ~/.local/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 sparkApp1.py

import sys
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

try:
    # nodeName = sys.argv[1]
    # sparkOutputTo = sys.argv[2]

    sparkInputFrom = "inTopic1,inTopic2"
    sparkOutputTo = "outTopic1"

    logging.basicConfig(filename="logs/output/spark1.log",\
		format='%(asctime)s %(levelname)s:%(message)s',\
		level=logging.INFO)
    
    nodeID = "2" #nodeName[1:]
    host = "10.0.0."+nodeID

    logging.info("node: "+nodeID)
    logging.info("input: "+sparkInputFrom)
    logging.info("output: "+sparkOutputTo)

    spark = SparkSession\
        .builder\
        .appName("StructuredNetworkWordCount"+nodeID)\
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    kafkaNode = host + ":9092"

    # Create DataFrame representing the stream of input lines from connection to host:port
    lines = spark\
        .readStream\
        .format('kafka')\
        .option('kafka.bootstrap.servers', kafkaNode) \
        .option("startingOffsets", "earliest")\
        .option("failOnDataLoss", False)\
        .option('subscribe', sparkInputFrom)\
        .load().selectExpr("CAST(value AS STRING)")

    # Separating the data to get the file number on one side and the words on the other
    split_result = split("value", " File: ")
    lines = lines.select(split_result.getItem(0).alias('value'), split_result.getItem(1).alias('file'))

    # Separating the data to get the topic name on one side and the words on the other
    split_result = split("value", " Topic: ")
    lines = lines.select(split_result.getItem(0).alias('value'), split_result.getItem(1).alias('topic'), 'file')


    # Splitting the sentences into one word per row along with the file it originated from
    words = lines.select('topic', 'file',\
            explode(
                split(lines.value, ' ')
                ).alias('word')
            )

    # # Getting the word frequency count per file
    result = words.groupBy('topic', 'file', 'word')\
        .agg( approx_count_distinct('word').alias('frequency') ).orderBy('topic','file')

    # # Formatting the result for storage into a kafka topic
    result = result.select( concat( lit('Topic: '), 'topic', lit(' File: '),\
         'file', lit('  Word: '), 'word', lit('  Frequency: '), 'frequency' ).alias('value') )

    # # Use of Kafka topic as a sink
    output = result.writeStream \
    .format("kafka") \
    .outputMode('complete')\
    .option("kafka.bootstrap.servers", kafkaNode) \
    .option("topic", sparkOutputTo) \
    .option("checkpointLocation", "logs/output/wordCount_checkpoint_intermediate") \
    .start()

    output.awaitTermination()
    # output.awaitTermination(30)
    # output.stop()

except Exception as e:
	logging.error(e)
	sys.exit(1)


