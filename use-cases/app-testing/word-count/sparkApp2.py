# to run this script: sudo ~/.local/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 sparkApp2.py

import sys
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


try:
    # nodeName = sys.argv[1]
    # sparkOutputTo = sys.argv[2]

    sparkInputFrom = "outTopic1"
    sparkOutputTo = "outTopic2"
    
    logging.basicConfig(filename="logs/output/spark2.log",\
		format='%(asctime)s %(levelname)s:%(message)s',\
		level=logging.INFO)
    
    nodeID = "2" #nodeName[1:]
    host = "10.0.0."+nodeID
    kafkaNode = host + ":9092"
    # kafkaNode = "10.0.0.1:9092,10.0.0.2:9092"
    logging.info("node: "+nodeID)
    logging.info("input: "+sparkInputFrom)
    logging.info("output: "+sparkOutputTo)

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

    # getting the frequency
    splitByFrequency = split('value', ' Frequency: ')
    lines = lines.select(splitByFrequency.getItem(0).alias('value'), splitByFrequency.getItem(1).alias('frequency'))

    # getting the words
    splitByWord = split('value', ' Word: ')
    lines = lines.select(splitByWord.getItem(0).alias('value'),\
        splitByWord.getItem(1).alias('word'),\
            'frequency')

    # getting the file
    splitByFile = split('value', ' File: ')
    lines = lines.select(splitByFile.getItem(0).alias('value'),\
         splitByFile.getItem(1).alias('file'),\
            'word', 'frequency')

    # getting the topic
    splitByTopic = split('value', 'Topic: ')
    lines = lines.select(splitByTopic.getItem(1).alias('topic'),\
                        'file', 'word', 'frequency')

    averageWords = lines.groupBy('topic')\
        .agg( (sum('frequency')/approx_count_distinct('file')).alias('avgNumberOfFiles'))
    print(averageWords.printSchema())

    result = averageWords.select( concat( lit('Topic: '), 'topic',\
        lit(' Avg number of words for this topic: '), 'avgNumberOfFiles').alias('value') )
    print(result.printSchema())

    output = result.writeStream \
    .format("kafka") \
    .outputMode("complete")\
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