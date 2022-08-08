# to run: sudo /home/monzurul/.local/lib/python3.8/site-packages/pyspark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 personSpark.py

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

import sys
import logging

try:
        nodeName = sys.argv[1]
        sparkOutputTo = sys.argv[2]

        nodeID = nodeName[1:]
        host = "10.0.0."+nodeID
        kafkaNode = host + ":9092"

        sparkInputFrom = "inTopic"

        logging.basicConfig(filename="logs/output/personSpark.log",\
		format='%(asctime)s %(levelname)s:%(message)s',\
		level=logging.INFO)
        logging.info("node: "+nodeName)
        logging.info("input: "+sparkInputFrom)
        logging.info("output: "+sparkOutputTo)

        spark = SparkSession.builder \
                .appName("Person Spark") \
                .getOrCreate()

        spark.sparkContext.setLogLevel('ERROR')

        df = spark.readStream\
                .format("kafka")\
                .option("kafka.bootstrap.servers", kafkaNode)\
                .option("subscribe", sparkInputFrom)\
                .option("startingOffsets", "earliest")\
                .load()
        
        logging.info("read dataframe: ")
        logging.info(df.isStreaming)
        logging.info("read schema: ")
        logging.info(df.printSchema())

        personStringDF = df.selectExpr("CAST(value AS STRING)")

        personDF = personStringDF.dropna()
        
        personDF\
        .writeStream\
        .format("kafka")\
        .outputMode("append")\
        .option("kafka.bootstrap.servers", kafkaNode)\
        .option("topic", sparkOutputTo)\
        .option("checkpointLocation", "logs/output/personCheck")\
        .start()\
        .awaitTermination(30)\
        .stop()

except Exception as e:
        logging.error(e)
        sys.exit(1)
