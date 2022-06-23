# to run this script: sudo ~/.local/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 sparkApp2.py

import sys
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

try:
    nodeName = sys.argv[1]
    sparkInputFrom = sys.argv[2]
    sparkOutputTo = sys.argv[3]

    logging.basicConfig(filename="logs/output/spark2.log",\
		format='%(asctime)s %(levelname)s:%(message)s',\
		level=logging.INFO)
    logging.info("node: "+nodeName)
    logging.info("input: "+sparkInputFrom)
    logging.info("output: "+sparkOutputTo)

    
    nodeID = nodeName[1:]
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
        .option('subscribe', sparkInputFrom)\
        .load()

    logging.info("dataframe: ")
    logging.info(lines.isStreaming)
    logging.info("schema: ")
    logging.info(lines.printSchema())

    lines = lines.selectExpr("CAST(value AS STRING)")
    #Split the lines into words
    # words = lines.select(
    #     # explode turns each item in an array into a separate row
    #     explode(
    #         split(lines.value, ' ')
    #     ).alias('word')
    # )

    # # Generate running word count
    # words = words.groupBy('word').count()

    # output to csv file
    output = lines.writeStream \
        .format("csv") \
        .option("path", sparkOutputTo+"/wordcount_output") \
        .option("checkpointLocation", sparkOutputTo+"/wordcount_checkpoint") \
        .start()

    output.awaitTermination(50)
    # output.stop()

except Exception as e:
    logging.error(e)
    sys.exit(1)