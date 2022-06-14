# command to run this script: sudo ~/.local/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 wordcount.py

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

try:
    nodeName = sys.argv[1]
    nodeID = nodeName[1:]
    host = "10.0.0."+nodeID

    spark = SparkSession\
        .builder\
        .appName("StructuredNetworkWordCount")\
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    kafkaNode = host + ":9092"
    # Create DataFrame representing the stream of input lines from connection to host:port
    lines = spark\
        .readStream\
        .format('kafka')\
        .option('kafka.bootstrap.servers', kafkaNode) \
        .option('subscribe', 'topic-0')\
        .load().selectExpr("CAST(value AS STRING)")

    #Split the lines into words
    words = lines.select(
        # explode turns each item in an array into a separate row
        explode(
            split(lines.value, ' ')
        ).alias('word')
    )

    # Generate running word count
    # wordCounts = words.groupBy('word').count()

    output = words.writeStream \
        .format("csv") \
        .option("path", "logs/output/wordcount_output") \
        .option("checkpointLocation", "logs/output/wordcount_checkpoint") \
        .start()

    output.awaitTermination(30)
    output.stop()

except Exception as e:
	# logging.error(e)
	sys.exit(1)


