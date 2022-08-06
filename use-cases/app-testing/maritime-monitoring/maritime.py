
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

    sparkInputFrom = "maritimeInput"

    logging.basicConfig(filename="logs/output/maritimeSpark.log",\
        format='%(asctime)s %(levelname)s:%(message)s',\
        level=logging.INFO)
    logging.info("node: "+nodeName)
    logging.info("input: "+sparkInputFrom)
    logging.info("output: "+sparkOutputTo)

    
    spark = SparkSession.builder \
        .appName("Maritime Spark") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    # vessel = spark\
    #     .readStream\
    #     .format('socket')\
    #     .option('host', host)\
    #     .option('port', port)\
    #     .load()\
    #     .selectExpr("CAST(value AS STRING)")

    # reading from Kafka source
    vesselDf = spark.readStream\
                .format("kafka")\
                .option("kafka.bootstrap.servers", kafkaNode)\
                .option("subscribe", sparkInputFrom)\
                .option("startingOffsets", "earliest")\
                .load()\
                .selectExpr("CAST(value AS STRING)")

    vesselSchema = StructType( [StructField('MMSI', IntegerType()),\
        StructField('BaseDateTime', TimestampType()),\
        StructField('LAT', DoubleType()),\
        StructField('LON', DoubleType())])

    # parsing dataframe according to provided schema
    def parse_data_from_kafka_message(sdf, schema):
        from pyspark.sql.functions import split
        assert sdf.isStreaming == True, "DataFrame doesn't receive treaming data"
        col = split(sdf['value'], ',') #split attributes to nested array in one Column
        #now expand col to multiple top-level columns
        for idx, field in enumerate(schema): 
            sdf = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))
        return sdf.select([field.name for field in schema])

    vessel = parse_data_from_kafka_message(vesselDf, vesselSchema)
    query = vessel.groupBy(window("BaseDateTime","1 hour"),"MMSI")\
        .agg( (count("MMSI").alias("Number of Updates")))\
            .orderBy("window.start")

    # writing processed dataframe to Kafka sink
    output = query\
        .writeStream\
        .format("kafka")\
        .outputMode("complete")\
        .option("kafka.bootstrap.servers", kafkaNode)\
        .option("topic", sparkOutputTo)\
        .option("checkpointLocation", "logs/output/maritimeCheckpoint")\
        .start()

    # writing the dataframe in a csv file
    # output = query.writeStream \
    # .format("csv") \
    # .option("path", "/tmp/maritime_output") \
    # .option("checkpointLocation", "/tmp/maritime_checkpoint") \
    # .start() 
    
    output.awaitTermination(30)
    output.stop()

except Exception as e:
	logging.error(e)
	sys.exit(1)