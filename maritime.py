
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

import sys
import logging

try:
    # nodeName = "h1" 
    # nodeID = "1" 
    # host = "10.0.0."+nodeID
    # port = 12345

    nodeName = sys.argv[1]
    nodeID = nodeName[1:]
    host = "10.0.0."+nodeID
    port = int(sys.argv[2])

    # logging.basicConfig(filename="logs/kafka/"+"nodes:1_mSize:fixed,10_mRate:1.0_topics:1_replication:1"+"/cons/client-"+nodeID+".log",\
    #         format='%(asctime)s %(levelname)s:%(message)s',\
    #         level=logging.INFO)
    # logger = logging.getLogger(__name__)

    # logging.info("node is: "+nodeID)
    # logging.info("host: "+host)
    # logging.info("port: "+str(port))

    # logger.setLevel(logging.DEBUG)
    # logger.debug("1 - DEBUG - Print the message")

    
    spark = SparkSession.builder \
        .appName("Spark Structured Streaming for Maritime") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')
    vessel = spark\
        .readStream\
        .format('socket')\
        .option('host', host)\
        .option('port', port)\
        .load()\
        .selectExpr("CAST(value AS STRING)")
    # logging.info(sdfRides)

    schema1 = StructType( [StructField('MMSI', IntegerType()),\
    StructField('BaseDateTime', TimestampType()),\
        StructField('LAT', DoubleType()),\
            StructField('LON', DoubleType())])


    def parse_data_from_kafka_message(sdf, schema):
        from pyspark.sql.functions import split
        assert sdf.isStreaming == True, "DataFrame doesn't receive treaming data"
        col = split(sdf['value'], ',') #split attributes to nested array in one Column
        #now expand col to multiple top-level columns
        for idx, field in enumerate(schema): 
            sdf = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))
        return sdf.select([field.name for field in schema])

    vessel = parse_data_from_kafka_message(vessel, schema1)
    query = vessel.groupBy(window("BaseDateTime","1 hour"),"MMSI")\
        .agg( (count("MMSI").alias("Number of Updates")))\
            .orderBy("window.start")


    # writing the dataframe in a csv file
    output = query.writeStream \
    .format("csv") \
    .option("path", "/tmp/maritime_output") \
    .option("checkpointLocation", "/tmp/maritime_checkpoint") \
    .start() 
    
    output.awaitTermination(30)
    output.stop()

except Exception as e:
	# logging.error(e)
	sys.exit(1)