
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

if __name__ == "__main__":
    dataDir = "data/paysim/"  #sys.argv[1]

    # create Spark session
    spark = SparkSession.builder.appName("Fraud Detection").getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
      
    dataSchema = StructType([ \
        StructField("step", IntegerType()), StructField("type", StringType()), \
        StructField("amount",DoubleType()),StructField("nameOrig",StringType()), \
        StructField("oldbalanceOrg",DoubleType()),StructField("newbalanceOrig",DoubleType()), \
        StructField("nameDest",StringType()),StructField("oldbalanceDest",DoubleType()), \
        StructField("newbalanceDest",DoubleType())])

        
    streaming = (
        spark.readStream.schema(dataSchema)
        .option("maxFilesPerTrigger", 1)
        .csv(dataDir)   #.csv("data/paysim/")
    )

    dest_count = streaming.groupBy("nameDest").count().orderBy(F.desc("count"))

    # writing the dataframe in a csv file
    dest_count.writeStream \
    .format("csv") \
    .option("path", "/tmp/filesink_output") \
    .option("checkpointLocation", "/tmp/checkpoint/filesink_checkpoint") \
    .start() \
    .awaitTermination()

    # activityQuery = (
    #     dest_count.writeStream.queryName("dest_counts")
    #     .format("memory")
    #     .outputMode("complete")
    #     .start()
    # )

    # include this in production
    # activityQuery.awaitTermination()

    # import time

    # for x in range(50):
    #     _df = spark.sql(
    #         "SELECT * FROM dest_counts WHERE nameDest != 'nameDest' AND count >= 2"
    #     )
    #     if _df.count() > 0:
    #         _df.show(10)
    #     time.sleep(0.5)

    