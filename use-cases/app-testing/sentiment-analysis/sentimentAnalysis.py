from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from textblob import TextBlob
import time
import sys

def preprocessing(lines):
    words = lines.select(explode(split(lines.value, "t_end")).alias("word"))
    words = words.na.replace('', None)
    words = words.na.drop()
    words = words.withColumn('word', F.regexp_replace('word', r'http\S+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '@\w+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '#', ''))
    words = words.withColumn('word', F.regexp_replace('word', 'RT', ''))
    words = words.withColumn('word', F.regexp_replace('word', ':', ''))
    return words

# text classification
def polarity_detection(text):
    return TextBlob(text).sentiment.polarity
def subjectivity_detection(text):
    return TextBlob(text).sentiment.subjectivity
def text_classification(words):
    # polarity detection
    polarity_detection_udf = udf(polarity_detection, StringType())
    words = words.withColumn("polarity", polarity_detection_udf("word"))
    # subjectivity detection
    subjectivity_detection_udf = udf(subjectivity_detection, StringType())
    words = words.withColumn("subjectivity", subjectivity_detection_udf("word"))
    return words

if __name__ == "__main__":
    try:
        # nodeName = sys.argv[1]
        # nodeID = nodeName[1:]
        # host = "10.0.0."+nodeID
        # port = int(sys.argv[2])

        nodeName = sys.argv[1]
        sparkOutputTo = sys.argv[2]

        nodeID = nodeName[1:]
        host = "10.0.0."+nodeID
        kafkaNode = host + ":9092"

        sparkInputFrom = "topic-0"

        # create Spark session
        spark = SparkSession.builder.appName("TwitterSentimentAnalysis").getOrCreate()
        spark.sparkContext.setLogLevel('ERROR')
        
        # read the tweet data from socket
        # lines = spark.readStream.format("socket").option("host", host).option("port", port).load()
        
        # Create DataFrame representing the stream of input lines from connection to host:port
        lines = spark\
            .readStream\
            .format('kafka')\
            .option('kafka.bootstrap.servers', kafkaNode) \
            .option('subscribe', sparkInputFrom)\
            .load()\
            .selectExpr("CAST(value AS STRING)")

        # Preprocess the data
        words = preprocessing(lines)
        # text classification to define polarity and subjectivity
        words = text_classification(words)

        words = words.repartition(1)
        output = words.writeStream.queryName("all_tweets")\
            .format("csv")\
            .option("path", sparkOutputTo+"/sentiment-analysis-result")\
            .option("checkpointLocation", sparkOutputTo+"/sentiment-analysis-checkpoint")\
            .start()
            # .trigger(processingTime='60 seconds').start()
        
        output.awaitTermination(50)
        output.stop()

    except Exception as e:
        sys.exit(1)