# In this file, we load our trained machine learning model, stream data from a kafka topic and have the model
# make predictions on the streamed data. We then save the predictions in a csv file. Alternatively, we can store the
# predictions in a Kafka topic.

# This file contains the 3rd and 4th sections of our application
# 
# 3 - Constructing dataframe from data streamed from a Kafka topic
# 4 - Making predictions on streaming dataframe using trained model

import pyspark
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LinearSVC

import sys
import logging

try:
        nodeName = sys.argv[1]
        sparkOutputTo = sys.argv[2]

        nodeID = "1" #nodeName[1:]
        host = "10.0.0."+nodeID
        kafkaNode = host + ":9092"

        sparkInputFrom = "fraudInput"

        logging.basicConfig(filename="logs/output/fraudSpark.log",\
        format='%(asctime)s %(levelname)s:%(message)s',\
        level=logging.INFO)
        logging.info("node: "+nodeName)
        logging.info("input: "+sparkInputFrom)
        logging.info("output: "+sparkOutputTo)


        trainedModel = "use-cases/app-testing/fraud-detection/trainedmodel"

        predpath = "logs/output/fraud_detection/prediction_output"
        checkpath = "logs/output/fraud_detection/fraudCheckpoint"
        # kafkatopic = "fraudInTopic"

        spark = SparkSession.builder.appName("Fraud Detections").getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")


        # 3 - Constructing dataframe from data streamed from a Kafka topic

        # We read the csv file loaded into a kafka topic
        stream = spark.readStream.format("kafka").option("startingOffsets", "earliest")\
                .option("kafka.bootstrap.servers", kafkaNode)\
                .option("subscribe", sparkInputFrom).load()

        #Here we change the value and key columns to the schema we desire, which is the same as the original dataset
        newStream = stream.select(col("value").cast("string")).alias("csv").select("csv.*")


        df = newStream.selectExpr(\
                                "split(value,',')[0] as step" \
                                ,"split(value,',')[1] as type" \
                                ,"split(value,',')[2] as amount" \
                                ,"split(value,',')[3] as nameOrig" \
                                ,"split(value,',')[4] as oldbalanceOrg" \
                                ,"split(value,',')[5] as newbalanceOrig" \
                                ,"split(value,',')[6] as nameDest" \
                                ,"split(value,',')[7] as oldbalanceDest"\
                                ,"split(value,',')[8] as newbalanceDest"\
                                ,"split(value,',')[9] as isFraud" \
                                ,"split(value,',')[10] as isFlaggedFraud" \
                                )


        # Here we sanitize some columns and drop some columns we do not need.
        df = df.dropna()


        df = df.drop("isFlaggedFraud", "step")

        columnsToSanitize = ["isFraud","amount", "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest"]

        for column in columnsToSanitize:

                df = df.withColumn(column, col(column).cast("double"))


        # 4 - Making predictions on streaming dataframe using trained model

        # We load the trained Linear SVM model
        model = PipelineModel.load(trainedModel)

        # We have the model make predictions on the streaming dataframe and store the result in a csv file or in a kafka topic.
        # You can comment out the storage method you are not using. Both methods are provided below.
        predictions = model.transform(df)

        # CSV as output sink
        # predictions.select("isFraud", "prediction").writeStream.format("csv").outputMode("append")\
        #                  .option("path", predpath).option("header", True)\
        #                  .option("checkpointLocation", checkpath)\
        #                  .start()

        # Alternatively, Kafka topic as output sink
        predictions = predictions.select(concat(lit("isFraud:  "),"isFraud", lit("  "), \
                lit("prediction:  "), "prediction").alias("value"))

        output = predictions.writeStream.format("kafka").outputMode("append")\
                .option("checkpointLocation", checkpath)\
                .option("kafka.bootstrap.servers", kafkaNode)\
                .option("topic", sparkOutputTo)\
                .start()
                
        output.awaitTermination(30)                
        output.stop()


except Exception as e:
	logging.error(e)
	sys.exit(1)

