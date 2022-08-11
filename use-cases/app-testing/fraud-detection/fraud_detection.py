# to run this file sudo /home/monzurul/Desktop/amnis-data-sync/spark/pyspark/bin/spark-submit fraud_detection.py

# This file trains a machine learning model on a portion of our fraud detection dataset. The trained model
# is then saved, ready to be loaded and used to make predictions in the second file: fraud_predicting.py
#
# This file contains the first 2 sections of our application
#
# 1 - Preparing the dataframe used for model training
# 2 - Training model on dataframe


import pyspark
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LinearSVC

import sys

nodeName = sys.argv[1]
sparkOutputTo = sys.argv[2]


dfpath =  "use-cases/app-testing/fraud-detection/training.csv"

#Basic spark session construction and logging setting
spark = SparkSession.builder.master("local[4]").appName("Fraud Detections").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 1 - Preparing the dataframe used for model training

# We will use this schema when reading the csv file containing the dataset
myschema = StructType([
        StructField("step", IntegerType(), True),
        StructField("type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("nameOrig", StringType(), True),
        StructField("oldbalanceOrg", DoubleType(), True),
        StructField("newbalanceOrig", DoubleType(), True),
        StructField("nameDest", StringType(), True),
        StructField("oldbalanceDest", DoubleType(), True),
        StructField("newbalanceDest", DoubleType(), True),
        StructField("isFraud", IntegerType(), True),
        StructField("isFlaggedFraud", IntegerType(), True)
        ])



df2 = spark.read.schema(myschema).csv(dfpath)

# Here we drop the columns we do not need and sanitize the dataframe

df2 = df2.drop("isFlaggedFraud", "step")

columnsToSanitize = ["isFraud","amount", "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest"]

for column in columnsToSanitize:

            df2 = df2.withColumn(column, col(column).cast("double"))


# Here we try to make our dataframe more balanced in terms of the number of frauds and not frauds
fraud = df2.filter(df2.isFraud == 1)
not_fraud = df2.filter(df2.isFraud == 0)


not_fraud = not_fraud.sample(False, 0.01, seed = 123)

df2 = not_fraud.union(fraud)


# 2 - Training model on dataframe

# In this part, we divide the dataframe into train and test. We then train a linear SVM model
(train, test) = df2.randomSplit([0.7, 0.3], seed = 123)

# To combine all feature data and separate 'label' data in a dataset, we use VectorAssembler
vecAssembler = VectorAssembler(inputCols = ['amount','oldbalanceOrg', 'newbalanceOrig'\
        , 'oldbalanceDest', 'newbalanceDest'], outputCol = 'features')



svc = LinearSVC(labelCol = 'isFraud', predictionCol = 'prediction')

model = Pipeline(stages = [vecAssembler, svc]).fit(train)


# We save the trained model and will use it to make predictions in the other file
model.save(sparkOutputTo)





