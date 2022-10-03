# to run this script: sudo /home/monzurul/Desktop/amnis-data-sync/spark/pyspark/bin/spark-submit latencyScript.py <logDir> <50ms>

# This file calculates the latency of processing each file by a spark application, using information
# from producer and consumer logs

from turtle import color
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

import matplotlib.pyplot as plt
import numpy as np

import builtins as p

import time
import sys
import shutil
import os

# Some basic spark set up
spark = SparkSession.builder.appName("Latency Script").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# Reading the producer and consumer log files into a dataframe each
logDir = sys.argv[1]
# plotLink = sys.argv[2]  #'varying-H2-S-link-only-bw1Gbps'
plotLinkLatency = sys.argv[2]  #'10ms'

producerLog = logDir + '/prod-1.log'
consumerLog = logDir + '/cons4.log'


producerDF = spark.read.option('inferSchema', True).option('header', True).text(producerLog)

consumerDF = spark.read.option('inferSchema', True).option('header', True).text(consumerLog)


# Processing to get the data in our desired format

split_result = split("value", "INFO:")
producerDF = producerDF.select(split_result.getItem(0).alias('timestamp'), split_result.getItem(1).alias('value'))

split_result = split("value", "INFO:")
consumerDF = consumerDF.select(split_result.getItem(0).alias('timestamp'), split_result.getItem(1).alias('value'))

# Getting the rows mentioning the file number
producerDF.createOrReplaceTempView('producer')

producerDF = spark.sql("SELECT * FROM producer WHERE value LIKE '%File:%'")

consumerDF.createOrReplaceTempView('consumer')

consumerDF = spark.sql("SELECT * FROM consumer WHERE value LIKE '%rrrr%'")

# Getting the number of each file that was sent along with the timestamp

split_result = split("value", "File:")
producerDF = producerDF.select(col('timestamp').alias('send_time'), split_result.getItem(0).alias('send_message'), \
        split_result.getItem(1).alias('file'))

producerDF.show(101,truncate=False)

split_result = split("value", "rrrr")
consumerDF = consumerDF.select(col('timestamp').alias('receive_time'), split_result.getItem(0).alias('message'), \
        split_result.getItem(1).alias('file'))

# We get the first instance of each file in the consumer log

consumerDF.createOrReplaceTempView('cons')

consumerDF = spark.sql("SELECT file, FIRST(receive_time) AS receive_time, FIRST(message) AS receive_message \
        FROM cons GROUP BY file")

consumerDF.show(101,truncate=False)

# # Now we combine the two dataframes in preparation for calculating the latency

combinedDF = producerDF.join(consumerDF, producerDF.file == consumerDF.file)
# combinedDF.show(101,truncate=False)


# Selecting the desired columns from the combined dataframe

combinedDF = combinedDF.select('cons.file', regexp_replace('send_time', ',', '.').alias('send_time'), \
        regexp_replace('receive_time', ',', '.').alias('receive_time'))


# Calculating the latency and adding it as a new column
# Note that the latency is in the form INTERVAL 'd hh:mm:ss.ms' DAY TO SECOND

# Where 
# d = day
# hh = hours
# mm = minutes
# ss = seconds
# ms = milliseconds

# resultDF = combinedDF.withColumn('latency', \
#         (to_timestamp('receive_time') - to_timestamp('send_time')).cast('string'))

combinedDF = combinedDF.select('file', to_timestamp( col('send_time') ).alias('send_time'), \
        to_timestamp( col('receive_time') ).alias('receive_time') )

# print(combinedDF.columns)
# print(combinedDF.printSchema())
# combinedDF.show(20, truncate = False)

# storing latency in miliseconds
resultDF = combinedDF.select('file',
            ((col('receive_time').cast('double') - col('send_time')\
                .cast('double'))*1000 ).cast('long').alias('latency')
            )

# Displaying each dataframe
# print(producerDF.columns)
# producerDF.show(20, truncate = False)

# print(consumerDF.columns)
# consumerDF.show(20000, truncate = False)

# print(combinedDF.columns)
# print(combinedDF.printSchema())
# combinedDF.show(20000, truncate = False)

print(resultDF.printSchema())
print(resultDF.columns)
resultDF.show(200, truncate = False)

# We display the results

files = [data[0] for data in resultDF.select('file').collect()]
latency = [data[0] for data in resultDF.select('latency').collect()]

print("files before sorting: ")
print(*files)
print("latency before sorting by filenumber: ")
print(*latency)

# from list of strings to list of integers
res = [eval(i) for i in files]

# sorting both list in ascending order of the file number
tuple1, tuple2 = zip(*sorted(zip(res, latency)))
print("files after sorting: ")
print(*tuple1)
# print(type(tuple1))
# print(len(tuple1))

print("latency after sorting by filenumber: ")
print(*tuple2)
print(type(tuple2))

minLatency = p.min(tuple2)
maxLatency = p.max(tuple2)
print(maxLatency)

# showing average as a horizontal line
latencySum = p.sum(tuple2)
averageLatency = float(latencySum/len(tuple1))
plt.axhline(y=averageLatency, color='r', linestyle='-')


plt.xlabel('File')
plt.ylabel('Latency in miliseconds')
plt.title('Latency Per File')

# plot X axis values at a interval
plt.xticks(range(0,105,10))
plt.yticks(range(minLatency, maxLatency+10, 10))
plt.scatter(tuple1, tuple2)

#plt.show()
plt.savefig(logDir+'/'+ plotLinkLatency +'-latency.png')

# time.sleep(5)

# #copying logs and latency plot from logs to latencyPlots directory

# # path to source directory
# src_dir = logDir
 
# # path to destination directory
# dest_dir = '/home/monzurul/Desktop/amnis-data-sync/use-cases/varying-networking-conditions/varying-link-latency/word-count/latencyPlots/'+\
#                 plotLink+'/'+plotLinkLatency+'/'
# os.makedirs(dest_dir)
 
# #shutil.copytree(src_dir, dest_dir)

# shutil.copy(logDir+'/prod-1.log', dest_dir)
# shutil.copy(logDir+'/cons4.log', dest_dir)
# shutil.copy(logDir+'/'+ plotLinkLatency +'-latency.png', dest_dir)




