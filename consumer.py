#!/usr/bin/python3

from kafka import KafkaConsumer

from random import seed, randint, random

import sys
import time

seed(2)

nodeName = sys.argv[1]
nodeID = nodeName[1:]

nTopics = int(sys.argv[2])
rate = float(sys.argv[3])


while True:

	#Randomly select topic
	topicID = randint(0, nTopics-1)
	topicName = 'topic-'+str(topicID)

	consumptionLag = random() < 0.95

	#TODO: erase debug code
	#fromBegin = ""

	if consumptionLag == True:
		consumer = KafkaConsumer(topicName,
			 bootstrap_servers="10.0.0."+str(nodeID)+":9092",
			 #auto_offset_reset='earliest',
			 enable_auto_commit=True,
			 consumer_timeout_ms=1000
			)
		#fromBegin = "latest"
	else:
		consumer = KafkaConsumer(topicName,
			 bootstrap_servers="10.0.0."+str(nodeID)+":9092",
			 auto_offset_reset='earliest',
			 enable_auto_commit=True,
			 consumer_timeout_ms=1000
			)
		#fromBegin = "begin"

	#f = open("test-consumer-"+str(nodeID)+"-"+str(topicID)+"-"+fromBegin+".txt", "a")
	#f.write("test\n")

	for msg in consumer:
		print (msg.value)

		#f.write(str(msg.value)+"\n")

	#f.close()

	consumer.close()

	time.sleep(1.0/rate)
















