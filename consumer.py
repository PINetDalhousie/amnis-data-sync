#!/usr/bin/python3

from kafka import KafkaConsumer

from random import seed, randint, random

import sys
import time

import logging


try:
	seed(2)

	nodeName = sys.argv[1]
	topicName = sys.argv[2]
	brokerId = sys.argv[3]

	nodeID = nodeName[1:]
    
	logging.basicConfig(filename="logs/output/"+"cons"+str(nodeID)+".log",
							format='%(asctime)s %(levelname)s:%(message)s',
							level=logging.INFO)    
	logging.info("node to initiate consumer: "+nodeID)
	logging.info("topicName "+topicName)
	logging.info("topicBroker "+brokerId)

	while True:

		bootstrapServers="10.0.0."+brokerId+":9092"
		
		consumer = KafkaConsumer(topicName,
			bootstrap_servers=bootstrapServers,
			#auto_offset_reset='earliest',
			enable_auto_commit=True,
			group_id="group-"+str(nodeID)                                     
			)

		logging.info('Connect to broker looking for topic %s. ', topicName)

		for msg in consumer:
			msgContent = str(msg.value, 'utf-8')
			logging.info("Message received:")
			logging.info(msgContent)


except Exception as e:
	logging.error(e)
	sys.exit(1)
