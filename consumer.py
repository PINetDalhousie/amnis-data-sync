#!/usr/bin/python3

from kafka import KafkaConsumer

from random import seed, randint, random

import sys
import time

import logging

try:
	seed(2)

	nodeName = sys.argv[1]
	nodeID = nodeName[1:]

	nTopics = int(sys.argv[2])
	rate = float(sys.argv[3])

	fetchMinBytes = int(sys.argv[4])
	fetchMaxWait = int(sys.argv[5])
	sessionTimeout = int(sys.argv[6])

	logging.basicConfig(filename='logs/cons/cons-'+nodeID+'.log',
						format='%(asctime)s %(levelname)s:%(message)s',
 						level=logging.INFO)
	while True:

		#Randomly select topic
		topicID = randint(0, nTopics-1)
		topicName = 'topic-'+str(topicID)

		consumptionLag = random() < 0.95

		timeout = int(1.0/rate) * 1000

		#TODO: erase debug code
		#fromBegin = ""
		bootstrapServers="10.0.0."+str(nodeID)+":9092"

		logging.info("**Configuring KafkaConsumer** bootstrap_servers=" + str(bootstrapServers) +
			" consumer_timeout_ms=" + str(timeout) + " fetch_min_bytes=" + str(fetchMinBytes) +
			" fetch_max_wait_ms=" + str(fetchMaxWait) + " session_timeout_ms=" + str(sessionTimeout))

		if consumptionLag == True:
			consumer = KafkaConsumer(topicName,
			 	bootstrap_servers=bootstrapServers,
			 	#auto_offset_reset='earliest',
			 	enable_auto_commit=True,
			 	consumer_timeout_ms=timeout,
			 	fetch_min_bytes=fetchMinBytes,
			 	fetch_max_wait_ms=fetchMaxWait,
			 	session_timeout_ms=sessionTimeout
				)
			#fromBegin = "latest"
		else:
			consumer = KafkaConsumer(topicName,
			 	bootstrap_servers=bootstrapServers,
			 	auto_offset_reset='earliest',
			 	enable_auto_commit=True,
			 	consumer_timeout_ms=timeout,
			 	fetch_min_bytes=fetchMinBytes,
			 	fetch_max_wait_ms=fetchMaxWait,
			 	session_timeout_ms=sessionTimeout
				)
			#fromBegin = "begin"

		#f = open("test-consumer-"+str(nodeID)+"-"+str(topicID)+"-"+fromBegin+".txt", "a")
		#f.write("test\n")

		logging.info('Connect to broker looking for topic %s. Timeout: %s.', topicName, str(timeout))

		for msg in consumer:
			msgContent = str(msg.value, 'utf-8')

			prodID = msgContent[0]
			bMsgID = bytearray(msgContent[1:5], 'utf-8')
			msgID = int.from_bytes(bMsgID, 'big')
			topic = msg.topic
			offset = str(msg.offset)
			logging.info('Prod ID: %s; Message ID: %s; Latest: %s; Topic: %s; Offset: %s; Size: %s', prodID, str(msgID), str(consumptionLag), topic, offset, str(len(msgContent)))

			#f.write(str(msg.value)+"\n")

		#f.close()

		consumer.close()
		logging.info('Disconnect from broker')

		#time.sleep(1.0/rate)

except Exception as e:
	logging.error(e)
	sys.exit(1)
