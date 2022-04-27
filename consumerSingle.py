#!/usr/bin/python3

from tracemalloc import start
from kafka import KafkaConsumer

from random import seed, random

import sys
import time

import logging


try:
	seed(2)

	nodeName = sys.argv[1]
	nodeID = nodeName[1:]

	nTopics = int(sys.argv[2])
	cRate = float(sys.argv[3])

	fetchMinBytes = int(sys.argv[4])
	fetchMaxWait = int(sys.argv[5])
	sessionTimeout = int(sys.argv[6])
	brokers = int(sys.argv[7])    
	mSizeString = sys.argv[8]
	mRate = float(sys.argv[9])    
	replication = int(sys.argv[10])   
	topicCheckInterval = float(sys.argv[11])  
    
	logging.basicConfig(filename="logs/kafka/"+"nodes:" +str(brokers)+ "_mSize:"+ mSizeString+ "_mRate:"+ str(mRate)+ "_topics:"+str(nTopics) +"_replication:"+str(replication)+"/cons/cons-"+nodeID+".log",
							format='%(asctime)s %(levelname)s:%(message)s',
							level=logging.INFO)    
	logging.info("node: "+nodeID)
	consumers = []
	timeout = int((1.0/cRate) * 1000)
	bootstrapServers="10.0.0."+str(nodeID)+":9092"


	# One consumer for all topics
	topicName = 'topic-*'
	consumptionLag = random() < 0.95				
	logging.info("**Configuring KafkaConsumer** topicName=" + topicName + " bootstrap_servers=" + str(bootstrapServers) +
		" consumer_timeout_ms=" + str(timeout) + " fetch_min_bytes=" + str(fetchMinBytes) +
		" fetch_max_wait_ms=" + str(fetchMaxWait) + " session_timeout_ms=" + str(sessionTimeout))

	consumer = KafkaConsumer(
		#topicName,
		bootstrap_servers=bootstrapServers,
		auto_offset_reset='latest' if consumptionLag else 'earliest',
		enable_auto_commit=True,
		consumer_timeout_ms=timeout,
		fetch_min_bytes=fetchMinBytes,
		fetch_max_wait_ms=fetchMaxWait,
		session_timeout_ms=sessionTimeout,
		group_id="group-"+str(nodeID)                                     
	)	
	consumer.subscribe(pattern=topicName)
	
	# Poll the data
	logging.info('Connect to broker looking for topic %s. Timeout: %s.', topicName, str(timeout))
	while True:
		startTime = time.time()
		for msg in consumer:
			msgContent = str(msg.value, 'utf-8', errors='ignore')
			prodID = msgContent[:2]
			bMsgID = bytearray(msgContent[2:6], 'utf-8')
			msgID = int.from_bytes(bMsgID, 'big')
			topic = msg.topic
			offset = str(msg.offset)                    
			logging.info('Prod ID: %s; Message ID: %s; Latest: %s; Topic: %s; Offset: %s; Size: %s', prodID, str(msgID), str(consumptionLag), topic, offset, str(len(msgContent)))           
		stopTime = time.time()
		topicCheckWait = topicCheckInterval -(stopTime - startTime)
		if(topicCheckWait > 0):
			logging.info('Sleeping for topicCheckWait: %s', str(topicCheckWait))
			time.sleep(topicCheckWait)


except Exception as e:
	logging.error(e)	
finally:
	consumer.close()
	logging.info('Disconnect from broker')
	sys.exit(1)