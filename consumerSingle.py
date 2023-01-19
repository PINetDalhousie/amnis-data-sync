#!/usr/bin/python3

from tracemalloc import start
from kafka import KafkaConsumer

from random import seed, random

import sys
import time
import os
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
	ssl = bool(sys.argv[12])
    
	logging.basicConfig(filename="logs/kafka/"+"nodes:" +str(brokers)+ "_mSize:"+ mSizeString+ "_mRate:"+ str(mRate)+ "_topics:"+str(nTopics) +"_replication:"+str(replication)+"/cons/cons-"+nodeID+".log",
							format='%(asctime)s %(levelname)s:%(message)s',
							level=logging.INFO)    
	logging.info("node: "+nodeID)
	consumers = []
	timeout = int((1.0/cRate) * 1000)
	#bootstrapServers="10.0.0."+str(nodeID)+":9092"
	bootstrapServers="10.0.0."+str(nodeID)+":9093"


	# One consumer for all topics
	topicName = 'topic-*'
	consumptionLag = random() < 0.95				
	logging.info("**Configuring KafkaConsumer** topicName=" + topicName + " bootstrap_servers=" + str(bootstrapServers) +
		" consumer_timeout_ms=" + str(timeout) + " fetch_min_bytes=" + str(fetchMinBytes) +
		" fetch_max_wait_ms=" + str(fetchMaxWait) + " session_timeout_ms=" + str(sessionTimeout))
		
	if ssl:
		consumer = KafkaConsumer(			
			bootstrap_servers=bootstrapServers,
			auto_offset_reset='latest' if consumptionLag else 'earliest',
			enable_auto_commit=True,
			consumer_timeout_ms=timeout,
			fetch_min_bytes=fetchMinBytes,
			fetch_max_wait_ms=fetchMaxWait,
			session_timeout_ms=sessionTimeout,
			group_id="group-"+str(nodeID),
			security_protocol='SSL',
			ssl_check_hostname=False,
			# ssl_cafile=os.getcwd()+'/certs-offical/CARoot.pem',
			# ssl_certfile=os.getcwd()+'/certs-offical/cacert.pem',
			# ssl_keyfile=os.getcwd()+'/certs-offical/cakey.pem',
			ssl_cafile=os.getcwd()+'/certs/CARoot.pem',
			ssl_certfile=os.getcwd()+'/certs/ca-cert',
			ssl_keyfile=os.getcwd()+'/certs/ca-key',
			ssl_password="password"
		)	
	else:
		consumer = KafkaConsumer(			
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
	messages = {}
	while True:
		startTime = time.time()		
		for msg in consumer:
			try:
				msgContent = str(msg.value, 'utf-8')
				prodID = msgContent[:2]
				msgID = msgContent[2:8]
				topic = msg.topic
				offset = str(msg.offset)    

				key = prodID+"-"+msgID+"-"+topic
				if key in messages:
					logging.warn('ProdID %s MSG %s Topic %s already read. Not logging.', prodID, msgID, topic)				         
				else:
					messages[key] = offset
					logging.info('Prod ID: %s; Message ID: %s; Latest: %s; Topic: %s; Offset: %s; Size: %s', prodID, msgID, str(consumptionLag), topic, offset, str(len(msgContent)))           			
			except Exception as e:
				logging.error(e + " from messageID %s", msgID)				
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
