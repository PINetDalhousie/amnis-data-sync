#!/usr/bin/python3

from kafka import KafkaConsumer

from random import seed, randint, random

import sys
import time

import logging

import socket

def stringToList(string):
    listRes = list(string.split(" "))
    return listRes

def dataConsumption(nodeID, nTopics, cRate, fetchMinBytes, fetchMaxWait, sessionTimeout, brokers, mSizeString, mRate, replication, topicCheckInterval, portId, consTopic):
	""" for topicID in range(0, nTopics):
			startTime = time.time()
			#Randomly select topic
			#topicID = randint(0, nTopics-1)
			topicName = 'topic-'+str(topicID) """

	topicName = consTopic

	consumptionLag = random() < 0.95

	timeout = int((1.0/cRate) * 1000)

	bootstrapServers="10.0.0."+str(nodeID)+":9092"
	

	logging.info("**Configuring KafkaConsumer** topicName=" + topicName + " bootstrap_servers=" + str(bootstrapServers) +
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
			session_timeout_ms=sessionTimeout,
			group_id="group-"+str(nodeID)                                     
			)
	else:
		consumer = KafkaConsumer(topicName,
			bootstrap_servers=bootstrapServers,
			auto_offset_reset='earliest',
			enable_auto_commit=True,
			consumer_timeout_ms=timeout,
			fetch_min_bytes=fetchMinBytes,
			fetch_max_wait_ms=fetchMaxWait,
			session_timeout_ms=sessionTimeout,
			group_id="group-"+str(nodeID)                                     
			)

	logging.info('Connect to broker looking for topic %s. Timeout: %s.', topicName, str(timeout))

	if portId == 0:
		for msg in consumer:
			msgContent = str(msg.value, 'utf-8', errors='ignore')

		# 				prodID = msgContent[0]
		# 				bMsgID = bytearray(msgContent[1:5], 'utf-8')
		# 				print(len(bMsgID))
		# 				msgID = int.from_bytes(bMsgID, 'big')
		# 				topic = msg.topic
		# 				offset = str(msg.offset)

			prodID = msgContent[:2]
			bMsgID = bytearray(msgContent[2:6], 'utf-8')
			msgID = int.from_bytes(bMsgID, 'big')
			topic = msg.topic
			offset = str(msg.offset)
			logging.info('Prod ID: %s; Message ID: %s; Latest: %s; Topic: %s; Offset: %s; Size: %s; whole message: %s', prodID, str(msgID), str(consumptionLag), topic, offset, str(len(msgContent)), msgContent)           

	# else:
	# 	host = '127.0.0.1' #"10.0.0."+str(nodeID)
	# 	logging.info("inside else")
	# 	logging.info("host id: " + host)
	# 	logging.info("port id: " + str(portId))
	# 	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
	# 		a = s.bind((host, portId))
	# 		logging.info(a)
	# 		s.listen()
	# 		conn, addr = s.accept()
	# 		logging.info("connected opened")                    
	# 		with conn:
	# 			logging.info("connected by: "+addr)
	# 			for msg in consumer:                        
	# 				messageReadFromConsumer(consumer, consumptionLag)
	# 				logging.info('Prod ID: %s; Message ID: %s; Latest: %s; Topic: %s; Offset: %s; Size: %s; whole message: %s', prodID, str(msgID), str(consumptionLag), topic, offset, str(len(msgContent)), msgContent)           
					
	# 				msgContent = str(msg.value, 'utf-8')
	# 				sentMsg = msgContent.encode("utf-8")
	# 				conn.send(sentMsg)


	consumer.close()
	logging.info('Disconnect from broker')
		#stopTime = time.time()
		
		
	""" topicCheckWait = topicCheckInterval -(stopTime - startTime)
	if(topicCheckWait > 0):
		time.sleep(topicCheckWait) """

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
	portId = int(sys.argv[12])

	consTopic = sys.argv[13]
    
    
	logging.basicConfig(filename="logs/kafka/"+"nodes:" +str(brokers)+ "_mSize:"+ mSizeString+ "_mRate:"+ str(mRate)+ "_topics:"+str(nTopics) +"_replication:"+str(replication)+"/cons/cons-"+nodeID+".log",
							format='%(asctime)s %(levelname)s:%(message)s',
							level=logging.INFO)    
	logging.info("node: "+nodeID)
	logging.info("consuming from topic: "+consTopic)
    


	while True:
		dataConsumption(nodeID, nTopics, cRate, fetchMinBytes, fetchMaxWait, sessionTimeout, brokers, mSizeString, mRate, replication, topicCheckInterval, portId, consTopic)
		
                


except Exception as e:
	logging.error(e)
	sys.exit(1)
