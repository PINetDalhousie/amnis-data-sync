#!/usr/bin/python3

from kafka import KafkaConsumer

from random import seed, randint, random

import sys
import time

import logging

import socket
import threading

def stringToList(string):
    listRes = list(string.split(" "))
    return listRes

def consumeUsingSocket(nodeID, nTopics, cRate, fetchMinBytes, fetchMaxWait, sessionTimeout, brokers, mSizeString, mRate, replication, topicCheckInterval, portId, consTopic, s):
	while True:
		topicName = consTopic

		consumptionLag = random() < 0.95

		timeout = int((1.0/cRate) * 1000)

		bootstrapServers="10.0.0."+str(nodeID)+":9092"
		

		logging.info("**Configuring KafkaConsumer** topicName=" + topicName + " bootstrap_servers=" + str(bootstrapServers) +
		" consumer_timeout_ms=" + str(timeout) + " fetch_min_bytes=" + str(fetchMinBytes) +
		" fetch_max_wait_ms=" + str(fetchMaxWait) + " session_timeout_ms=" + str(sessionTimeout))

		logging.info("Thread %s: starting", nodeID)

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

		for msg in consumer:
			msgContent = str(msg.value, 'utf-8', errors='ignore')

			prodID = msgContent[:2]
			bMsgID = bytearray(msgContent[2:6], 'utf-8')
			msgID = int.from_bytes(bMsgID, 'big')
			topic = msg.topic
			offset = str(msg.offset)
			logging.info('Prod ID: %s; Message ID: %s; Latest: %s; Topic: %s; Offset: %s; Size: %s; whole message: %s', prodID, str(msgID), str(consumptionLag), topic, offset, str(len(msgContent)), msgContent)           

		



def dataConsumption(nodeID, nTopics, cRate, fetchMinBytes, fetchMaxWait, sessionTimeout, brokers, mSizeString, mRate, replication, topicCheckInterval, portId, consTopic):

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
			prodID = msgContent[:2]
			bMsgID = bytearray(msgContent[2:6], 'utf-8')
			msgID = int.from_bytes(bMsgID, 'big')
			topic = msg.topic
			offset = str(msg.offset)
			logging.info('Prod ID: %s; Message ID: %s; Latest: %s; Topic: %s; Offset: %s; Size: %s; whole message: %s', prodID, str(msgID), str(consumptionLag), topic, offset, str(len(msgContent)), msgContent)           

	# else:
	# 	host = '127.0.0.1' #"10.0.0."+str(nodeID)

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
    
    
	logging.basicConfig(filename="logs/kafka/"+"nodes:" +str(brokers)+ "_mSize:"+ mSizeString+ "_mRate:"+ str(mRate)\
		+ "_topics:"+str(nTopics) +"_replication:"+str(replication)+"/cons/cons-"+nodeID+".log",\
		format='%(asctime)s %(levelname)s:%(message)s',\
		level=logging.INFO)    
	logging.info("node: "+nodeID)
	logging.info("consuming from topic: "+consTopic)

	host = "10.0.0."+nodeID
	port = 12345
	try:
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		logging.info("Socket successfully created")

		s.bind((host, port))        
		logging.info("socket binded to %s" %(port))

		# put the socket into listening mode
		s.listen(5)    
		logging.info("socket is listening") 

		while True:
			topicName = consTopic

			consumptionLag = random() < 0.95

			timeout = int((1.0/cRate) * 1000)

			# bootstrapServers="10.0.0."+str(nodeID)+":9092"
			bootstrapServers="10.0.0."+nodeID+":9092"
			

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

			conn, addr = s.accept()
			logging.info('connected')
			with conn:
				logging.info(f"Connected by {addr}\n")
				for msg in consumer:                        
					msgContent = str(msg.value, 'utf-8', errors='ignore')
					prodID = msgContent[:2]
					bMsgID = bytearray(msgContent[2:6], 'utf-8')
					msgID = int.from_bytes(bMsgID, 'big')
					topic = msg.topic
					offset = str(msg.offset)
										
					sentMsg = bytearray(msgContent[6:], 'utf-8')
					# sentMsg = msgContent.encode("utf-8")

					logging.info('Prod ID: %s; Message ID: %s; Latest: %s; Topic: %s; Offset: %s; Size: %s; whole message: %s', prodID, str(msgID), str(consumptionLag), topic, offset, str(len(msgContent)), msgContent)
					logging.info("message: %s sent to the client",str(msgID))

					conn.send(sentMsg)

		# Threading code
		# logging.info("Consumer main    : before creating thread")
		# x = threading.Thread(target=consumeUsingSocket, args=(nodeID, nTopics, cRate, fetchMinBytes, fetchMaxWait, sessionTimeout,\
		# 	brokers, mSizeString, mRate, replication, topicCheckInterval, portId, consTopic, s))

		# logging.info("Consumer main    : before running thread")
		# x.start()
		# logging.info("Consumer main    : wait for the thread to finish")
		# x.join()
		# logging.info("Consumer main    : all done")
		
		
	except socket.error as err:
		logging.info("socket creation failed with error %s" %(err))
    


	# while True:
	# 	dataConsumption(nodeID, nTopics, cRate, fetchMinBytes, fetchMaxWait, sessionTimeout, brokers, mSizeString, mRate, replication, topicCheckInterval, portId, consTopic)
		
                


except Exception as e:
	logging.error(e)
	sys.exit(1)