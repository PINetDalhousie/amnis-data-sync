#!/usr/bin/python3

from kafka import KafkaConsumer

from random import seed, randint, random

import sys
import time

import logging

import socket
import select

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
	topicName = sys.argv[13]
    
	logging.basicConfig(filename="logs/kafka/"+"nodes:" +str(brokers)+ "_mSize:"+ mSizeString+ "_mRate:"+ str(mRate)\
		+ "_topics:"+str(nTopics) +"_replication:"+str(replication)+"/cons/cons-"+nodeID+".log",\
		format='%(asctime)s %(levelname)s:%(message)s',\
		level=logging.INFO)    
	logging.info("node: "+nodeID)
	logging.info("consuming from topic: "+topicName)

	host = "10.0.0."+nodeID
	port = 12345

	bootstrapServers="10.0.0."+nodeID+":9092"

	consumer = KafkaConsumer(topicName,
		bootstrap_servers=bootstrapServers,
		#auto_offset_reset='earliest',
		enable_auto_commit=True                                  
		)
	logging.info(consumer)
	
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
		sock.setblocking(0)
		# bind the socket to the port
		sock.bind((host, port))
		# listen for incoming connections
		sock.listen(5)
		logging.info("Server started...")

		# sockets from which we expect to read
		inputs = [sock]
		outputs = []

		while inputs:
			# wait for at least one of the sockets to be ready for processing
			readable, writable, exceptional = select.select(inputs, outputs, inputs)

			for s in readable:

				conn, addr = s.accept()
				logging.info(f"Connected by {addr}")
				inputs.append(conn)

				for msg in consumer:
					msgContent = str(msg.value, 'utf-8', errors='ignore')	# for testing the msg

					sentMsg = msgContent[6:].encode("utf-8")
					
					logging.info("Message consumed and send to client:\n")
					logging.info(sentMsg)
					conn.send(sentMsg)

				inputs.remove(s)
				s.close()
                   


except Exception as e:
	logging.error(e)
	sys.exit(1)