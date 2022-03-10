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
    
#     skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# #     skt.bind((socket.gethostname(), 9999))
#     skt.bind(("10.0.0."+str(nodeID), 9999))
# #     skt.listen()

	while True:
#         client, address = skt.accept()
		for topicID in range(0, nTopics):
			startTime = time.time()
			#Randomly select topic
			#topicID = randint(0, nTopics-1)
			topicName = 'topic-'+str(topicID)

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

# 				logging.info('Latest: %s; Topic: %s; Offset: %s; Size: %s', str(consumptionLag), topic, offset, str(len(msgContent)))
# 				logging.info("message %s:%d:%d: key=%s value=%s" % (msg.topic, msg.partition,msg.offset, msg.key,msg.value.decode('utf-8')))
                info = msgContent[6:]
#                 client.send(bytes(info,"utf-8"))


			consumer.close()
			logging.info('Disconnect from broker')
			stopTime = time.time()
            
            
			topicCheckWait = topicCheckInterval -(stopTime - startTime)
			if(topicCheckWait > 0):
				time.sleep(topicCheckWait)
                
# 	client.close()
# 	logging.info('Disconnect from client')                

except Exception as e:
	logging.error(e)
	sys.exit(1)
