#!/usr/bin/python3

from kafka import KafkaProducer

from random import seed, randint, gauss

import time
import sys
import logging
import re
import random
import os

def processXmlFileMessage(file):
	lines = file.readlines()
	processedFile = ' '
	for line in lines:
		randomNum = str(random.randint(1,999))
		# Randomize values in XML file
		line = re.sub('[0-9]+', randomNum, line)
		processedFile += line
	return processedFile.encode()

def processFileMessage(file):
	message = file.read().encode()
	return message

def readMessageFromFile(filePath):
	file = open(filePath, 'r')
	_, fileExt = os.path.splitext(filePath)

	if(fileExt.lower() == '.xml'):
		message = processXmlFileMessage(file)
	#elif(fileExt.lower == '.svg'):
	#	message = processSvgFile(file)
	else:
		message = processFileMessage(file)

	return message

def generateMessage(mSizeParams):
	if mSizeParams[0] == 'fixed':
		msgSize = int(mSizeParams[1])
	elif mSizeParams[0] == 'gaussian':
		msgSize = int(gauss(float(mSizeParams[1]), float(mSizeParams[2])))
	
		if msgSize < 1:
			msgSize = 1
		
	payloadSize = msgSize - 4
            

	if payloadSize < 0:
		payloadSize = 0

	message = [97] * payloadSize
	return message



try:
	node = sys.argv[1]
	tClass = float(sys.argv[2])
	mSizeString = sys.argv[3]
	mRate = float(sys.argv[4])
	nTopics = int(sys.argv[5])

	acks = int(sys.argv[6])
	compression = sys.argv[7]
	batchSize = int(sys.argv[8])
	linger = int(sys.argv[9])
	requestTimeout = int(sys.argv[10])
	brokers = int(sys.argv[11])    
	replication = int(sys.argv[12]) 
	messageFilePath = sys.argv[13] 
	prodTopic = sys.argv[14] 

	#print(nTopics)
	#print(compression)
	#print(prodFile)
	#print(prodTopic)

	#prodFile, prodTopic = readProdConfig(prodConfigPath)


	seed(1)

	mSizeParams = mSizeString.split(',')
	nodeID = node[1:]
	msgID = 0
    

	logging.basicConfig(filename="logs/kafka/"+"nodes:" +str(brokers)+ "_mSize:"+ mSizeString+ "_mRate:"+ str(mRate)+ "_topics:"+str(nTopics) +"_replication:"+str(replication)+"/prod/prod-"+nodeID+".log",
							format='%(asctime)s %(levelname)s:%(message)s',
							level=logging.INFO)                             
# 						format='%(levelname)s:%(message)s',
#  						level=logging.INFO)

       
	logging.info("node: "+nodeID)
	logging.info("topic: "+prodTopic)
	#logging.info("input file: "+prodFile)
	#logging.info("produce data in topic: "+prodTopic)
    

	bootstrapServers="10.0.0."+nodeID+":9092"

	# Convert acks=2 to 'all'
	if(acks == 2):
		acks = 'all'

	logging.info("**Configuring KafkaProducer** bootstrap_servers=" + str(bootstrapServers) + 
		" acks=" + str(acks) + " compression_type=" + str(compression) + " batch_size=" + str(batchSize) + 
		" linger_ms=" + str(linger) + " request_timeout_ms=" + str(requestTimeout))


	if(compression == 'None'):
		producer = KafkaProducer(bootstrap_servers=bootstrapServers,
			acks=acks,
			batch_size=batchSize, 
			linger_ms=linger,
			request_timeout_ms=requestTimeout)
	else:
		producer = KafkaProducer(bootstrap_servers=bootstrapServers,
			acks=acks,
			compression_type=compression,
			batch_size=batchSize,
			linger_ms=linger,
			request_timeout_ms=requestTimeout)

 

	while True:
		if(messageFilePath != 'None'):
			message = readMessageFromFile(messageFilePath)
			logging.info("Message Generated From File")

		else:
			message = generateMessage(mSizeParams)
			logging.info("Generated Message")
		
		bMsgID = msgID.to_bytes(4, 'big')
		newNodeID = nodeID.zfill(2)
		bNodeID = bytes(newNodeID, 'utf-8')
		bMsg = bNodeID + bMsgID + bytearray(message)
		
		#for producing data in random topic
		# topicID = randint(0, nTopics-1)
		# topicName = 'topic-'+str(topicID)

		#for producing data in fixed topic from producer config
		topicName = prodTopic

		producer.send(topicName, bMsg)
		logging.info('Topic: %s; Message ID: %s;', topicName, str(msgID))
# 		logging.info('Topic: %s; Message ID: %s;', topicName, str(msgID).zfill(3))        
		msgID += 1
		time.sleep(1.0/(mRate*tClass))

except Exception as e:
	logging.error(e)
	sys.exit(1)

#def on_send_success(record_metadata):
    #print(record_metadata.topic)
    #print(record_metadata.partition)
    #print(record_metadata.offset)

#def on_send_error(excp):
    #log.error('I am an errback', exc_info=excp)
    # handle exception

#producer.send('topic-0', b'raw_bytes1').add_callback(on_send_success).add_errback(on_send_error)

#Sleep is important to give time for producer
#to send the message before process ends
#time.sleep(0.01)