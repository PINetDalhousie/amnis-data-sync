#!/usr/bin/python3

from email.headerregistry import MessageIDHeader
from kafka import KafkaProducer

from random import seed, randint, gauss

import time
import sys
import logging
import re
import random
import os

msgID = 0

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

def messageProductionSFST(messageFilePath):
	global msgID
	if(messageFilePath != 'None'):
			message = readMessageFromFile(messageFilePath)
			logging.info("Message Generated From File "+messageFilePath)

	else:
		message = generateMessage(mSizeParams)
		logging.info("Generated Message")
	
	# bMsg = bytearray(message)

	# producer.send(topicName, bMsg)
	# logging.info('Topic: %s; Message ID: %s; Message: %s', topicName, str(msgID), message)       
	# msgID += 1

	# sending a single file till the duration of the simulation
	separator = 'rrrr'
	sentMessage = message + bytes(separator,'utf-8') + bytes(str(msgID), 'utf-8')

	return sentMessage

# def messageProductionSFST(bootstrapServers, messageFilePath,topicName):
# 	global msgID
# 	if(messageFilePath != 'None'):
# 			message = readMessageFromFile(messageFilePath)
# 			logging.info("Message Generated From File "+messageFilePath)

# 	else:
# 		message = generateMessage(mSizeParams)
# 		logging.info("Generated Message")
	
# 	separator = 'rrrr'
# 	sentMessage = message + bytes(separator,'utf-8') + bytes(str(msgID), 'utf-8')

# 	producer = KafkaProducer(bootstrap_servers = bootstrapServers)
# 	producer.send(topicName, sentMessage)

# 	fileID = "File: " +str(msgID)
# 	logging.info('      File has been sent ->  Topic: %s; File ID: %s', \
#                         topicName, str(fileID))

# 	msgID += 1


def messageProductionMFST(messageFilePath,fileNumber):
	if(messageFilePath != 'None'):
			message = readMessageFromFile(messageFilePath)
			logging.info("Message Generated From File "+messageFilePath)

	else:
		message = generateMessage(mSizeParams)
		logging.info("Generated Message")
	
	separator = 'rrrr'
	sentMessage = message + bytes(separator,'utf-8') + bytes(str(fileNumber), 'utf-8')

	return sentMessage

	# producer = KafkaProducer(bootstrap_servers = bootstrapServers)
	# producer.send(topicName, sentMessage)

	# fileID = "File: " +str(msgID)
	# logging.info('      File has been sent ->  Topic: %s; File ID: %s', \
    #                     topicName, str(fileID))

	# msgID += 1


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
	brokerId = sys.argv[11]
	replication = int(sys.argv[12]) 
	# messageFilePath = sys.argv[13] 
	directoryPath = sys.argv[13]  #it will hold the file path/directory path based on producer type SFST or MFST respectively
	prodTopic = sys.argv[14] 
	prodType = sys.argv[15] 

	#print(nTopics)
	#print(compression)
	#print(prodFile)
	#print(prodTopic)

	seed(1)

	mSizeParams = mSizeString.split(',')
	nodeID = node[1:]
	# msgID = 0
    
	logging.basicConfig(filename="logs/output/"+"prod-"+str(nodeID)+".log",
							format='%(asctime)s %(levelname)s:%(message)s',
							level=logging.INFO)                             
       
	logging.info("node to initiate producer: "+nodeID)
	logging.info("topic name: "+prodTopic)
	logging.info("topic broker: "+brokerId)
	#logging.info("input file: "+prodFile)
	#logging.info("produce data in topic: "+prodTopic)
    

	bootstrapServers="10.0.0."+brokerId+":9092"

	# Convert acks=2 to 'all'
	if(acks == 2):
		acks = 'all'

	logging.info("**Configuring KafkaProducer** bootstrap_servers=" + str(bootstrapServers) + 
		" acks=" + str(acks) + " compression_type=" + str(compression) + " batch_size=" + str(batchSize) + 
		" linger_ms=" + str(linger) + " request_timeout_ms=" + str(requestTimeout))

	producer = KafkaProducer(bootstrap_servers=bootstrapServers)
	i = 0
	
	if prodType == "MFST":
		files = os.listdir(directoryPath)
		
		while True:
			for oneFile in files:
				messageFilePath = directoryPath + oneFile
				sentMessage = messageProductionMFST(messageFilePath, i)
				fileID = "File: " +str(i)

				producer.send(prodTopic, sentMessage)
				
				logging.info('      File has been sent ->  Topic: %s; File ID: %s', \
									prodTopic, str(fileID))

				i += 1


	elif prodType == "SFST":
		while True:
		# while msgID <= 100:
			if msgID <= 100:
				# approach 1
				sentMessage = messageProductionSFST(directoryPath)
				fileID = "File: " +str(msgID)

				producer.send(prodTopic, sentMessage)
				logging.info('      File has been sent ->  Topic: %s; File ID: %s', \
									prodTopic, str(fileID))

				msgID += 1
			else:
				continue

			#approach 2
			# messageProductionSFST(bootstrapServers, directoryPath,prodTopic)


except Exception as e:
	logging.error(e)
	sys.exit(1)