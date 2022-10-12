#!/usr/bin/python3

from mininet.net import Mininet
from mininet.cli import CLI

from random import seed, randint, choice

import time
import os
import sys
import itertools

import subprocess
from subprocess import Popen, PIPE, STDOUT
from datetime import datetime


def spawnProducers(net, mSizeString, mRate, tClassString, nTopics, args, prodDetailsList, topicPlace):

	acks = args.acks
	compression = args.compression
	batchSize = args.batchSize
	linger = args.linger
	requestTimeout = args.requestTimeout
	brokers = args.nBroker
	replication = args.replication 
	messageFilePath = args.messageFilePath   

	tClasses = tClassString.split(',')
	#print("Traffic classes: " + str(tClasses))

	nodeClassification = {}
	netNodes = {}    

	classID = 1

	for tClass in tClasses:
		nodeClassification[classID] = []
		classID += 1
	
	#Distribute nodes among classes
	for node in net.hosts:
		netNodes[node.name] = node

	j =0
	for j in prodDetailsList:
		j['tClasses'] = str(randint(1,len(tClasses)))
	
	for i in prodDetailsList:
		nodeId = 'h' + i['nodeId']
		
		producerType = i["producerType"]
		messageFilePath = i['produceFromFile']
		tClasses = i['tClasses']
		prodTopic = i['produceInTopic']
		prodNumberOfFiles = i['prodNumberOfFiles']

		try:
			topicName = [x for x in topicPlace if x['topicName'] == prodTopic][0]["topicName"]
			brokerId = [x for x in topicPlace if x['topicName'] == prodTopic][0]["topicBroker"] 

			print("Producing messages to topic "+topicName+" at broker "+brokerId)

			node.popen("python3 producer.py "+nodeId+" "+tClasses+" "+mSizeString+" "+str(mRate)+" "+str(nTopics)+" "+str(acks)+" "+str(compression)\
			+" "+str(batchSize)+" "+str(linger)+" "+str(requestTimeout)+" "+brokerId+" "+str(replication)+" "+messageFilePath\
			+" "+topicName+" "+producerType+" "+prodNumberOfFiles+" &", shell=True)

		except IndexError:
			print("Error: Production topic name not matched with the already created topics")
			sys.exit(1)
			



def spawnConsumers(net, consDetailsList, topicPlace):

	netNodes = {}

	for node in net.hosts:
		netNodes[node.name] = node
        
	for cons in consDetailsList:
		consInstance = 1
		
		consNode = cons["nodeId"]
		topicName = cons["consumeFromTopic"][0]
		consID = "h"+consNode      
		node = netNodes[consID]

		# number of consumers
		numberOfConsumers = int(cons["consumeFromTopic"][-1])

		print("consumer node: "+consNode)
		print("topic: "+topicName)
		print("Number of consumers for this topic: "+str(numberOfConsumers))

		try:
			topicName = [x for x in topicPlace if x['topicName'] == topicName][0]["topicName"]
			brokerId = [x for x in topicPlace if x['topicName'] == topicName][0]["topicBroker"] 

			print("Consuming messages from topic "+topicName+" at broker "+brokerId)

			while consInstance <= int(numberOfConsumers):
				node.popen("python3 consumer.py "+str(node.name)+" "+topicName+" "+brokerId+" "+str(consInstance)+" &", shell=True)
				consInstance += 1

		except IndexError:
			print("Error: Consume topic name not matched with the already created topics")
			sys.exit(1)


def spawnSparkClients(net, sparkDetailsList):
	netNodes = {}

	for node in net.hosts:
		netNodes[node.name] = node

	for sprk in sparkDetailsList:
		time.sleep(30)
		
		sparkNode = sprk["nodeId"]
		# sparkInputFrom = sprk["topicsToConsume"]
		sparkApp = sprk["applicationPath"]
		sparkOutputTo = sprk["produceTo"]
		print("spark node: "+sparkNode)
		print("spark App: "+sparkApp)
		# print("spark input from: "+sparkInputFrom)
		print("spark output to: "+sparkOutputTo)
		print("*************************")

		sprkID = "h"+sparkNode
		node = netNodes[sprkID]

		# out= node.cmd("sudo ~/.local/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 "+sparkApp\
		# 			+" "+str(node.name)+" "+sparkOutputTo, shell=True) 

		# out= node.cmd("sudo /home/monzurul/.local/lib/python3.8/site-packages/pyspark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 "+sparkApp\
		# 			+" "+str(node.name)+" "+sparkOutputTo, shell=True) 

		node.popen("sudo spark/pyspark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 "+sparkApp\
					+" "+str(node.name)+" "+sparkOutputTo+" &", shell=True)

		# node.popen("sudo spark/pyspark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 "+sparkApp\
		# 			+" &", shell=True) 
		# print(out)

		
def spawnKafkaMySQLConnector(net, prodDetailsList, mysqlPath):
	netNodes = {}

	for node in net.hosts:
		netNodes[node.name] = node
	
	connNode = prodDetailsList[0]["nodeId"]
	connID = "h"+connNode      
	node = netNodes[connID]

	print("=========")
	print("connector starts on node: "+connID)
	
	node.popen("sudo kafka/bin/connect-standalone.sh kafka/config/connect-standalone-new.properties "+ mysqlPath +" > logs/connectorOutput.txt &", shell=True)
	

def runLoad(net, args, topicPlace, prodDetailsList, consDetailsList, sparkDetailsList, \
	mysqlPath, brokerPlace, isDisconnect, dcDuration, dcLinks, topicWaitTime=100):

	nTopics = args.nTopics
	replication = args.replication
	mSizeString = args.mSizeString
	mRate = args.mRate
	tClassString = args.tClassString
	consumerRate = args.consumerRate
	duration = args.duration

	# give some time to warm up the brokers
	time.sleep(5)

	print("Start workload")

	seed(1)

	nHosts = len(net.hosts)
	print("Number of hosts: " + str(nHosts))
    
	#Creating topic(s) in respective broker
	topicNodes = []
	startTime = time.time()

	for topic in topicPlace:
		topicName = topic["topicName"]
		issuingID = (int) (topic["topicBroker"])-1
		topicPartition = topic["topicPartition"]
		issuingNode = net.hosts[issuingID]

		print("Creating topic "+topicName+" at broker "+str(issuingID+1)+" partition "+str(topicPartition))

		out = issuingNode.cmd("kafka/bin/kafka-topics.sh --create --bootstrap-server 10.0.0."+str(issuingID+1)+":9092\
			 --replication-factor "+str(replication)+" --partitions " + topicPartition + \
				" --topic "+topicName, shell=True)         
		
		print(out)
		topicNodes.append(issuingNode)

		topicDetails = issuingNode.cmd("kafka/bin/kafka-topics.sh --describe --bootstrap-server 10.0.0."+str(issuingID+1)+":9092", shell=True)
		print(topicDetails)

	stopTime = time.time()
	totalTime = stopTime - startTime
	print("Successfully Created " + str(len(topicPlace)) + " Topics in " + str(totalTime) + " seconds")
	
	#starting Kafka-MySQL connector
	if mysqlPath != "":
		spawnKafkaMySQLConnector(net, prodDetailsList, mysqlPath)
		print("Kafka-MySQL connector instance created")

	if args.onlyKafka == 0:
		spawnSparkClients(net, sparkDetailsList)
		time.sleep(30)
		print("Spark Clients created")

	spawnProducers(net, mSizeString, mRate, tClassString, nTopics, args, prodDetailsList, topicPlace)
	# time.sleep(120)
	print("Producers created")
	
	spawnConsumers(net, consDetailsList, topicPlace)
	# time.sleep(10)
	print("Consumers created")

	# spawnProducers(net, mSizeString, mRate, tClassString, nTopics, args, prodDetailsList, topicPlace)
	# # time.sleep(10)
	# print("Producers created")

	# spark start its processing once the production is done
	# time.sleep(30)

	# spawnSparkClients(net, sparkDetailsList)
	# # time.sleep(10)
	# print("Spark Clients created")

	# time.sleep(150)

	timer = 0

	# hard-coded disconnection for testing
	# isDisconnect = 1
	# args.disconnectDuration = 10
	# args.disconnectLink = ['s1-h2','s1-h3']

	# Set up disconnect
	if isDisconnect:
		isDisconnected = False
		disconnectTimer = dcDuration

	print(f"Starting workload at {str(datetime.now())}")
	
	while timer < duration:
		time.sleep(10)
		percentComplete = int((timer/duration)*100)
		print("Processing workload: "+str(percentComplete)+"%")

		if isDisconnect and percentComplete >= 10:
			if not isDisconnected:	
				for link in dcLinks:
					linkSplit = link.split('-')
					n1 = net.getNodeByName(linkSplit[0])
					n2 = net.getNodeByName(linkSplit[1])
					disconnectLink(net, n1, n2)
				isDisconnected = True

			elif isDisconnected and disconnectTimer <= 0: 	
				for link in dcLinks:
					linkSplit = link.split('-')
					n1 = net.getNodeByName(linkSplit[0])
					n2 = net.getNodeByName(linkSplit[1])				
					reconnectLink(net, n1, n2)
				isDisconnected = False
				isDisconnect = False
				
			if isDisconnected:
				disconnectTimer -= 10

		timer += 10

	print(f"Workload finished at {str(datetime.now())}")	


def disconnectLink(net, n1, n2):
	print(f"***********Setting link down from {n1.name} <-> {n2.name} at {str(datetime.now())}")
	net.configLinkStatus(n2.name, n1.name, "down")
	net.pingAll()

def reconnectLink(net, n1, n2):
	print(f"***********Setting link up from {n1.name} <-> {n2.name} at {str(datetime.now())}")
	net.configLinkStatus(n2.name, n1.name, "up")
	net.pingAll()