#!/usr/bin/python3

from mininet.net import Mininet
from mininet.cli import CLI

from random import seed, randint

import time
import os
import sys
import itertools

import subprocess
from subprocess import Popen, PIPE, STDOUT


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
		
		messageFilePath = i['produceFromFile']
		tClasses = i['tClasses']
		prodTopic = i['produceInTopic']

		try:
			topicName = [x for x in topicPlace if x['topicName'] == prodTopic][0]["topicName"]
			brokerId = [x for x in topicPlace if x['topicName'] == prodTopic][0]["topicBroker"] 

			print("Producing messages to topic "+topicName+" at broker "+brokerId)

			node.popen("python3 producer.py "+nodeId+" "+tClasses+" "+mSizeString+" "+str(mRate)+" "+str(nTopics)+" "+str(acks)+" "+str(compression)\
			+" "+str(batchSize)+" "+str(linger)+" "+str(requestTimeout)+" "+brokerId+" "+str(replication)+" "+messageFilePath\
			+" "+topicName+" &", shell=True)

		except IndexError:
			print("Error: Production topic name not matched with the already created topics")
			sys.exit(1)
			



def spawnConsumers(net, consDetailsList, topicPlace):

	netNodes = {}

	for node in net.hosts:
		netNodes[node.name] = node
        
	for cons in consDetailsList:
		consNode = cons["nodeId"]
		topicName = cons["consumeFromTopic"][0]
		consID = "h"+consNode      
		node = netNodes[consID]

		print("consumer node: "+consNode)
		print("topic: "+topicName)

		try:
			topicName = [x for x in topicPlace if x['topicName'] == topicName][0]["topicName"]
			brokerId = [x for x in topicPlace if x['topicName'] == topicName][0]["topicBroker"] 

			print("Consuming messages from topic "+topicName+" at broker "+brokerId)

			node.popen("python3 consumer.py "+str(node.name)+" "+topicName+" "+brokerId+" &", shell=True)

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
	

def runLoad(net, args, topicPlace, prodDetailsList, consDetailsList, sparkDetailsList, mysqlPath, brokerPlace, topicWaitTime=100):
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
		issuingNode = net.hosts[issuingID]

		print("Creating topic "+topicName+" at broker "+str(issuingID+1))

		out = issuingNode.cmd("kafka/bin/kafka-topics.sh --create --bootstrap-server 10.0.0."+str(issuingID+1)+":9092\
			 --replication-factor "+str(replication)+" --partitions 1 --topic "+topicName, shell=True)         
		
		print(out)
		topicNodes.append(issuingNode)
	
	stopTime = time.time()
	totalTime = stopTime - startTime
	print("Successfully Created " + str(len(topicPlace)) + " Topics in " + str(totalTime) + " seconds")
	
	#starting Kafka-MySQL connector
	if mysqlPath != "":
		spawnKafkaMySQLConnector(net, prodDetailsList, mysqlPath)
		print("Kafka-MySQL connector instance created")

	# calculate end-to-end latency (after all setup are done)
	produceStartTime = time.time()

	spawnProducers(net, mSizeString, mRate, tClassString, nTopics, args, prodDetailsList, topicPlace)
	time.sleep(10)
	print("Producers created")

	spawnConsumers(net, consDetailsList, topicPlace)
	time.sleep(10)
	print("Consumers created")

	time.sleep(30)

	spawnSparkClients(net, sparkDetailsList)
	time.sleep(10)
	print("Spark Clients created")

	time.sleep(50)

	produceStopTime = time.time()
	endToEndLatency = produceStopTime - produceStartTime
	print("Time taken to process produced messages: " + str(endToEndLatency) + " seconds")

   

	timer = 0

	while timer < duration:
		time.sleep(10)
		print("Processing workload: "+str(int((timer/duration)*100))+"%")
		timer += 10

	print("Workload finished")