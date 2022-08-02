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

def spawnProducers(net, mSizeString, mRate, tClassString, nTopics, args, prodDetailsList):

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

		node.popen("python3 producer.py "+nodeId+" "+tClasses+" "+mSizeString+" "+str(mRate)+" "+str(nTopics)+" "+str(acks)+" "+str(compression)\
		+" "+str(batchSize)+" "+str(linger)+" "+str(requestTimeout)+" "+str(brokers)+" "+str(replication)+" "+messageFilePath\
		+" "+prodTopic+" &", shell=True)



def spawnConsumers(net, consDetailsList):

	netNodes = {}

	for node in net.hosts:
		netNodes[node.name] = node
        
	for cons in consDetailsList:
		consNode = cons["nodeId"]
		topicName = cons["consumeFromTopic"][0]
		consID = "h"+consNode      
		node = netNodes[consID]
		node.popen("python3 consumer.py "+str(node.name)+" "+str(topicName)+" &", shell=True)


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
		# 			+" "+str(node.name)+" "+sparkInputFrom+" "+sparkOutputTo, shell=True) 

		# out= node.cmd("sudo ~/.local/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 "+sparkApp\
		# 			+" "+str(node.name)+" "+sparkOutputTo, shell=True) 

		out= node.cmd("sudo /home/monzurul/.local/lib/python3.8/site-packages/pyspark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 "+sparkApp\
					+" "+str(node.name)+" "+sparkOutputTo, shell=True) 
		print(out)
		
def spawnKafkaMySQLConnector(net, prodDetailsList):
	netNodes = {}

	for node in net.hosts:
		netNodes[node.name] = node
	
	connNode = prodDetailsList[0]["nodeId"]
	connID = "h"+connNode      
	node = netNodes[connID]

	print("=========")
	print("connector starts on node: "+connID)
	
	node.popen("sudo kafka/bin/connect-standalone.sh kafka/config/connect-standalone.properties kafka/config/mysql-bulk-sink.properties > logs/connectorOutput.txt &", shell=True)
	

def runLoad(net, args, topicPlace, prodDetailsList, consDetailsList, sparkDetailsList, topicWaitTime=100):
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
    
	#Create topics
	topicNodes = []
	startTime = time.time()

	for topicName in topicPlace:
		#Creating all topics in broker 1
		issuingID = 0    #randint(0, nHosts-1)
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
	if args.mysqlConnector:
		spawnKafkaMySQLConnector(net, prodDetailsList)
		print("Kafka-MySQL connector instance created")

	spawnProducers(net, mSizeString, mRate, tClassString, nTopics, args, prodDetailsList)
	time.sleep(10)
	print("Producers created")

	spawnConsumers(net, consDetailsList)
	time.sleep(10)
	print("Consumers created")

	time.sleep(30)

	spawnSparkClients(net, sparkDetailsList)
	time.sleep(10)
	print("Spark Clients created")

	time.sleep(60)
   

	timer = 0

	while timer < duration:
		time.sleep(10)
		print("Processing workload: "+str(int((timer/duration)*100))+"%")
		timer += 10

	print("Workload finished")