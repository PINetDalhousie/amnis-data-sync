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



def spawnConsumers(net, nTopics, cRate, args, consDetailsList, sparkSocket):

	fetchMinBytes = args.fetchMinBytes
	fetchMaxWait = args.fetchMaxWait
	sessionTimeout = args.sessionTimeout
	brokers = args.nBroker    
	mSizeString = args.mSizeString
	mRate = args.mRate    
	replication = args.replication  
	topicCheckInterval = args.topicCheckInterval   

	netNodes = {}
	if sparkSocket == 1:
		portId = 65450
	else:
		portId = 0

	for node in net.hosts:
		netNodes[node.name] = node
        
	for cons in consDetailsList:
		consNode = cons["nodeId"]
		consTopic = cons["consumeFromTopic"][0]
		consID = "h"+consNode      
		node = netNodes[consID]
		node.popen("python3 consumer.py "+str(node.name)+" "+str(nTopics)+" "+str(cRate)+" "+str(fetchMinBytes)+" "+str(fetchMaxWait)+" "
		+str(sessionTimeout)+" "+str(brokers)+" "+mSizeString+" "+str(mRate)+" "+str(replication)+" "+str(topicCheckInterval)+" "+str(portId)
		+" "+str(consTopic)+" &", shell=True)
		if sparkSocket == 1:        
			portId += 1

def spawnSparkClients(net, sparkDetailsList):
	netNodes = {}
	port = 12345

	for node in net.hosts:
		netNodes[node.name] = node

	for sprk in sparkDetailsList:
		sparkNode = sprk["nodeId"]
		sparkApp = sprk["applicationPath"]
		sparkTopic = sprk["topicsToConsume"][0]
		print("spark node: "+sparkNode)
		print("spark App: "+sparkApp)

		sprkID = "h"+sparkNode
		node = netNodes[sprkID]
		print("node is: "+str(node.name))
		print("port is: "+str(port))
		node.cmd("sudo ~/.local/bin/spark-submit "+sparkApp+" "+str(node.name)+" "+str(port), shell=True) 
		

def runLoad(net, args, topicPlace, prodDetailsList, consDetailsList, sparkDetailsList, topicWaitTime=100):

	sparkSocket = args.sparkSocket
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
		issuingID = randint(0, nHosts-1)
		issuingNode = net.hosts[issuingID]

		print("Creating topic "+topicName+" at broker "+str(issuingID+1))

		out = issuingNode.cmd("kafka/bin/kafka-topics.sh --create --bootstrap-server 10.0.0."+str(issuingID+1)+":9092\
			 --replication-factor "+str(replication)+" --partitions 1 --topic "+topicName, shell=True)         
		
		print(out)
		topicNodes.append(issuingNode)
	
	stopTime = time.time()
	totalTime = stopTime - startTime
	print("Successfully Created " + str(nTopics) + " Topics in " + str(totalTime) + " seconds")
	

	spawnProducers(net, mSizeString, mRate, tClassString, nTopics, args, prodDetailsList)
	time.sleep(10)
	print("Producers created")

	spawnConsumers(net, nTopics, consumerRate, args, consDetailsList, sparkSocket)
	time.sleep(10)
	print("Consumers created")

	time.sleep(30)

	spawnSparkClients(net, sparkDetailsList)
	time.sleep(10)
	print("Spark Clients created")

	time.sleep(30)
   

	timer = 0

	while timer < duration:
		time.sleep(10)
		print("Processing workload: "+str(int((timer/duration)*100))+"%")
		timer += 10

	print("Workload finished")