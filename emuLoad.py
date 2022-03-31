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

def readProdConfig(prodConfigPath):
	f = open(prodConfigPath, "r")
	prodFile = f.readline()
	prodTopic = f.readline()

	f.close()

	return prodFile, prodTopic

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


	#accessing list of dictionaries
	#prodConfig = [{'nodeId': '1', 'producerType': 'SFST', 'produceFromFile': 'filePath1', 'produceInTopic': 'topic-0'}, {'nodeId': '2', 'producerType': 'SFST', 'produceFromFile': 'filePath1', 'produceInTopic': 'topic-0'}]
	# for i in prodDetailsList:
	# 	for (k,v) in zip(i.keys(), i.values()):
	# 		if k == 'nodeId':
	# 			pNode = v
	# 			prodID = "h"+pNode=0
	# for nodeList in nodeClassification.values():
	# 	for prod in zip(nodeList, producerConfigFile) :
	# 		prodFile, prodTopic = readProdConfig(prodConfig)

	# 		messageFilePath = prodFile.strip()
	# 		proc = node.popen("python3 producer.py "+str(node)+" "+tClasses[i]+" "+mSizeString+" "+str(mRate)+" "+str(nTopics)+" "+str(acks)+" "+str(compression)
	# 		+" "+str(batchSize)+" "+str(linger)+" "+str(requestTimeout)+" "+str(brokers)+" "+str(replication)+" "+str(messageFilePath)
	# 		+" "+str(prodTopic)+" &", shell=True) #, stdout=PIPE, stderr=PIPE)
	# 		""" (output, error) = proc.communicate()
	# 		print("output=")
	# 		print(output)
	# 		print(error) """
	# 	i += 1
	# 			prodNode = netNodes[prodID]
				
	# 			nodeClass = randint(1,len(tClasses))
	# 			nodeClassification[nodeClass].append(prodNode)

	j =0
	for j in prodDetailsList:
		j['tClasses'] = str(randint(1,len(tClasses)))
	print(*[(key,value) for i in prodDetailsList for (key,value) in zip(i.keys(), i.values())], sep = "\n")
		

	# for nodeList in nodeClassification.values():
	# 	for (node,prodConfig) in zip(nodeList, producerConfigFile) :
	# 		prodFile, prodTopic = readProdConfig(prodConfig)
	# 		print("node:"+str(node))
	# 		print("input file: "+prodFile.strip())
	# 		print("produce data in topic: "+prodTopic)
	# 		messageFilePath = prodFile.strip()
	# 		proc = node.popen("python3 producer.py "+str(node)+" "+tClasses[i]+" "+mSizeString+" "+str(mRate)+" "+str(nTopics)+" "+str(acks)+" "+str(compression)
	# 		+" "+str(batchSize)+" "+str(linger)+" "+str(requestTimeout)+" "+str(brokers)+" "+str(replication)+" "+str(messageFilePath)
	# 		+" "+str(prodTopic)+" &", shell=True) #, stdout=PIPE, stderr=PIPE)
	# 		""" (output, error) = proc.communicate()
	# 		print("output=")
	# 		print(output)
	# 		print(error) """
	# 	i += 1


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

	#h2.cmd("python3 kafka-python-consumer.py > consumed-data.txt", shell=True)
	#print("Data consumed")

# 	for node in net.hosts:
# 		if str(node.name) == "h2":        
# 			node.popen("python3 consumer.py "+str(node.name)+" "+str(nTopics)+" "+str(cRate)+" "+str(fetchMinBytes)+" "+str(fetchMaxWait)+" "+str(sessionTimeout)+" "+str(brokers)+" "+mSizeString+" "+str(mRate)+" "+str(replication)+" "+str(topicCheckInterval)+" &", shell=True)

	netNodes = {}
	if sparkSocket == 1:
		portId = 65450
	else:
		portId = 0

	for node in net.hosts:
		netNodes[node.name] = node
        
	# for (cNode, cTopicFile) in zip(consumerPlace, consumerTopicFile):
	# 	consTopic = readConsConfig(cTopicFile)
	# 	consID = "h"+str(cNode)      
	# 	node = netNodes[consID]
	# 	node.popen("python3 consumer.py "+str(node.name)+" "+str(nTopics)+" "+str(cRate)+" "+str(fetchMinBytes)+" "+str(fetchMaxWait)+" "
	# 	+str(sessionTimeout)+" "+str(brokers)+" "+mSizeString+" "+str(mRate)+" "+str(replication)+" "+str(topicCheckInterval)+" "+str(portId)
	# 	+" "+str(consTopic)+" &", shell=True)
	# 	if sparkSocket == 1:        
	# 		portId += 1        

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


        
def spawnClients(net, nTopics, cRate, args, consumerPlace):
	fetchMinBytes = args.fetchMinBytes
	fetchMaxWait = args.fetchMaxWait
	sessionTimeout = args.sessionTimeout
	brokers = args.nBroker    
	mSizeString = args.mSizeString
	mRate = args.mRate    
	replication = args.replication  
	topicCheckInterval = args.topicCheckInterval    
    
	netNodes = {}

	for node in net.hosts:
		netNodes[node.name] = node
        
	for cNode in consumerPlace:
		consID = "h"+str(cNode)      
		node = netNodes[consID]
		node.popen("python3 client.py "+str(node.name)+" "+str(nTopics)+" "+str(cRate)+" "+str(fetchMinBytes)+" "+str(fetchMaxWait)+" "+str(sessionTimeout)+" "+str(brokers)+" "+mSizeString+" "+str(mRate)+" "+str(replication)+" "+str(topicCheckInterval)+" &", shell=True)    


def runLoad(net, args, topicPlace, prodDetailsList, consDetailsList, topicWaitTime=100):

	sparkSocket = args.sparkSocket
	nTopics = args.nTopics
	replication = args.replication
	mSizeString = args.mSizeString
	mRate = args.mRate
	tClassString = args.tClassString
	consumerRate = args.consumerRate
	duration = args.duration


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
	

	spawnConsumers(net, nTopics, consumerRate, args, consDetailsList, sparkSocket)
	time.sleep(2)
	print("Consumers created")
    
	# spawnClients(net, nTopics, consumerRate, args, consumerPlace)
	# time.sleep(10)
	# print("Clients created")    
    
	spawnProducers(net, mSizeString, mRate, tClassString, nTopics, args, prodDetailsList)
	time.sleep(1)
	print("Producers created")

# 	spawnConsumers(net, nTopics, consumerRate, args)
# 	time.sleep(1)
# 	print("Consumers created")
    
# 	for i in range(nHosts):    
# 		consumer_groups = net.hosts[i].cmd("kafka/bin/kafka-consumer-groups.sh --bootstrap-server 10.0.0."+str(i+1)+":9092 --list", shell=True)
# 		print("output for "+str(i+1)+" node:"+consumer_groups)

	timer = 0

	while timer < duration:
		time.sleep(10)
		print("Processing workload: "+str(int((timer/duration)*100))+"%")
		timer += 10

	print("Workload finished")