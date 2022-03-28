#!/usr/bin/python3

from mininet.net import Mininet
from mininet.cli import CLI

from random import seed, randint

import time
import os
import sys


def spawnProducers(net, mSizeString, mRate, tClassString, nTopics, args, producerPlace):

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
        
	for pNode in producerPlace:      
		prodID = "h"+str(pNode)
		prodNode = netNodes[prodID]
        
		nodeClass = randint(1,len(tClasses))
		nodeClassification[nodeClass].append(prodNode)

	#print("Node classification")
	#print(nodeClassification)

	#print("Message size: " + mSizeString)
	#print("Message rate: " + str(mRate))

	i=0

	for nodeList in nodeClassification.values():
		for node in nodeList:
			node.popen("python3 producer.py "+str(node)+" "+tClasses[i]+" "+mSizeString+" "+str(mRate)+" "+str(nTopics)+" "+str(acks)+" "+str(compression)+" "+str(batchSize)+" "+str(linger)+" "+str(requestTimeout)+" "+str(brokers)+" "+str(replication)+" "+str(messageFilePath)+" &", shell=True)
		i += 1


def spawnConsumers(net, nTopics, cRate, args, consumerPlace, sparkSocket):

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
        
	for cNode in consumerPlace:
		consID = "h"+str(cNode)      
		node = netNodes[consID]
		node.popen("python3 consumer.py "+str(node.name)+" "+str(nTopics)+" "+str(cRate)+" "+str(fetchMinBytes)+" "+str(fetchMaxWait)+" "+str(sessionTimeout)+" "+str(brokers)+" "+mSizeString+" "+str(mRate)+" "+str(replication)+" "+str(topicCheckInterval)+" "+str(portId)+" &", shell=True)
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


def runLoad(net, nTopics, replication, mSizeString, mRate, tClassString, consumerRate, duration, args, producerPlace, consumerPlace, sparkSocket, topicWaitTime=100):

	print("Start workload")

	seed(1)

	nHosts = len(net.hosts)
	print("Number of hosts: " + str(nHosts))
    
	#Create topics
	topicNodes = []
	startTime = time.time()
	for i in range(nTopics):
		issuingID = randint(0, nHosts-1)
		issuingNode = net.hosts[issuingID]
		
		print("Creating topic "+str(i)+" at broker "+str(issuingID+1))

		out = issuingNode.cmd("kafka/bin/kafka-topics.sh --create --bootstrap-server 10.0.0."+str(issuingID+1)+":9092 --replication-factor "+str(replication)+" --partitions 1 --topic topic-"+str(i), shell=True)
# 		issuingNode.popen("kafka/bin/kafka-topics.sh --create --bootstrap-server 10.0.0."+str(issuingID+1)+":9092 --replication-factor "+str(replication)+" --partitions "+str(nHosts)+" --topic topic-"+str(i)+" &", shell=True)        
		print(out)
		topicNodes.append(issuingNode)
	
	stopTime = time.time()
	totalTime = stopTime - startTime
	print("Successfully Created " + str(nTopics) + " Topics in " + str(totalTime) + " seconds")
	
	#topicWait = True
	#startTime = time.time()
	#totalTime = 0
	#for host in topicNodes:
	#    while topicWait:
	#        print("Checking Topic Creation for Host " + str(host.IP()) + "...")
	#        out = host.cmd("kafka/bin/kafka-topics.sh --list --bootstrap-server " + str(host.IP()) + ":9092", shell=True)
	#        stopTime = time.time()
	#        totalTime = stopTime - startTime
	#        if "topic-" in out:
	#            topicWait = False
	#            print(out)
	#        elif(totalTime > topicWaitTime):
	#            print("ERROR: Timed out waiting for topics to be created")
	#            sys.exit(1)
	#        else:
	#            time.sleep(10)
	#    topicWait = True
	#    
	#print("Successfully Created Topics in " + str(totalTime) + " seconds")
	

	spawnConsumers(net, nTopics, consumerRate, args, consumerPlace, sparkSocket)
	time.sleep(2)
	print("Consumers created")
    
	spawnClients(net, nTopics, consumerRate, args, consumerPlace)
	time.sleep(10)
	print("Clients created")    
    
	spawnProducers(net, mSizeString, mRate, tClassString, nTopics, args, producerPlace)
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











