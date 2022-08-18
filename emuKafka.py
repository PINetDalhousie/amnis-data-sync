#!/usr/bin/python3

from mininet.net import Mininet

import os
import sys
import subprocess
import time
import networkx as nx

def readTopicConfig(topicConfigPath):
	allTopics = []
	topicDetails = {}
	
	f = open(topicConfigPath, "r")
	for line in f:
		topicName = line.split(' broker:')[0].strip()
		topicBroker = line.split(' broker:')[1].strip()
		topicDetails = {"topicName": topicName, "topicBroker": topicBroker}
		allTopics.append(topicDetails)
	
	f.close()
	# print(*allTopics)

	return allTopics

def readProdConfig(prodType, prodConfig):
	# if prodType == 'SFST':
	prodFile = prodConfig.split(",")[0]     #prodFile will hold the file path/directory path based on producer type SFST or MFST respectively
	prodTopic = prodConfig.split(",")[1]

	return prodFile, prodTopic

def readConsConfig(consConfig):
	#topic list contains the topics from where the consumer will consume
	consTopic = consConfig.split(",")		

	return consTopic


def configureKafkaCluster(brokerPlace, zkPlace, args):
	print("Configure kafka cluster")

	propertyFile = open("kafka/config/server.properties", "r")
	serverProperties = propertyFile.read()

	for bID in brokerPlace:
		os.system("sudo mkdir kafka/kafka" + str(bID) + "/")

		bProperties = serverProperties
		bProperties = bProperties.replace("broker.id=0", "broker.id="+str(bID))
		bProperties = bProperties.replace(
			"#advertised.listeners=PLAINTEXT://your.host.name:9092", 
			"advertised.listeners=PLAINTEXT://10.0.0." + str(bID) + ":9092")
		bProperties = bProperties.replace("log.dirs=/tmp/kafka-logs",
			"log.dirs=./kafka/kafka" + str(bID))

		bProperties = bProperties.replace("#replica.fetch.wait.max.ms=500", "replica.fetch.wait.max.ms="+str(args.replicaMaxWait))
		bProperties = bProperties.replace("#replica.fetch.min.bytes=1", "replica.fetch.min.bytes="+str(args.replicaMinBytes))

		#Specify zookeeper addresses to connect
		zkAddresses = ""
		zkPort = 2181

# 		for i in range(len(zkPlace)-1):
# 			zkAddresses += "localhost:"+str(zkPort)+","
# 			zkPort += 1
    
		for i in range(len(zkPlace)-1):
			zkAddresses += "10.0.0." + str(zkPlace[i]) + ":" +str(zkPort)+","
			zkPort += 1

# 		zkAddresses += "localhost:"+str(zkPort)
		zkAddresses += "10.0.0."+str(zkPlace[-1])+ ":" +str(zkPort)
		print("zk connect: " + zkAddresses)

		bProperties = bProperties.replace(
			"zookeeper.connect=localhost:2181",
			"zookeeper.connect="+zkAddresses)

		#bProperties = bProperties.replace(
		#	"zookeeper.connection.timeout.ms=18000",
		#	"zookeeper.connection.timeout.ms=30000")

		bFile = open("kafka/config/server" + str(bID) + ".properties", "w")
		bFile.write(bProperties)
		bFile.close()

	propertyFile.close()


def placeKafkaBrokers(net, inputTopoFile, onlySpark):
	print(onlySpark)
	brokerPlace = []
	zkPlace = []

	topicPlace = []

	prodDetailsList = []
	prodDetails = {}
	prodDetailsKeys = {"nodeId", "producerType","produceFromFile", "produceInTopic"}

	consDetailsList = []
	consDetails = {}
	consDetailsKeys = {"nodeId", "consumeFromTopic"}

	#Read topo information
	try:
		inputTopo = nx.read_graphml(inputTopoFile)
	except Exception as e:
		print("ERROR: Could not read topo properly.")
		print(str(e))
		sys.exit(1)

	#Read topic information
	if onlySpark == 0: 
		topicConfigPath = inputTopo.graph["topicConfig"]
		print("topic config directory: " + topicConfigPath)
		topicPlace = readTopicConfig(topicConfigPath)
		# print("Topic(s): ")
		# print(*topicPlace)
	
	#Read nodewise broker, zookeeper, producer, consumer information
	for node, data in inputTopo.nodes(data=True):  
		if node[0] == 'h':
			# print("node id: "+node[1])
			#print("zk : "+str(data["zookeeper"]))
			if 'zookeeper' in data: 
				zkPlace.append(node[1]) 
			if 'broker' in data: 
				brokerPlace.append(node[1])
			if 'producerType' in data: 
				prodFile, prodTopic = readProdConfig(data["producerType"], data["producerConfig"])
				prodDetails = {"nodeId": node[1], "producerType": data["producerType"],\
					"produceFromFile":prodFile, "produceInTopic": prodTopic}
				prodDetailsList.append(prodDetails)

			if 'consumerConfig' in data: 
				consTopics = readConsConfig(data["consumerConfig"])
				consDetails = {"nodeId": node[1], "consumeFromTopic": consTopics}
				consDetailsList.append(consDetails)
            
	# print("zookeepers:\n")
	# print(*zkPlace)
	# print("brokers: \n")
	# print(*brokerPlace)

	# print("producer details")
	# print(*prodDetailsList)

	# print("consumer details")
	# print(*consDetailsList)

	return brokerPlace, zkPlace, topicPlace, prodDetailsList, consDetailsList




def runKafka(net, brokerPlace, brokerWaitTime=200):

	netNodes = {}

	for node in net.hosts:
		netNodes[node.name] = node
		
	startTime = time.time()
	for bNode in brokerPlace:
		bID = "h"+str(bNode)

		startingHost = netNodes[bID]
		
		print("Creating Kafka broker at node "+str(bNode))

		startingHost.popen("kafka/bin/kafka-server-start.sh kafka/config/server"+str(bNode)+".properties &", shell=True)
		
		time.sleep(1)

# 	brokerWait = True
# 	totalTime = 0
# 	for bNode in brokerPlace:
# 	    while brokerWait:
# 	        print("Testing Connection to Broker " + str(bNode) + "...")
# 	        out, err, exitCode = startingHost.pexec("nc -z -v 10.0.0." + str(bNode) + " 9092")
# 	        stopTime = time.time()
# 	        totalTime = stopTime - startTime
# 	        if(exitCode == 0):
# 	            brokerWait = False
# 	        #elif(totalTime > brokerWaitTime):
# 	        #    print("ERROR: Timed out waiting for Kafka brokers to start")
# 	        #    sys.exit(1)
# 	        else:
# 	            print("Waiting for Broker " + str(bNode) + " to Start...")
# 	            time.sleep(10)
# 	    brokerWait = True
# 	print("Successfully Created Kafka Brokers in " + str(totalTime) + " seconds")


def cleanKafkaState(brokerPlace):
	for bID in brokerPlace:
		os.system("sudo rm -rf kafka/kafka" + str(bID) + "/")
		os.system("sudo rm -f kafka/config/server" + str(bID) + ".properties")









