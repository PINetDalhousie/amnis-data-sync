#!/usr/bin/python3

from mininet.net import Mininet

import os
import sys
import subprocess
import time

def configureKraftCluster(brokerPlace):
	print("Configure kraft cluster")

	propertyFile = open("kafka/config/kraft/server.properties", "r")
	serverProperties = propertyFile.read()

	quorumAddresses = ""    
	kraftPort = 2180  
    
	for i in range(len(brokerPlace)):
		quorumAddresses += str(i+1) + "@10.0.0." + str(i+1) + ":"+str(kraftPort+(i+1)) + ","

	serverProperties = serverProperties.replace(
		"controller.quorum.voters=1@localhost:9093",
		"controller.quorum.voters="+quorumAddresses[:-1])    

	for bID in brokerPlace:     
		os.system("sudo mkdir kafka/kraft" + str(bID) + "/")

		bProperties = serverProperties
		bProperties = bProperties.replace("node.id=1", "node.id="+str(bID))
        
		bProperties = bProperties.replace(
			"listeners=PLAINTEXT://:9092,CONTROLLER://:9093",
			"listeners=PLAINTEXT://10.0.0." + str(bID) + ":9092,CONTROLLER://10.0.0." + str(bID) + ":" + str(kraftPort+bID) )
		bProperties = bProperties.replace(
			"advertised.listeners=PLAINTEXT://localhost:9092",
			"advertised.listeners=PLAINTEXT://10.0.0." + str(bID) + ":9092")
        
		bProperties = bProperties.replace("log.dirs=/tmp/kraft-combined-logs",
			"log.dirs=./kafka/kraft" + str(bID))
# 			"log.dirs=./tmp/kraft" + str(bID))                                          


# 		bProperties = bProperties.replace(
# 			"controller.quorum.voters=1@localhost:9093",
# 			"controller.quorum.voters="+quorumAddresses)


		bFile = open("kafka/config/kraft/server" + str(bID) + ".properties", "w")
		bFile.write(bProperties)
		bFile.close()

	propertyFile.close()


def placeKraftBrokers(net, nBroker):

	brokerPlace = []

	if nBroker < 0 or nBroker > len(net.hosts):
		print("ERROR: Cannot support specified number of broker instances.");
		sys.exit(1)

	if nBroker == len(net.hosts):
		for i in range(nBroker):
			brokerPlace.append(i+1)
	else:
		print("ERROR: Support for broker placement will be added in the future. Please consider setting the number of brokers to the number of end hosts in your network.")
		sys.exit(1)

	return brokerPlace



def runKraft(net, brokerPlace, brokerWaitTime=100):

	netNodes = {}

	for node in net.hosts:
		netNodes[node.name] = node
    
	uuID = netNodes["h1"].cmd("kafka/bin/kafka-storage.sh random-uuid", shell=True)    
	print("uuID: " + str(uuID))    
        
	for bNode in brokerPlace:
		bID = "h"+str(bNode)

		startingHost = netNodes[bID]
		print(startingHost)    
        
# configuring the storage directory        
		storageCheck = startingHost.cmd("kafka/bin/kafka-storage.sh format --config kafka/config/kraft/server" + str(bNode) +".properties " + "--cluster-id " + str(uuID), shell=True)
 
		print(storageCheck)        

# starting the kraft servers        
		startingHost.popen("kafka/bin/kafka-server-start.sh kafka/config/kraft/server"+str(bNode)+".properties &", shell=True)
		
		print("Creating Kafka broker at node "+str(bNode))

	brokerWait = True
	startTime = time.time()
	totalTime = 0
	for bNode in brokerPlace:
	    while brokerWait:
	        print("Testing Connection to Broker " + str(bNode) + "...")
	        out, err, exitCode = startingHost.pexec("nc -z -v 10.0.0." + str(bNode) + " 9092")
	        stopTime = time.time()
	        totalTime = stopTime - startTime
	        if(exitCode == 0):
	            brokerWait = False
	        elif(totalTime > brokerWaitTime):
	            print("ERROR: Timed out waiting for Kafka brokers to start")
	            sys.exit(1)
	        else:
	            time.sleep(10)
	    brokerWait = True
	print("Successfully Created Kafka Brokers in " + str(totalTime) + " seconds")


def cleanKraftState(brokerPlace):
	for bID in brokerPlace:
		os.system("sudo rm -rf kafka/kraft" + str(bID) + "/")
		os.system("sudo rm kafka/config/kraft/server" + str(bID) + ".properties")









