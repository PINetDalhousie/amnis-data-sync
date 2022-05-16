#!/usr/bin/python3

from mininet.net import Mininet

import os
import sys
import subprocess
import time

def configureKafkaCluster(brokerPlace, zkPlace, args):
	print("Configure kafka cluster")

	propertyFile = open("kafka-3.1.0/config/kraft/server.properties", "r")
	serverProperties = propertyFile.read()
	controllerPort = 9093

	for bID in brokerPlace:
		os.system("sudo mkdir kafka-3.1.0/kafka" + str(bID) + "/")

		bProperties = serverProperties
		# bProperties = bProperties.replace("node.id=1", "node.id="+str(bID))
		# #bProperties = bProperties.replace("controller.quorum.voters=1@localhost", "controller.quorum.voters="+str(bID)+"@"+"10.0.0." + str(bID))
		# bProperties = bProperties.replace(
		# 	"listeners=PLAINTEXT://localhost:9092,CONTROLLER://:9093", 
		# 	"listeners=PLAINTEXT://10.0.0." + str(bID) + ":9092,CONTROLLER://localhost:9093")
		# controllerPort += 1
		# bProperties = bProperties.replace(
		# 	"advertised.listeners=PLAINTEXT://localhost:9092", 
		# 	"advertised.listeners=PLAINTEXT://10.0.0." + str(bID) + ":9092")
		# bProperties = bProperties.replace("log.dirs=/tmp/kraft-combined-logs",
		# 	"log.dirs=./kafka-3.1.0-logs/kafka" + str(bID))
		
			

		#bProperties = bProperties.replace("#replica.fetch.wait.max.ms=500", "replica.fetch.wait.max.ms="+str(args.replicaMaxWait))
		#bProperties = bProperties.replace("#replica.fetch.min.bytes=1", "replica.fetch.min.bytes="+str(args.replicaMinBytes))

		# #Specify zookeeper addresses to connect
		# zkAddresses = ""
		# zkPort = 2181

		# for i in range(len(zkPlace)-1):
		# 	zkAddresses += "localhost:"+str(zkPort)+","
		# 	zkPort += 1

		# zkAddresses += "localhost:"+str(zkPort)

		# bProperties = bProperties.replace(
		# 	"zookeeper.connect=localhost:2181",
		# 	"zookeeper.connect="+zkAddresses)

		#bProperties = bProperties.replace(
		#	"zookeeper.connection.timeout.ms=18000",
		#	"zookeeper.connection.timeout.ms=30000")

		#bFile = open("kafka-3.1.0/config/kraft/server" + str(bID) + ".properties", "w")
		#bFile.write(bProperties)
		#bFile.close()

	propertyFile.close()



def placeKafkaBrokers(net, nBroker, nZk):

	brokerPlace = []
	zkPlace = []

	if nBroker < 0 or nBroker > len(net.hosts):
		print("ERROR: Cannot support specified number of broker instances.");
		sys.exit(1)
	elif nZk < 0 or nZk > len(net.hosts):
		print("ERROR: Cannot support specified number of Zookeeper instances.");
		sys.exit(1)	

	if nBroker == len(net.hosts):
		for i in range(nBroker):
			brokerPlace.append(i+1)
	else:
		print("ERROR: Support for broker placement will be added in the future. Please consider setting the number of brokers to the number of end hosts in your network.")
		sys.exit(1)

	if nZk == len(net.hosts):
		for i in range(nZk):
			zkPlace.append(i+1)
	else:
		print("ERROR: Support for zookeeper placement will be added in the future. Please consider setting the number of zookeeper instances to the number of end hosts in your network.")
		sys.exit(1)

	return brokerPlace, zkPlace



def runKafka(net, brokerPlace, brokerWaitTime=200):

	netNodes = {}
	uuid = 'JmL9ihFRQmSabrNWrYSpjg'

	for node in net.hosts:
		netNodes[node.name] = node
		
	startTime = time.time()
	for bNode in brokerPlace:
		bID = "h"+str(bNode)
		startingHost = netNodes[bID]		
		print("Setting Kafka storage for node "+str(bNode))
		startingHost.popen("kafka-3.1.0/bin/kafka-storage.sh format -t "+ uuid +"-c kafka-3.1.0/config/kraft/server"+str(bNode)+".properties &", shell=True)		
		time.sleep(1)		

	for bNode in brokerPlace:
		bID = "h"+str(bNode)
		startingHost = netNodes[bID]		
		print("Creating Kafka broker at node "+str(bNode))		
		startingHost.popen("kafka-3.1.0/bin/kafka-server-start.sh kafka-3.1.0/config/kraft/server"+str(bNode)+".properties &", shell=True)
		
		time.sleep(1)

	brokerWait = True
	totalTime = 0	
	for bNode in brokerPlace:
		while brokerWait:
			print("Testing Connection to Broker " + str(bNode) + "...")
			out, err, exitCode = startingHost.pexec("nc -z -v 10.0.0.1 9092")
			out, err, exitCode = startingHost.pexec("nc -z -v 10.0.0.1 19092")
			out, err, exitCode = startingHost.pexec("nc -z -v localhost 9092")
			out, err, exitCode = startingHost.pexec("nc -z -v localhost 19092")

			out, err, exitCode = startingHost.pexec("nc -z -v 10.0.0.2 9093")
			out, err, exitCode = startingHost.pexec("nc -z -v 10.0.0.2 19093")
			out, err, exitCode = startingHost.pexec("nc -z -v localhost 9093")
			out, err, exitCode = startingHost.pexec("nc -z -v localhost 19093")
			
			out, err, exitCode = startingHost.pexec("nc -z -v 10.0.0.3 9094")
			out, err, exitCode = startingHost.pexec("nc -z -v 10.0.0.3 19094")
			out, err, exitCode = startingHost.pexec("nc -z -v localhost 9094")
			out, err, exitCode = startingHost.pexec("nc -z -v localhost 19094")
			
			stopTime = time.time()
			totalTime = stopTime - startTime
			if(exitCode == 0):
				brokerWait = False
	        #elif(totalTime > brokerWaitTime):
	        #    print("ERROR: Timed out waiting for Kafka brokers to start")
	        #    sys.exit(1)
			else:
				print("Waiting for Broker " + str(bNode) + " to Start...")
				time.sleep(10)
		brokerWait = True
	print("Successfully Created Kafka Brokers in " + str(totalTime) + " seconds")


def cleanKafkaState(brokerPlace):
	for bID in brokerPlace:
		os.system("sudo rm -rf kafka-3.1.0/kafka" + str(bID) + "/")
		#os.system("sudo rm -f kafka-3.1.0/config/kraft/server" + str(bID) + ".properties")









