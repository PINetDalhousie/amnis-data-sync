#!/usr/bin/python3

from mininet.net import Mininet

import os
import sys
import subprocess
import time


def configureZkCluster(zkPlace):
	print("Congigure Zookeeper cluster")

	clientPort = 2181

	propertyFile = open("kafka/config/zookeeper.properties", "r")
	zkProperties = propertyFile.read()

	for zkID in zkPlace:
		print("in emuZk configureZkCluster with ZK: "+ str(zkID))        
		os.system("sudo mkdir kafka/zookeeper" + str(zkID) + "/")
		os.system("sudo mkdir kafka/zookeeper" + str(zkID) + "/data/")
		os.system("sudo mkdir kafka/zookeeper" + str(zkID) + "/logs/")

		os.system("echo \""+str(zkID)+"\" > kafka/zookeeper"+str(zkID)+"/data/myid")

		newProperties = zkProperties
		newProperties = newProperties.replace("dataDir=/tmp/zookeeper",
			"dataDir=./kafka/zookeeper"+str(zkID)+"/data\n"\
			"dataLogDir=./kafka/zookeeper"+str(zkID)+"/logs")
		newProperties = newProperties.replace("clientPort=2181",
			"clientPort="+str(clientPort))

		newProperties += \
			"#\n" \
			"# keeps a heartbeat of zookeeper in milliseconds\n" \
			"tickTime=2000\n" \
			"#\n" \
			"# time for initial synchronization (i.e., within each of the nodes in 				 the quorum needs to connect to the leader)\n" \
			"initLimit=10\n" \
			"#\n" \
			"# time for sending a request and receiving an acknowledgement\n" \
			"syncLimit=5\n" \
			"#\n"

		newProperties += \
			"#\n" \
			"# Define servers ip and internal ports to Zookeeper\n"

		for i in range(len(zkPlace)):
			newProperties += "server."+str(i+1)+"=10.0.0."+str(zkPlace[i])+":2888:3888\n"
			
		newProperties += "#\n"

		clientPort += 1

		zkFile = open("kafka/config/zookeeper" + str(zkID) + ".properties", "w")
		zkFile.write(newProperties)
		zkFile.close()

	propertyFile.close()




def runZk(net, zkPlace, zkWaitTime=100):

	netNodes = {}

	for node in net.hosts:
		netNodes[node.name] = node
	
	startTime = time.time()
	for zNode in zkPlace:
		zID = "h"+str(zNode)

		startingHost = netNodes[zID]
		
		print("Creating Zookeeper instance at node "+str(zNode))

		startingHost.popen("kafka/bin/zookeeper-server-start.sh kafka/config/zookeeper"+str(zNode)+".properties &", shell=True)
		time.sleep(1)
	
	zkWait = True
	totalTime = 0
	clientPort = 2181
	for zNode in zkPlace:
	    while zkWait:
	        print("Testing Connection to Zookeeper at node " + str(zNode) + "...")
	        zID = "h"+str(zNode)
	        startingHost = netNodes[zID]
	        out, err, exitCode = startingHost.pexec("nc -z -v 10.0.0." + str(zNode) + " " + str(clientPort))
	        stopTime = time.time()
	        totalTime = stopTime - startTime
	        if(exitCode == 0):
	            zkWait = False
	        #elif(totalTime > zkWaitTime):
	        #    print("ERROR: Timed out waiting for zookeeper instances to start")
	        #    sys.exit(1)
	        else:
	            print("Waiting for Zookeeper at node " + str(zNode) + " to Start...")
	            time.sleep(10)
	    zkWait = True
	    clientPort += 1
	print("Successfully Created Zookeeper Instances in " + str(totalTime) + " seconds")



def cleanZkState(zkPlace):

	for zkID in zkPlace:
		#Erase log directory
		os.system("sudo rm -rf kafka/zookeeper" + str(zkID) + "/")

		#Erase properties file
		os.system("sudo rm -f kafka/config/zookeeper" + str(zkID) + ".properties")










