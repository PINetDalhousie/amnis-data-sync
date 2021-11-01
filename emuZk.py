#!/usr/bin/python3

from mininet.net import Mininet

import os
import subprocess
import time


def configureZkCluster(zkPlace):
	print("Congigure Zookeeper cluster")

	clientPort = 2181

	propertyFile = open("kafka/config/zookeeper.properties", "r")
	zkProperties = propertyFile.read()

	for zkID in zkPlace:
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




def runZk(net, zkPlace):

	netNodes = {}

	for node in net.hosts:
		netNodes[node.name] = node

	for zNode in zkPlace:
		zID = "h"+str(zNode)

		startingHost = netNodes[zID]

		startingHost.popen("kafka/bin/zookeeper-server-start.sh kafka/config/zookeeper"+str(zNode)+".properties &", shell=True)
		time.sleep(10) 
		print("Created Zookeeper instance at node "+str(zNode))



def cleanZkState(zkPlace):

	for zkID in zkPlace:
		#Erase log directory
		os.system("sudo rm -rf kafka/zookeeper" + str(zkID) + "/")

		#Erase properties file
		os.system("sudo rm kafka/config/zookeeper" + str(zkID) + ".properties")










