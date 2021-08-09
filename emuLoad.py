#!/usr/bin/python3

from mininet.net import Mininet
from mininet.cli import CLI

from random import seed, randint

import time
import os


def spawnProducers(net, mSizeString, mRate, tClassString, nTopics):

	tClasses = tClassString.split(',')
	#print("Traffic classes: " + str(tClasses))

	nodeClassification = {}

	classID = 1

	for tClass in tClasses:
		nodeClassification[classID] = []
		classID += 1
	
	#Distribute nodes among classes
	for node in net.hosts:
		nodeClass = randint(1,len(tClasses))
		nodeClassification[nodeClass].append(node)

	#print("Node classification")
	#print(nodeClassification)

	#print("Message size: " + mSizeString)
	#print("Message rate: " + str(mRate))

	i=0

	for nodeList in nodeClassification.values():
		for node in nodeList:
			node.popen("python3 producer.py "+str(node)+" "+tClasses[i]+" "+mSizeString+" "+str(mRate)+" "+str(nTopics)+" &", shell=True)

		i += 1


def spawnConsumers(net, nTopics, rate):
	#h2.cmd("python3 kafka-python-consumer.py > consumed-data.txt", shell=True)
	#print("Data consumed")

	for node in net.hosts:
		node.popen("python3 consumer.py "+str(node.name)+" "+str(nTopics)+" "+str(rate)+" &", shell=True)


def runLoad(net, nTopics, mSizeString, mRate, tClassString, consumerRate, duration):

	print("Start workload")

	seed(1)

	nHosts = len(net.hosts)
	print("Number of hosts: " + str(nHosts))

	#Create topics
	for i in range(nTopics):
		issuingID = randint(0, nHosts-1)
		issuingNode = net.hosts[issuingID]

		issuingNode.popen("kafka/bin/kafka-topics.sh --create --bootstrap-server 10.0.0."+str(issuingID+1)+":9092 --replication-factor 1 --partitions 1 --topic topic-"+str(i)+" &", shell=True)
		print("Creating topic "+str(i)+" at broker "+str(issuingID+1))
	
	time.sleep(15)
	print("Topics created")	

	h1=net.hosts[0]
	h2=net.hosts[1]

	#h1.cmd("kafka/bin/kafka-topics.sh --create --bootstrap-server 10.0.0.1:9092 --replication-factor 1 --partitions 1 --topic example-topic", shell=True)
	#time.sleep(10)
	#print("Created a topic")

	out = h1.cmd("kafka/bin/kafka-topics.sh --list --bootstrap-server 10.0.0.1:9092", shell=True)
	print("#######################")
	print(out)

	spawnProducers(net, mSizeString, mRate, tClassString, nTopics)
	time.sleep(1)
	print("Producers created")

	spawnConsumers(net, nTopics, consumerRate)
	time.sleep(1)
	print("Consumers created")

	timer = 0

	while timer < duration:
		time.sleep(10)
		print("Processing workload: "+str(int((timer/duration)*100))+"%")
		timer += 10

	print("Workload finished")











