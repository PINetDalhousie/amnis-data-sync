#!/usr/bin/python3

from datetime import datetime
from mininet.net import Mininet
from mininet.cli import CLI
from mininet.node import Host

from random import seed, randint

import time
import os
import sys


def spawnProducers(net, mSizeString, mRate, tClassString, nTopics, args):

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
			node.popen("python3 producer.py "+str(node)+" "+tClasses[i]+" "+mSizeString+" "+str(mRate)+" "+str(nTopics)+" "+str(acks)+" "+str(compression)+" "+str(batchSize)+" "+str(linger)+" "+str(requestTimeout)+" "+str(brokers)+" "+str(replication)+" "+str(messageFilePath)+" &", shell=True)
		i += 1


def spawnConsumers(net, nTopics, cRate, args):

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

	for node in net.hosts:
		node.popen("python3 consumer.py "+str(node.name)+" "+str(nTopics)+" "+str(cRate)+" "+str(fetchMinBytes)+" "+str(fetchMaxWait)+" "+str(sessionTimeout)+" "+str(brokers)+" "+mSizeString+" "+str(mRate)+" "+str(replication)+" "+str(topicCheckInterval)+" &", shell=True)



def printLinksBetween(net , n1, n2):	
	linksBetween = net.linksBetween(n1, n2)
	print(f"Links between {n1.name} {n2.name} {linksBetween}")	
	if len(linksBetween) > 0:
		for link in linksBetween:			
			print(link.intf1)
			print(link.intf2)
	

def runLoad(net, nTopics, replication, mSizeString, mRate, tClassString, consumerRate, duration, args, topicWaitTime=100):

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
	

	spawnConsumers(net, nTopics, consumerRate, args)
	time.sleep(1)
	print("Consumers created")
    
	spawnProducers(net, mSizeString, mRate, tClassString, nTopics, args)
	time.sleep(1)
	print("Producers created")

# 	spawnConsumers(net, nTopics, consumerRate, args)
# 	time.sleep(1)
# 	print("Consumers created")
    
# 	for i in range(nHosts):    
# 		consumer_groups = net.hosts[i].cmd("kafka/bin/kafka-consumer-groups.sh --bootstrap-server 10.0.0."+str(i+1)+":9092 --list", shell=True)
# 		print("output for "+str(i+1)+" node:"+consumer_groups)

	timer = 0

	if args.disconnect or args.relocate:
		# Get hosts and switches
		hosts = []	
		for h in net.hosts:
			hosts.append(net.getNodeByName(h.name))
		
		switches = []
		for s in net.switches:
			switches.append(net.getNodeByName(s.name))

		#TODO: Pick random node to disconnect
		k = net.keys()
		h = hosts[1]
		s = switches[1]		
		s2 = switches[2]	
		st = 1
		dt = 1

	print(f"Starting workload at {str(datetime.now())}")

	while timer < duration:
		time.sleep(10)
		percentComplete = int((timer/duration)*100)
		print("Processing workload: "+str(percentComplete)+"%")
		if args.disconnect:
			processDisconnect(percentComplete, net, h, s)
		elif args.relocate:
			processRelocate(percentComplete, net, h, s, s2)
		timer += 10

	print(f"Workload finished at {str(datetime.now())}")	





def processRelocate(percentComplete, net, h, s, s2):
	if percentComplete == 10:
		net.pingAll()
		printLinksBetween(net, h, s)
		print(f"***********Deleting link from {h.name} <-> {s.name} at {str(datetime.now())}")								
		net.delLinkBetween(net.get(h.name), s)		
		printLinksBetween(net, h, s)
		net.pingAll()
	elif percentComplete == 40:		
		print(f"***********Adding link from {h.name} <-> {s2.name} at {str(datetime.now())}")
		link = net.addLink(net.get(h.name), s2, 4, 4)					
		s2.attach(link.intf2) 
		net.configHosts()
		printLinksBetween(net, h, s2)
		net.pingAll()	


def processDisconnect(percentComplete, net, h, s):
	if percentComplete == 10:
		net.pingAll()
		printLinksBetween(net, h, s)
		print(f"***********Setting link down from {h.name} <-> {s.name} at {str(datetime.now())}")						
		net.configLinkStatus(s.name, h.name, "down")	
		printLinksBetween(net, h, s)
		net.pingAll()
	elif percentComplete == 40:
		print(f"***********Setting link up from {h.name} <-> {s.name} at {str(datetime.now())}")
		net.configLinkStatus(s.name, h.name, "up")								
		printLinksBetween(net, h, s)
		net.pingAll()








