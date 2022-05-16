#!/usr/bin/python3

from datetime import datetime
from mininet.net import Mininet
from mininet.cli import CLI
from mininet.node import Host

from random import seed, randint, choice

import logging
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
	script ='consumer.py '
	if args.singleConsumer:
		script = 'consumerSingle.py ' 

	#h2.cmd("python3 kafka-python-consumer.py > consumed-data.txt", shell=True)
	#print("Data consumed")

	for node in net.hosts:
		node.popen("python3 " + script +str(node.name)+" "+str(nTopics)+" "+str(cRate)+" "+str(fetchMinBytes)+" "+str(fetchMaxWait)+" "+str(sessionTimeout)+" "+str(brokers)+" "+mSizeString+" "+str(mRate)+" "+str(replication)+" "+str(topicCheckInterval)+" &", shell=True)



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

		out = issuingNode.cmd("kafka-3.1.0/bin/kafka-topics.sh --create --bootstrap-server 10.0.0."+str(issuingID+1)+":9092 --replication-factor "+str(replication)+" --partitions 1 --topic topic-"+str(i), shell=True)
# 		issuingNode.popen("kafka/bin/kafka-topics.sh --create --bootstrap-server 10.0.0."+str(issuingID+1)+":9092 --replication-factor "+str(replication)+" --partitions "+str(nHosts)+" --topic topic-"+str(i)+" &", shell=True)        
		print(out)
		topicNodes.append(issuingNode)
	
	stopTime = time.time()
	totalTime = stopTime - startTime
	print("Successfully Created " + str(nTopics) + " Topics in " + str(totalTime) + " seconds")
	


	spawnConsumers(net, nTopics, consumerRate, args)
	print(f"Consumers created at {str(datetime.now())}")
	time.sleep(5)	

	# Set the network delay back to to .graphml values before spawning producers so we get accurate latency
	if args.latencyAfterSetup:
		setNetworkDelay(net)
		time.sleep(1)

	spawnProducers(net, mSizeString, mRate, tClassString, nTopics, args)
	print(f"Producers created at {str(datetime.now())}")
	time.sleep(1)


	timer = 0
	disconnect = args.disconnectDuration > 0
	relocate = args.relocate

	if disconnect:
		disconnectTimer = args.disconnectDuration
		isDisconnected = False
		hosts = {k:v for k,v in net.topo.ports.items() if 'h' in k}
		seed()
		randomHost = choice(list(hosts.items()))				
		h = net.getNodeByName(randomHost[0])		
		s = net.getNodeByName(randomHost[1][1][0])		
		print(f"Host {h.name} to disconnect from switch {s.name} for {disconnectTimer}s")
	elif relocate:
		seed()
		hosts = {k:v for k,v in net.topo.ports.items() if 'h' in k}
		randomHost = choice(list(hosts.items()))
		h = net.getNodeByName(randomHost[0])		
		s = net.getNodeByName(randomHost[1][1][0])	
		switches = {k:v for k,v in net.topo.ports.items() if 's' in k}
		randomSwitch = choice(list(switches.items()))		
		# Check that the random switch is not the same as the first 
		while s.name == randomSwitch[0]:		
			randomSwitch = choice(list(switches.items()))	
		s2 = net.getNodeByName(randomSwitch[0])
		print(f"{h.name} to relocate from {s.name} to {s2.name}")

		

	print(f"Starting workload at {str(datetime.now())}")		

	while timer < duration:
		time.sleep(10)
		percentComplete = int((timer/duration)*100)
		print("Processing workload: "+str(percentComplete)+"%")
		if disconnect and percentComplete >= 10:
			if not isDisconnected:			
				disconnectHost(net, h, s)
				isDisconnected = True
			elif isDisconnected and disconnectTimer <= 0: 			
				reconnectHost(net, h, s)
				isDisconnected = False
				disconnect = False
			if isDisconnected:
				disconnectTimer -= 10
		elif relocate:
			delLink(net, h, s)
			addLink(net, h, s2)
			relocate = False
		timer += 10

	print(f"Workload finished at {str(datetime.now())}")	



def delLink(net, h, s):
	printLinksBetween(net, h, s)
	print(f"***********Deleting link from {h.name} <-> {s.name} at {str(datetime.now())}")								
	net.delLinkBetween(net.get(h.name), s)		
	printLinksBetween(net, h, s)
	net.pingAll()

def addLink(net, h, s2):
	print(f"***********Adding link from {h.name} <-> {s2.name} at {str(datetime.now())}")	
	link = net.addLink(net.get(h.name), s2)
	s2.attach(link.intf2) 
	net.configHosts()
	printLinksBetween(net, h, s2)
	net.pingAll()

def disconnectHost(net, h, s):		
	print(f"***********Setting link down from {h.name} <-> {s.name} at {str(datetime.now())}")						
	logging.info('Disconnected %s <-> %s at %s', h.name, s.name,  str(datetime.now()))
	net.configLinkStatus(s.name, h.name, "down")		
	net.pingAll()

def reconnectHost(net, h, s):
	print(f"***********Setting link up from {h.name} <-> {s.name} at {str(datetime.now())}")
	logging.info('Connected %s <-> %s at %s', h.name, s.name,  str(datetime.now()))
	net.configLinkStatus(s.name, h.name, "up")									
	net.pingAll()

def setNetworkDelay(net, newDelay=None):
	nodes = net.switches + net.hosts	 
	print(f"Checking network nodes to set new delay")
	logging.info('Setting nework delay at %s', str(datetime.now()))
	for node in nodes:
		for intf in node.intfList(): # loop on interfaces of node
			if intf.link: # get link that connects to interface (if any)
				# Check if the link is switch to switch
				if intf.link.intf1.name[0] == 's' and intf.link.intf2.name[0] == 's':
					if newDelay is None:
						# Use the values from graph.ml
						intf1Delay = intf.link.intf1.params['delay']
						intf.link.intf1.config(delay=intf1Delay)
						intf2Delay = intf.link.intf2.params['delay']
						intf.link.intf2.config(delay=intf2Delay)
					else:						
						# Use the passed in param				
						intf.link.intf1.config(delay=newDelay)
						intf.link.intf2.config(delay=newDelay)
					#print(intf.link.intf1.node.lastCmd + "\n" + intf.link.intf2.node.lastCmd)					