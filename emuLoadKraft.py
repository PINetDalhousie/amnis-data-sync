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

from emuLogs import ZOOKEEPER_LOG_FILE


def spawnProducers(net, mSizeString, mRate, tClassString, nTopics, args):

	acks = args.acks
	compression = args.compression
	batchSize = args.batchSize
	linger = args.linger
	requestTimeout = args.requestTimeout
	brokers = args.nBroker
	replication = args.replication 
	messageFilePath = args.messageFilePath   
	ssl = args.ssl

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
			node.popen("python3 producer.py "+str(node)+" "+tClasses[i]+" "+mSizeString+" "+str(mRate)+" "+str(nTopics)+" "+str(acks)+" "+str(compression)+" "+str(batchSize)+" "+str(linger)+" "+str(requestTimeout)+" "+str(brokers)+" "+str(replication)+" "+str(messageFilePath)+" "+str(ssl)+" &", shell=True)
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
	ssl = args.ssl

	#h2.cmd("python3 kafka-python-consumer.py > consumed-data.txt", shell=True)
	#print("Data consumed")

	for node in net.hosts:					
		if args.java:
			nodeID = str(node.name)[1:]	
			command = "java -jar java/target/amnis-java-1.0-SNAPSHOT.jar " + nodeID + " "+str(nTopics)+" "+str(fetchMinBytes)+" "+str(fetchMaxWait)+" "+str(sessionTimeout)+" &"
			node.popen(command, shell=True)			
		else:
			node.popen("python3 " + script +str(node.name)+" "+str(nTopics)+" "+str(cRate)+" "+str(fetchMinBytes)+" "+str(fetchMaxWait)+" "+str(sessionTimeout)+" "+str(brokers)+" "+mSizeString+" "+str(mRate)+" "+str(replication)+" "+str(topicCheckInterval)+" "+str(ssl)+" &", shell=True)



def printLinksBetween(net , n1, n2):	
	linksBetween = net.linksBetween(n1, n2)
	print(f"Links between {n1.name} {n2.name} {linksBetween}")	
	if len(linksBetween) > 0:
		for link in linksBetween:			
			print(link.intf1)
			print(link.intf2)
	
def logTopicLeaders(net, logDir, args):
	issuingNode = net.hosts[0]
	print(f"Finding topic leaders at {issuingNode.name}\r")
	for i in range(args.nTopics):
		if args.ssl:
			out = issuingNode.cmd("kafka-3.2.0/bin/kafka-topics.sh --bootstrap-server localhost:9093 --command-config kafka-3.2.0/config/consumer.properties --describe --topic topic-"+str(i), shell=True)				
		else:
			out = issuingNode.cmd("kafka-3.2.0/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic topic-"+str(i), shell=True)	
		print(out)
		if 'Error' in out:
			continue
		split1 = out.split('Leader: ')
		split2 = split1[1].split('\t')
		topicLeaderNode = 'h' + split2[0]			
		print(f"Leader for topic-{str(i)} is node {topicLeaderNode}\r")
		logging.info("topic-"+ str(i) +" leader is node " + topicLeaderNode)


def getTopicLeader(issuingNode, i, args):
	n = None
	try:
		if args.ssl:
			out = issuingNode.cmd("kafka-3.2.0/bin/kafka-topics.sh --bootstrap-server localhost:9093 --command-config kafka-3.2.0/config/consumer.properties --describe --topic topic-"+i, shell=True)				
		else:			
			out = issuingNode.cmd("kafka-3.2.0/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic topic-"+i, shell=True)	
		if 'ERROR' in out or 'Error' in out:
			print(out)
		else:
			split1 = out.split('Leader: ')
			split2 = split1[1].split('\t')
			n = 'h' + split2[0]			
			print(f"Leader for topic-{i} is node {n}\r")
	except Exception as e:
		print(e)
	finally:
		return n


def readCurrentKraftLeader(logDir):
	kraftLeader = None
	with open(logDir+"/kraft/" + 'server-h1.log') as f:
		for line in f:			
			if "Completed transition to Leader" in line:
				first = line.split("localId=")[1]				
				kraftLeader = "h" + first.split(",")[0]	
				break
			elif "Completed transition to FollowerState" in line:
				first = line.split("leaderId=")[1]
				kraftLeader = "h" + first.split(",")[0]				
				break
	print(f'Kraft leader is {kraftLeader}\r')
	return kraftLeader


def processDisconnect(net, logDir, args):	
	hostsToDisconnect = []
	netHosts = {k:v for k,v in net.topo.ports.items() if 'h' in k}

	if args.disconnectRandom != 0:
		seed()
		randomIndex = randint(0, len(netHosts) -1)
		netHostsList = list(netHosts.items())
		while args.disconnectRandom != len(hostsToDisconnect):			
			h = net.getNodeByName(netHostsList[randomIndex][0])
			if not hostsToDisconnect.__contains__(h):		
				hostsToDisconnect.append(h)
				print(f"Host {h.name} to be disconnected for {args.disconnectDuration}s\r")
			randomIndex = randint(0, len(netHosts) -1)					
	elif args.disconnectHosts is not None:
		hostNames = args.disconnectHosts.split(',')			
		for hostName in hostNames:
			h = net.getNodeByName(hostName)
			hostsToDisconnect.append(h)
	elif args.disconnectTopicLeaders != 0:				
		issuingNode = net.hosts[0]
		print("Finding topic leaders at localhost:2181\r")
		kraftLeaderNode = readCurrentKraftLeader(logDir)
		# Find topic leaders
		for i in range(args.nTopics):
			# out = issuingNode.cmd("kafka-3.2.0/bin/kafka-topics.sh --zookeeper localhost:2181 --describe --topic topic-"+str(i), shell=True)			
			# split1 = out.split('Leader: ')
			# split2 = split1[1].split('\t')
			# topicLeaderNode = 'h' + split2[0]	
			topicLeaderNode = getTopicLeader(issuingNode, str(i), args)					
			print(f"Leader for topic-{str(i)} is node {topicLeaderNode}\r")
			if topicLeaderNode == kraftLeaderNode:
				# Don't disconnect leader
				print(f"Not adding {topicLeaderNode} to disconnect list as it is Kraft leader \t")	
				continue
			else:
				h = net.getNodeByName(topicLeaderNode)
				if not hostsToDisconnect.__contains__(h):
					hostsToDisconnect.append(h)
			if args.disconnectTopicLeaders == len(hostsToDisconnect):
				break		

	if args.disconnectKraftLeader:
		kraftLeaderNode = readCurrentKraftLeader(logDir)
		h = net.getNodeByName(kraftLeaderNode)
		if not hostsToDisconnect.__contains__(h):
			hostsToDisconnect.append(h)

	return netHosts, hostsToDisconnect


def traceWireshark(hostsToCapture, f):
	for h in hostsToCapture:		
		#temp = h.nameToIntf		
		hostName = h.name
		filename = "/tmp/"+ hostName + "-eth1" + "-" + f + ".pcap"
		output = h.cmd("sudo tcpdump -i " + hostName +"-eth1 -w "+ filename +" &", shell=True)	
		print(output)


def setAuth(net):
	# TODO - Temp hardcoded rule: Only one producer allowed to write for topic-1

	nodeToAllowClassified = net.hosts[0].name

	# Loop through and apply rule
	for h in net.hosts:				
		if h.name == nodeToAllowClassified:
			print(f"Not setting authentication rule for {nodeToAllowClassified}")
			continue
		rule = "--add --deny-principal User:'*' --deny-host '" + h.IP() + "' --operation Write --topic topic-0"		
		print(f"Creating authentication rule {rule} for {h.name}")		
		outAuth = h.cmd("kafka-3.2.0/bin/kafka-acls.sh --bootstrap-server localhost:9093 --command-config kafka-3.2.0/config/consumer.properties " + rule)	
		print(outAuth)
	


def runLoad(net, nTopics, replication, mSizeString, mRate, tClassString, consumerRate, duration, logDir, args, topicWaitTime=100):

	print("Start workload\r")	
	if args.captureAll:
		traceWireshark(net.hosts, "start")

	seed(1)

	nHosts = len(net.hosts)
	print("Number of hosts: " + str(nHosts)+ "\r")

	if args.ssl:
		print("Updating SSL consumer.properties\r")		
		propertyFile = open("kafka-3.2.0/config/consumer.properties", "r")
		consumerProperties = propertyFile.read()
		bProperties = consumerProperties
		bProperties = bProperties.replace(
				"ssl.truststore.location=truststore",
				"ssl.truststore.location="+os.getcwd()+"/certs/server.truststore.jks")

		bProperties = bProperties.replace(
				"ssl.keystore.location=keystore",
				"ssl.keystore.location="+os.getcwd()+"/certs/server.keystore.jks")

		bFile = open("kafka-3.2.0/config/consumer.properties", "w")
		bFile.write(bProperties)
		bFile.close()
		propertyFile.close()		


	# Set authentication
	if args.auth:
		setAuth(net)


	#Create topics
	topicNodes = []
	startTime = time.time()
	time.sleep(10)
	for i in range(nTopics):
		issuingID = randint(0, nHosts-1)
		issuingNode = net.hosts[issuingID]
		
		print("Creating topic "+str(i)+" at broker "+str(issuingNode.name)+"\r")
		if args.ssl:		
			# Use consumer.properties		
			out = issuingNode.cmd("kafka-3.2.0/bin/kafka-topics.sh --create --bootstrap-server 10.0.0."+str(issuingID+1)+":9093 --command-config kafka-3.2.0/config/consumer.properties --replication-factor "+str(replication)+" --partitions 1 --topic topic-"+str(i), shell=True)
		else:
			out = issuingNode.cmd("kafka-3.2.0/bin/kafka-topics.sh --create --bootstrap-server 10.0.0."+str(issuingID+1)+":9092 --replication-factor "+str(replication)+" --partitions 1 --topic topic-"+str(i), shell=True)
		print(out)
		topicNodes.append(issuingNode)
	
	stopTime = time.time()
	totalTime = stopTime - startTime
	print("Successfully Created " + str(nTopics) + " Topics in " + str(totalTime) + " seconds\r")
	


	spawnConsumers(net, nTopics, consumerRate, args)
	print(f"Consumers created at {str(datetime.now())}\r")
	time.sleep(5)	

	# Set the network delay back to to .graphml values before spawning producers so we get accurate latency
	if args.latencyAfterSetup:
		setNetworkDelay(net)
		time.sleep(1)

	print(f"Sleeping for {args.consumerSetupSleep} seconds to allow consumers to connect\r")
	time.sleep(args.consumerSetupSleep)

	spawnProducers(net, mSizeString, mRate, tClassString, nTopics, args)
	print(f"Producers created at {str(datetime.now())}\r")
	time.sleep(1)


	# Log the topic leaders
	logTopicLeaders(net, logDir, args)	
	
	timer = 0
	isDisconnect = args.disconnectRandom != 0 or args.disconnectHosts is not None or args.disconnectKraftLeader or args.disconnectTopicLeaders != 0
	relocate = args.relocate
	
	# Set up disconnect
	if isDisconnect:
		isDisconnected = False
		disconnectTimer = args.disconnectDuration
		netHosts, hostsToDisconnect = processDisconnect(net, logDir, args)
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
		print(f"{h.name} to relocate from {s.name} to {s2.name}\r")

		

	print(f"Starting workload at {str(datetime.now())}\r")
	logging.info('Starting workload at ' + str(datetime.now())+"\r")

	while timer < duration:
		time.sleep(10)
		percentComplete = int((timer/duration)*100)
		print("Processing workload: "+str(percentComplete)+"%\r")
		if isDisconnect and percentComplete >= 10:
			if not isDisconnected:			
				disconnectHosts(net, netHosts, hostsToDisconnect)				
				isDisconnected = True
			elif isDisconnected and disconnectTimer <= 0: 			
				reconnectHosts(net, netHosts, hostsToDisconnect)
				if args.captureAll:
					traceWireshark(hostsToDisconnect, "reconnect")
				isDisconnected = False
				isDisconnect = False
			if isDisconnected:
				disconnectTimer -= 10
		elif relocate:
			delLink(net, h, s)
			addLink(net, h, s2)
			relocate = False
		timer += 10

	logTopicLeaders(net, logDir, args)
	print(f"Workload finished at {str(datetime.now())}\r")	
	logging.info('Workload finished at ' + str(datetime.now())+"\r")



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

def disconnectHosts(net, netHosts, hosts):	
	for h in hosts:
		netHost = netHosts[h.name]		
		s = net.getNodeByName(netHost[1][0])		
		disconnectHost(net, h, s)
	#net.pingAll()

def disconnectHost(net, h, s):		
	print(f"***********Setting link down from {h.name} <-> {s.name} at {str(datetime.now())}")						
	logging.info('Disconnected %s <-> %s at %s', h.name, s.name,  str(datetime.now()))
	net.configLinkStatus(s.name, h.name, "down")			

def reconnectHosts(net, netHosts, hosts):
	for h in hosts:
		netHost = netHosts[h.name]		
		s = net.getNodeByName(netHost[1][0])
		reconnectHost(net, h, s)
	#net.pingAll()

def reconnectHost(net, h, s):
	print(f"***********Setting link up from {h.name} <-> {s.name} at {str(datetime.now())}")
	logging.info('Connected %s <-> %s at %s', h.name, s.name,  str(datetime.now()))
	net.configLinkStatus(s.name, h.name, "up")										

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