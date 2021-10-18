#!/usr/bin/python3

from mininet.net import Mininet
from mininet.cli import CLI

from random import seed, randint

import time
import os
import sys

leaderReplicaList = []

def spawnProducers(net, mSizeString, mRate, tClassString, nTopics, nZk):

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
			node.popen("python3 producer.py "+str(node)+" "+tClasses[i]+" "+mSizeString+" "+str(mRate)+" "+str(nTopics)+" "+str(nZk)+" &", shell=True)

		i += 1


def spawnConsumers(net, nTopics, rate, nZk):
	#h2.cmd("python3 kafka-python-consumer.py > consumed-data.txt", shell=True)
	#print("Data consumed")

	for node in net.hosts:
		node.popen("python3 consumer.py "+str(node.name)+" "+str(nTopics)+" "+str(rate)+" "+str(nZk)+" &", shell=True)


def runLoad(net, nTopics, replication, mSizeString, mRate, tClassString, consumerRate, duration, nZk, topicWaitTime=300):

	print("Start workload")

	seed(1)

	nHosts = len(net.hosts)
	print("Number of hosts: " + str(nHosts))
    
	#Create topics
	topicNodes = []
	for i in range(nTopics):
		issuingID = randint(0, nHosts-1)
		issuingNode = net.hosts[issuingID]
		kraftPort = 2181        

# 		if nZk == 0:        
# 			issuingNode.popen("kafka/bin/kafka-topics.sh --create --bootstrap-server 10.0.0."+str(issuingID+1)+":"+str(kraftPort+issuingID)+ " --replication-factor "+str(replication)+" --partitions 1 --topic topic-"+str(i)+" &", shell=True)
# 			print("Creating kraft topic "+str(i)+" at broker "+str(issuingID+1))            
# 		else:        
# 			issuingNode.popen("kafka/bin/kafka-topics.sh --create --bootstrap-server 10.0.0."+str(issuingID+1)+":9092 --replication-factor "+str(replication)+" --partitions 1 --topic topic-"+str(i)+" &", shell=True)            
# 			print("Creating kafka topic-"+str(i)+" at broker "+str(issuingID+1))
		issuingNode.popen("kafka/bin/kafka-topics.sh --create --bootstrap-server 10.0.0."+str(issuingID+1)+":9092 --replication-factor "+str(replication)+" --partitions 1 --topic topic-"+str(i)+" &", shell=True)
		print("Creating topic "+str(i)+" at broker "+str(issuingID+1))
		topicNodes.append(issuingNode)

	topicWait = True
	startTime = time.time()
	totalTime = 0
	topicNumber = 0    
    
    # printing the list using loop
	for x in range(len(topicNodes)):
	    print (topicNodes[x])
    
# 	for host in topicNodes:
# 	    while topicWait:
# 	        if topicNumber < nTopics:
# 	            print("Checking Topic Creation for Host " + str(host.IP()) + "...")
# 	            out = host.cmd("kafka/bin/kafka-topics.sh --list --bootstrap-server " + str(host.IP()) + ":9092", shell=True)
                
# 	            topicDescription = host.cmd("kafka/bin/kafka-topics.sh --bootstrap-server " + str(host.IP()) + ":9092 --describe --topic topic-"+str(topicNumber), shell=True)
# 	            print(topicDescription) 
# 	            if topicDescription.find('Leader:') != -1:
#                     # parse and get the leader id from topic description                
# 	                leaderSplit = topicDescription.split('Leader: ')   
#     # 	            print("leaderSplit: " + str(leaderSplit))
#                     # using list comprehension
#     # 	            listToStr = ' '.join(map(str, leaderSplit))
#     # 	            if listToStr.find('Replicas:') != -1:
# 	                leaderNumberSplit = leaderSplit[1].split('Replicas:')           
# 	                leaderNumber = int(leaderNumberSplit[0])
# 	                leaderReplicaList.append(leaderNumber)
# 	                print("leaderNumber: " + str(leaderNumber))
# 	                topicNumber = topicNumber+1
            
    
# 	            stopTime = time.time()
# 	            totalTime = stopTime - startTime
# 	            if "topic-" in out:
# 	                topicWait = False
# 	                print(out)
# 	            elif(totalTime > topicWaitTime):
# 	                print("ERROR: Timed out waiting for topics to be created")
# 	                sys.exit(1)
# 	            else:
# 	                time.sleep(10)
# 	        else:
# 	            break
# 	    topicWait = True
# # 	    topicNumber = 0
	    
# 	print("Successfully Created Topics in " + str(totalTime) + " seconds")

	for host in topicNodes:
	    while topicWait:
	        print("Checking Topic Creation for Host " + str(host.IP()) + "...")
	        out = host.cmd("kafka/bin/kafka-topics.sh --list --bootstrap-server " + str(host.IP()) + ":9092", shell=True)
	        stopTime = time.time()
	        totalTime = stopTime - startTime
	        if "topic-" in out:
	            topicWait = False
	            print(out)
	        elif(totalTime > topicWaitTime):
	            print("ERROR: Timed out waiting for topics to be created")
	            sys.exit(1)
	        else:
	            time.sleep(10)
	    topicWait = True
	    
	print("Successfully Created Topics in " + str(totalTime) + " seconds")
	
    # saving leader replica list in a file for convenience
# 	if nZk == 0:
# 	    folderPath = "logs/kraft/bandwidth/"
# 	else:
# 	    folderPath = "logs/kafka/bandwidth/"
# 	leaderFile = open(folderPath+"nodes:" +str(len(net.hosts))+ "_mSize:"+ mSizeString+ "_mRate:"+ str(mRate)+ "_topics:"+str(nTopics) +"_replication:"+str(replication)+"/leader-list.txt", "w")    
# 	for element in leaderReplicaList:
# 	    leaderFile.write(str(element) + "\n")
# 	leaderFile.close()
# 	print("Leader replica list updated in file")

	spawnProducers(net, mSizeString, mRate, tClassString, nTopics, nZk)
	time.sleep(1)
	print("Producers created")

	spawnConsumers(net, nTopics, consumerRate, nZk)
	time.sleep(1)
	print("Consumers created")

	timer = 0

	while timer < duration:
		time.sleep(10)
		print("Processing workload: "+str(int((timer/duration)*100))+"%")
		timer += 10

	print("Workload finished")











