#!/usr/bin/python3

import os
import logging

import matplotlib.pyplot as plt
from mininet.util import pmonitor

ZOOKEEPER_LOG_FILE = "zk-log.txt"
BROKER_LOG_FILE = "broker-log.txt"


def configureLogDir(brokers, mSizeString, mRate, nTopics, replication):  
	logDir = "logs/kafka/nodes:" +str(brokers)+ "_mSize:"+ mSizeString+ "_mRate:"+ str(mRate)+ "_topics:"+str(nTopics) +"_replication:"+str(replication)	
	os.system("sudo rm -rf logs/kafka/" + ZOOKEEPER_LOG_FILE)

	os.system("sudo rm -rf logs/kafka/" + BROKER_LOG_FILE)

	os.system("sudo rm -rf " + logDir + "/bandwidth/; " + "sudo mkdir -p " + logDir + "/bandwidth/")
	os.system("sudo rm -rf " + logDir + "/prod/; " + "sudo mkdir -p " + logDir + "/prod/")    
	os.system("sudo rm -rf " + logDir + "/cons/; " + "sudo mkdir -p " + logDir + "/cons/")

	logging.basicConfig(filename=logDir+"/events.log",
						format='%(levelname)s:%(message)s',
 						level=logging.INFO)
	return logDir


def cleanLogs():
	#os.system("sudo rm -rf logs/kafka/")
	os.system("sudo rm -rf kafka/logs/")   	

def logMininetProcesses(popens, logFileName):
    logFilePath = "logs/kafka/" + logFileName
    bandwidthLog = open(logFilePath, "a")
    for host, line in pmonitor(popens):
        if host:
            bandwidthLog.write("<%s>: %s" % (host.name, line))

def logEvents(logDir, switches):
	# Log when consumers got first messages
	for consId in range(1, switches+1):
		f = open(logDir+'/cons/cons-'+str(consId)+'.log')
		for lineNum, line in enumerate(f,1):         #to get the line number
			if "Prod ID: " in line:
				lineParts = line.split(" ")				
				logDateTime = lineParts[0] + " " + lineParts[1]
				logging.info("Consumer %s first received message: %s", consId, logDateTime)
				break
		f.close()
	
	# Log when producers sent first messages
	for prodId in range(1, switches+1):
		f = open(logDir+'/prod/prod-'+str(prodId)+'.log')
		for lineNum, line in enumerate(f,1):         #to get the line number
			if "Topic: topic-" in line:
				lineParts = line.split(" ")				
				logDateTime = lineParts[0] + " " + lineParts[1]
				logging.info("Producer %s first sent message: %s", prodId, logDateTime)
				break
		f.close()