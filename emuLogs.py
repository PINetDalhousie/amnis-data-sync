#!/usr/bin/python3

import os
import logging

import matplotlib.pyplot as plt


def configureLogDir(brokers, mSizeString, mRate, nTopics, replication):  
	logDir = "logs/kafka/nodes:" +str(brokers)+ "_mSize:"+ mSizeString+ "_mRate:"+ str(mRate)+ "_topics:"+str(nTopics) +"_replication:"+str(replication)	
	os.system("sudo rm -rf " + logDir + "/bandwidth/; " + "sudo mkdir -p " + logDir + "/bandwidth/")
	os.system("sudo rm -rf " + logDir + "/prod/; " + "sudo mkdir -p " + logDir + "/prod/")    
	os.system("sudo rm -rf " + logDir + "/cons/; " + "sudo mkdir -p " + logDir + "/cons/")

	logging.basicConfig(filename=logDir+"/events.log",
						format='%(levelname)s:%(message)s',
 						level=logging.INFO)
	return logDir


def cleanLogs():
# 	os.system("sudo rm -rf logs/kafka/")
	os.system("sudo rm -rf kafka/logs/")    