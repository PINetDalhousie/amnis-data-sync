#!/usr/bin/python3

import os
import logging

# import matplotlib.pyplot as plt


def configureLogDir(brokers, mSizeString, mRate, nTopics, replication):  
	os.system("sudo rm -rf logs/kafka/"+"nodes:" +str(brokers)+ "_mSize:"+ mSizeString+ "_mRate:"+ str(mRate)+ "_topics:"+str(nTopics) +"_replication:"+str(replication)+"/bandwidth/"+"; sudo mkdir -p logs/kafka/"+"nodes:" +str(brokers)+ "_mSize:"+ mSizeString+ "_mRate:"+ str(mRate)+ "_topics:"+str(nTopics) +"_replication:"+str(replication)+"/bandwidth/")
    
	os.system("sudo rm -rf logs/kafka/"+"nodes:" +str(brokers)+ "_mSize:"+ mSizeString+ "_mRate:"+ str(mRate)+ "_topics:"+str(nTopics) +"_replication:"+str(replication)+"/prod/"+"; sudo mkdir -p logs/kafka/"+"nodes:" +str(brokers)+ "_mSize:"+ mSizeString+ "_mRate:"+ str(mRate)+ "_topics:"+str(nTopics) +"_replication:"+str(replication)+"/prod/")    

	os.system("sudo rm -rf logs/kafka/"+"nodes:" +str(brokers)+ "_mSize:"+ mSizeString+ "_mRate:"+ str(mRate)+ "_topics:"+str(nTopics) +"_replication:"+str(replication)+"/cons/"+"; sudo mkdir -p logs/kafka/"+"nodes:" +str(brokers)+ "_mSize:"+ mSizeString+ "_mRate:"+ str(mRate)+ "_topics:"+str(nTopics) +"_replication:"+str(replication)+"/cons/")

	os.system("sudo rm -rf logs/output/; sudo mkdir -p logs/output/")

	logging.basicConfig(filename="logs/kafka/"+"nodes:" +str(brokers)+ "_mSize:"+ mSizeString+ "_mRate:"+ str(mRate)+ "_topics:"+str(nTopics) +"_replication:"+str(replication)+"/events.log",
						format='%(levelname)s:%(message)s',
 						level=logging.INFO)


def cleanLogs():
# 	os.system("sudo rm -rf logs/kafka/")
	os.system("sudo rm -rf logs/output/")
	os.system("sudo rm -rf kafka/logs/")    