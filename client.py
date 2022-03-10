#!/usr/bin/python3

import sys
import time

import logging

import socket

nodeName = sys.argv[1]
nodeID = nodeName[1:]

nTopics = int(sys.argv[2])
cRate = float(sys.argv[3])

fetchMinBytes = int(sys.argv[4])
fetchMaxWait = int(sys.argv[5])
sessionTimeout = int(sys.argv[6])
brokers = int(sys.argv[7])    
mSizeString = sys.argv[8]
mRate = float(sys.argv[9])    
replication = int(sys.argv[10])   
topicCheckInterval = float(sys.argv[11])  

logging.basicConfig(filename="logs/kafka/"+"nodes:" +str(brokers)+ "_mSize:"+ mSizeString+ "_mRate:"+ str(mRate)+ "_topics:"+str(nTopics) +"_replication:"+str(replication)+"/cons/client-"+nodeID+".log",
                        format='%(asctime)s %(levelname)s:%(message)s',
                        level=logging.INFO)    
logging.info("node: "+nodeID)

# client = socket.socket()
# client.connect(("10.0.0."+str(nodeID),9999))
# logging.info("Node id: " + str(nodeID))
# logging.info("Data received" + client.recv(1024).decode())

serverAddress = ("10.0.0."+str(nodeID), 7070)
# Create a datagram socket

tempSensorSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
response = tempSensorSocket.recv(1024);
logging.info("Data received" + response.decode())