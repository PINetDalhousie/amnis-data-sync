#!/usr/bin/python3

import sys
import os
import networkx as nx

# Add relevant dependency to connect Kafka with Spark
def addSparkDependency():
    src = "dependency/*"

	# Local maven directory
    dst = "/root/.ivy2"

    os.system("sudo mkdir -p "+dst+"/cache "+dst+"/jars")
    os.system("sudo cp -r "+src+" "+dst)

def readSparkConfig(sparkConfig):
    # topicsToConsume = sparkConfig.split(",")[0]
    # sparkApp = sparkConfig.split(",")[1]
    # produceTo = sparkConfig.split(",")[2]
    
    # return topicsToConsume, sparkApp, produceTo

    sparkApp = sparkConfig.split(",")[0]
    produceTo = sparkConfig.split(",")[1]
    
    return sparkApp, produceTo
    
def getSparkDetails(net, inputTopoFile):

	sparkDetailsList = []
	sparkDetails = {}
    #Spark can produceTo a topic or a CSV file
	# sparkDetailsKeys = {"nodeId", "topicsToConsume", "applicationPath", "produceTo"}
	sparkDetailsKeys = {"nodeId", "applicationPath", "produceTo"}

	mysqlPath = ''


	#Read topo information
	try:
		inputTopo = nx.read_graphml(inputTopoFile)
	except Exception as e:
		print("ERROR: Could not read topo properly.")
		print(str(e))
		sys.exit(1)
	
	#Read nodewise spark information
	for node, data in inputTopo.nodes(data=True):  
		if node[0] == 'h':
			print("node id: "+node[1])

			if 'sparkConfig' in data: 
				# topicsToConsume, sparkApp, produceTo = readSparkConfig(data["sparkConfig"])
				
				# sparkDetails = {"nodeId": node[1], "topicsToConsume": topicsToConsume, \
                #                 "applicationPath": sparkApp, "produceTo": produceTo}

				sparkApp, produceTo = readSparkConfig(data["sparkConfig"])
				sparkDetails = {"nodeId": node[1], "applicationPath": sparkApp, "produceTo": produceTo}
				
				sparkDetailsList.append(sparkDetails)
			
			if 'mysqlConfig' in data:
				mysqlPath = mysqlPath + data["mysqlConfig"]

            
	print("spark details")
	print(*sparkDetailsList)

	print("mysql config path: "+mysqlPath)

	return sparkDetailsList,mysqlPath

def cleanSparkDependency():
# 	os.system("sudo rm -rf logs/kafka/")
	os.system("sudo rm -rf /root/.ivy2/cache")
	os.system("sudo rm -rf /root/.ivy2/jars")