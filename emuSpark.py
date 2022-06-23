#!/usr/bin/python3

import sys
import networkx as nx

def readSparkConfig(sparkConfig):
    topicsToConsume = sparkConfig.split(",")[0]
    sparkApp = sparkConfig.split(",")[1]
    produceTo = sparkConfig.split(",")[2]
    
    return topicsToConsume, sparkApp, produceTo
    
def placeKafkaBrokers(net, inputTopoFile):

	sparkDetailsList = []
	sparkDetails = {}
    #Spark can produceTo a topic or a CSV file
	sparkDetailsKeys = {"nodeId", "topicsToConsume", "applicationPath", "produceTo"}


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
				topicsToConsume, sparkApp, produceTo = readSparkConfig(data["sparkConfig"])
				
				sparkDetails = {"nodeId": node[1], "topicsToConsume": topicsToConsume, \
                                "applicationPath": sparkApp, "produceTo": produceTo}
				sparkDetailsList.append(sparkDetails)
            
	print("spark details")
	print(*sparkDetailsList)

	return sparkDetailsList