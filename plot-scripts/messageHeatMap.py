#!/usr/bin/env python3

import sys
import os
import argparse

sys.path.insert(0, '../utils/')

from utils import logparsing
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

parser = argparse.ArgumentParser(description='Script for visualizing message delivery.')
parser.add_argument('--log-dir', dest='logDir', type=str, help='Log directory')
parser.add_argument('--prod', dest='nProd', type=int, default=0, help='Number of producers')
parser.add_argument('--cons', dest='nCons', type=int, default=0, help='Number of consumers')
parser.add_argument('--topic', dest='nTopic', type=int, default=0, help='Number of topics')

args = parser.parse_args()

params = {  
			'num_consumers' : args.nCons,
			'cons_dir' : args.logDir+'cons/',
			'num_producers' : args.nProd,
			'prod_dir' : args.logDir+'prod/',
			'num_topics' : args.nTopic
		}
		
prodLog = logparsing.ProducerLog()
consLog = logparsing.ConsumerLog()
		
#Plot broker confirmations
brokerConfirmations = prodLog.getAllBrokerConfirmations(params['prod_dir'], params['num_producers'])

for producer in range(params['num_producers']):

	confirmationHeatData = []

	for confirmation in brokerConfirmations[producer]:
		confirmationHeatData.append(int(confirmation[1]))

	dfConf = pd.DataFrame(confirmationHeatData, columns=["Prod"])

	sns.heatmap(dfConf.T)

	plt.xlabel('Message ID')
	plt.title("Producer " + str(producer+1) + "- Broker confirmations")

	os.makedirs("broker-confirmation", exist_ok=True)

	plt.savefig("broker-confirmation/producer-"+str(producer+1)+".png",format='png', bbox_inches="tight")

	#plt.close()
	plt.cla()
	plt.clf()  
	

#Initialize topic colors
topicColors = {}
colorInc = round(255 / params['num_topics'])

for i in range(params['num_topics']):
	topicColors[i] = colorInc*i

#Read producer data
prodData = prodLog.getAllProdData(params['prod_dir'], params['num_producers'])

#Read consumer data
consData = consLog.getAllConsData(params['cons_dir'], params['num_consumers'])

#Create heat maps for all producers
iterCount = 1

for producer in range(params['num_producers']):

	#Create heat map
	rawHeatData = []
	
	#Retrieve message confirmation info
	confirmations = brokerConfirmations[producer]

	#Initialize heat matrix: [consumer, prod msg]
	for i in range(params['num_consumers']):
		recvMsg = []
		prodMsg = [0]*len(prodData[producer])
		recvMsg.append(prodMsg)

		rawHeatData.append(prodMsg)

	#Fill heat matrix with message delivery information
	for consumer in range(params['num_consumers']):
	
		#Read consumer data
		consID = consumer

		rawRecvMsgs = consLog.getAllMsgFromProd( consData[consumer], str(producer+1).zfill(2) )
		#print(len(rawRecvMsgs))

		#Fill heat matrix with received messages
		for msg in rawRecvMsgs:
			msgID = msg[2]
			rawHeatData[consID][int(msgID)] = 255

		#Color unreceived messages according to their topic
		consHeatMap = rawHeatData[consumer]

		missingMsgs = []
		i = 0

		for msg in consHeatMap:
			if msg == 0:
				missingMsgs.append(i)

			i += 1

		missingTopic = []

		for miss in missingMsgs:
			missMsg = prodLog.getMsgData(prodData[producer], str(miss))
			missTopic = missMsg[1]

			rawHeatData[consID][miss] = topicColors[int(missTopic)]
			
		#Mark unconfirmed messages as sent
		for conf in confirmations:
			if conf[1] == "0":
				if rawHeatData[consID][int(conf[0])] == 255:
					print("ERROR: inconsistency found. Check message ", conf[0], ", producer ", str(producer), ", consumer ", str(consID))
				else:
					#Message not confirmed. Mark as sent.
					rawHeatData[consID][int(conf[0])] = 255

	os.makedirs("failed-messages", exist_ok=True)
	
	f = open("failed-messages/fail-log-prod-"+str(producer+1)+".txt", "w")

	f.write("Producer "+str(producer)+"\n")
	
	for consumer in range(params['num_consumers']): 

		f.write("Consumer "+str(consumer)+"\n")

		for msgIdx in range(len(rawHeatData[consumer])):
			if rawHeatData[consumer][msgIdx] != 255 and msgIdx < 9000:
				f.write("Index: "+str(msgIdx)+"\n")

	f.close()

	#Plot heatmap
	df = pd.DataFrame(rawHeatData, columns=[i for i in range(len(prodData[producer]))])

	sns.heatmap(df)

	plt.xlabel('Message ID')
	plt.ylabel('Consumer')
	plt.title("Producer " + str(producer+1) + "- Message delivery")

	os.makedirs("msg-delivery", exist_ok=True)

	plt.savefig("msg-delivery/producer-"+str(producer+1)+".png",format='png', bbox_inches="tight")

	#plt.close()
	plt.cla()
	plt.clf()  
	
	#Show processing status
	print('Processing: '+str((iterCount/params['num_producers'])*100.0)+'% complete')
	iterCount += 1

#	plt.show()



























