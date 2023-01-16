#!/usr/bin/env python3

import sys
import os
import argparse

sys.path.insert(0, '../utils/')

from utils import logparsing
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

parser = argparse.ArgumentParser(description='Script for visualizing message ordering.')
parser.add_argument('--log-dir', dest='logDir', type=str, help='Log directory')
parser.add_argument('--prod', dest='nProd', type=int, default=0, help='Number of producers')
parser.add_argument('--cons', dest='nCons', type=int, default=0, help='Number of consumers')
parser.add_argument('--num-topics', dest='nTopic', type=int, default=0, help='Number of topics')
parser.add_argument('--topic', dest='topicID', type=int, default=0, help='Number of topics')

args = parser.parse_args()

params = {  
			'num_consumers' : args.nCons,
			'cons_dir' : args.logDir+'cons/',
			'num_producers' : args.nProd,
			'prod_dir' : args.logDir+'prod/',
			'num_topics' : args.nTopic,
			'topic_id' : args.topicID
		}
		

prodLog = logparsing.ProducerLog()
consLog = logparsing.ConsumerLog()


#Read producer data
print("Reading producer data")
prodData = prodLog.getAllProdData(params['prod_dir'], params['num_producers'])

#Read consumer data
print("Reading consumer data")
consData = consLog.getAllConsData(params['cons_dir'], params['num_consumers'])


commitID = [] 
msgID = [] 
prodID = []
prodInterval = []


def convertTimeToMilliseconds(timeString):

	hourISO = timeString.split(" ")[1]
	#print(hourISO)
	
	splitHour = hourISO.split(":")
	splitSeconds = splitHour[2].split(",")

	prodTimeMilliseconds = int(splitHour[0])*3600*1000 + \
				int(splitHour[1])*60*1000 + \
				int(splitSeconds[0])*1000 + \
				int(splitSeconds[1])*10

	return prodTimeMilliseconds


print("Computing message order")

for consumedMsg in consData[0]:	
	
	if( int(consumedMsg[3]) == params['topic_id'] ):
		#print(prodID)
		if int(consumedMsg[4]) in commitID:
			print("ERROR: Duplicated commit ID.")
		
		commitID.append(int(consumedMsg[4]))
		msgID.append(int(consumedMsg[2]))
		prodID.append(int(consumedMsg[1]) - 1)
		
		prodTime = prodData[int(consumedMsg[1])-1][int(consumedMsg[2])][0]
		prodTimeMilliseconds = convertTimeToMilliseconds(prodTime)
		
		#TODO: parse disconnection/reconnection times from log
		disconnTime = "2022-11-04 17:04:13,67"
		disconnTimeMilliseconds = convertTimeToMilliseconds(disconnTime)
		
		reconnTime = "2022-11-04 17:05:13,74"
		reconnTimeMilliseconds = convertTimeToMilliseconds(reconnTime)

		if prodTimeMilliseconds < disconnTimeMilliseconds: 
			prodInterval.append(0)
		elif prodTimeMilliseconds < reconnTimeMilliseconds:
			prodInterval.append(128)
		else:
			prodInterval.append(255) 
	

os.makedirs("msg-order", exist_ok=True)

#Plot commit offset order for different producers
plt.xlabel('Commit offset')
plt.ylabel('Message ID')

plt.scatter(commitID, msgID, c=prodID)

plt.savefig("msg-order/consumer-1.png",format='png', bbox_inches="tight")

plt.cla()
plt.clf()  

#Plot commit offset order for different time intervals 
#(i.e., before disconnection, during disconnection, and after reconnection)
plt.xlabel('Commit offset')
plt.ylabel('Message ID')

plt.scatter(commitID, msgID, c=prodInterval)
	
plt.savefig("msg-order/consumer-1-intervals.png",format='png', bbox_inches="tight")



























