#!/usr/bin/python3

import os
import logging

import argparse
from datetime import datetime, timedelta
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import seaborn as sns
import pandas as pd

consLogs = []

prodCount = 0
consCount = 0
extraLatencyMessage = 0
latencyYAxis = []


def clearExistingPlot():
    # clear the previous figure
    plt.close()
    plt.cla()
    plt.clf()   


def getProdDetails(prodId):
    global prodCount
    global consLogs

    latencyLog = open(logDir+"/latency-log.txt", "a")
    
    with open(logDir+'/prod/prod-'+str(prodId)+'.log') as f:
        for line in f:
            if "Topic: topic-" in line:
#                 msgProdTime = line.split(",")[0]
                msgProdTime = line.split(" INFO:Topic:")[0]
                topicSplit = line.split("topic-")
                topicId = topicSplit[1].split(";")[0]
                msgIdSplit = line.split("Message ID: ")
                msgId = msgIdSplit[1].split(";")[0]
                
                #print("producer: "+str(prodId)+" time: "+msgProdTime+" topic: "+topicId+" message ID: "+msgId)
                prodCount+=1

                if prodId < 10:
                    formattedProdId = "0"+str(prodId)
                else:
                    formattedProdId = str(prodId)

                for consId in range(switches):
                    #print(formattedProdId+"-"+msgId+"-topic-"+topicId)
                    if formattedProdId+"-"+msgId+"-topic-"+topicId in consLogs[consId].keys():
                        msgConsTime = consLogs[consId][formattedProdId+"-"+msgId+"-topic-"+topicId]

                        prodTime = datetime.strptime(msgProdTime, "%Y-%m-%d %H:%M:%S,%f")
                        consTime = datetime.strptime(msgConsTime, "%Y-%m-%d %H:%M:%S,%f")
                        latencyMessage = consTime - prodTime

                        #print(latencyMessage)
                        latencyLog.write("Producer ID: "+str(prodId)+" Message ID: "+msgId+" Topic ID: "+topicId+" Consumer ID: "+str(consId+1)+" Production time: "+msgProdTime+" Consumtion time: "+str(msgConsTime)+" Latency of this message: "+str(latencyMessage))
                        latencyLog.write("\n")    #latencyLog.write("\r\n")

        print("Prod " + str(prodId) + ": " + str(datetime.now()))

    latencyLog.close()
                

def initConsStruct(switches):
    global consLogs

    for consId in range(switches):
        newDict = {}
        consLogs.append(newDict)


def readConsumerData(producerID, topicName, messageID):

    #print("Start reading cons data: " + str(datetime.now()))
    offset = ""
    for consId in range(1, switches+1):
        #print(logDir+'cons/cons-'+str(consId)+'.log')
        f = open(logDir+'cons/cons-'+str(consId)+'.log')

        for lineNum, line in enumerate(f,1):         #to get the line number
            #print(line)

            if "Prod ID: " in line:
                lineParts = line.split(" ")
                #print(lineParts)

                prodID = lineParts[4][0:-1]
                #print(prodID)

                msgID = lineParts[7][0:-1]
                #print(msgID)

                topic = lineParts[11][0:-1]
                #print(topic)

                #print(prodID+"-"+msgID+"-"+topic)
                consLogs[consId-1][prodID+"-"+msgID+"-"+topic] = lineParts[0] + " " + lineParts[1]

                # Get the offset
                if prodID == producerID and msgID == messageID and topic == topicName:
                    offset = lineParts[13][0:-1]

        f.close()
    return offset        


def plotLatencyMessage(switches, prodID, topicName, messageID, offset):
    '''
    Function that receives as input all consumer and producer logs, plus a given topic and message ID.
    Returns a plot showing the latency of each node to consumer that particular message (plus the offset)
	Can be either scattor or bar plot. Y axis is latency, x axis is consumer (which can be ordered based on consumption time)
	Offset should be the same for all consumers as the message is the smae, so we don't need to plot that (can add it as a label or as part of the plot title)
    '''
    consumersId = {}
    consumersLatency = {}
    timesReceived = {}
    timesSent = {}
    for consId in range(switches):
        break
        consumersLatency.append({})
        timesReceived.append({})
        timesSent.append({})
    
    topicID = topicName.split("topic-")[1]
    with open(logDir+"/latency-log.txt", "r") as f:
        for lineNum, line in enumerate(f,1):
            stringCheck = "Producer ID: "+ prodID +" Message ID: " + messageID + " Topic ID: " + topicID
            if stringCheck in line:                
                lineSplit = line.split(" ")                
                consID = lineSplit[11]
                timeSent = lineSplit[15]
                timeReceived = lineSplit[19]
                
                firstSplit = line.split("Latency of this message: 0:")
                latency = float(firstSplit[1][0:2])*60.0 + float(firstSplit[1][3:5])
                
                timesSent[lineNum] = timeSent.replace(",", ".")                
                timesReceived[lineNum] = timeReceived.replace(",", ".")                
                consumersLatency[lineNum] = latency
                consumersId[lineNum] = consID

    # Sort based on time sent
    y = []
    x = []        
    for k in sorted(timesReceived.items(), key=lambda x: x[1]) :                    
        lat = consumersLatency[k[0]]
        cID = consumersId[k[0]]
        y.append(lat)
        x.append(cID)            


    # Add colors and plot each scatter
    colors = cm.rainbow(np.linspace(0, 1, len(y)))
    k = 0
    for dataset,color in zip(y,colors):        
        plt.scatter(x[k],dataset,color=color, label=str(k+1))
        k = k+1

    # Create rest of plot
    plt.xlabel('Consumer')
    plt.ylabel('Latency(s)')
    plt.title("Latency for Prod " + prodID + " " + topicName + " Msg " + messageID + " Offset " + offset)
    # plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.15),
    #       fancybox=True, shadow=True, ncol=5, markerscale=4, title="Consumer")
    plt.savefig(logDir+"prod-"+prodID+"-"+topicName+"-msg-"+messageID+"-latency-plot",bbox_inches="tight")
      
parser = argparse.ArgumentParser(description='Script for measuring latency for each message.')
parser.add_argument('--number-of-switches', dest='switches', type=int, default=0, help='Number of switches')
parser.add_argument('--log-dir', dest='logDir', type=str, help='Producer log directory')
parser.add_argument('--prod', dest='prod', type=int, default=0, help='Producer ID')
parser.add_argument('--topic', dest='topic', type=str, default="topic-0", help='Topic name')
parser.add_argument('--msg', dest='msg', type=int, default=0, help='Message ID')


args = parser.parse_args()

switches = args.switches
logDir = args.logDir
prod = str(args.prod)
topic = args.topic
msg = str(args.msg)

# For testing
# switches = 10
# logDir = "logs/kafka/test/"
# prod = "1"
# topic = "topic-0"
# msg = "6487"

msg = msg.zfill(6)


initConsStruct(switches)

offset = readConsumerData(prod.zfill(2) , topic, msg)
if not os.path.exists(logDir+"latency-log.txt"):
    os.system("sudo touch "+logDir+"latency-log.txt")      
    for prodId in range(switches):
        getProdDetails(prodId+1)

plotLatencyMessage(switches, prod, topic, msg, offset)

clearExistingPlot()