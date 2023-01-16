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
    
def latencyBoxPlot():
	# Put the CSV file in the same directory containing four columns with the experiment values 
	df = pd.read_csv(r'hierarchical_boxplot_data.csv',na_values="NaN") 
	df = df.dropna(how='any')
	# print(df.info())

	df['100ms']=df['100ms'].transform(lambda x: x.split(':')[-1])
	df['500ms']=df['500ms'].transform(lambda x: x.split(':')[-1])
	df['1000ms']=df['1000ms'].transform(lambda x: x.split(':')[-1])
	df['5000ms']=df['5000ms'].transform(lambda x: x.split(':')[-1])

	print(df)

	features = df.columns
	temp_df = pd.DataFrame(df, dtype='float')
	boxplot = temp_df.boxplot(figsize = (6,6), rot = 90, fontsize= '10', grid = False)

	# temp_df = pd.DataFrame(df, columns=[features[0]], dtype='float')
	# boxplot = temp_df.boxplot()

	plt.xlabel('Replica Max Wait')
	plt.ylabel('Latency(s)')
	plt.title("Latency Boxplot for different simulation")
	plt.savefig('Hierarchical latency boxplot.png')
    
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

                        # Write to the consumer latency log
                        consLatencyLog = open(logDir+"/cons-latency-logs/latency-log-cons-"+ str(consId+1) + ".txt", "a")
                        consLatencyLog.write("Producer ID: "+str(prodId)+" Message ID: "+msgId+" Topic ID: "+topicId+" Consumer ID: "+str(consId)+" Production time: "+msgProdTime+" Consumtion time: "+str(msgConsTime)+" Latency of this message: "+str(latencyMessage))
                        consLatencyLog.write("\n")    #latencyLog.write("\r\n")
                        consLatencyLog.close()

                        #getConsDetails(consId+1, prodId, msgProdTime, topicId, msgId)

        print("Prod " + str(prodId) + ": " + str(datetime.now()))

    latencyLog.close()
                

def initConsStruct(switches):
    global consLogs

    for consId in range(switches):
        newDict = {}
        consLogs.append(newDict)
    
def readConsumerData():

    #print("Start reading cons data: " + str(datetime.now()))
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

        f.close()
        

        #print(datetime.now())


def getConsDetails(consId, prodId, msgProdTime, topicId, msgId):
    global consCount
    global extraLatencyMessage
    syncLatency = 1
    
    with open(logDir+'cons/cons-'+str(consId)+'.log') as f:
        for lineNum, line in enumerate(f,1):         #to get the line number
            if "Prod ID: "+str(prodId).zfill(2) in line and "Message ID: "+msgId+";" in line and "topic-"+topicId in line:
#                 print("found in consumer: "+str(consId)+" at line: "+str(lineNum))
#                 msgConsTime = line.split(",")[0]
                msgConsTime = line.split(" INFO:Prod ID:")[0]
                
                prodTime = datetime.strptime(msgProdTime, "%Y-%m-%d %H:%M:%S,%f")
                consTime = datetime.strptime(msgConsTime, "%Y-%m-%d %H:%M:%S,%f")
                latencyMessage = consTime - prodTime
                
                consCount+=1  
        
                if latencyMessage > timedelta(seconds=syncLatency):
                    extraLatencyMessage+=1
                    
                latencyLog(consId, prodId, msgProdTime, msgConsTime, topicId, msgId, latencyMessage)
                
                
def latencyLog(consId, prodId, msgProdTime, msgConsTime, topicId, msgId, latencyMessage):
    latencyLog = open(logDir+"/latency-log.txt", "a")

    latencyLog.write("Producer ID: "+str(prodId)+" Message ID: "+msgId+" Topic ID: "+topicId+" Consumer ID: "+str(consId)+" Production time: "+msgProdTime+" Consumtion time: "+str(msgConsTime)+" Latency of this message: "+str(latencyMessage))
    latencyLog.write("\n")    #latencyLog.write("\r\n")
    latencyLog.close()
        
def plotLatencyScatter():
    lineXAxis = []
#     latencyYAxis = []
    global latencyYAxis
    with open(logDir+"/latency-log.txt", "r") as f:
        for lineNum, line in enumerate(f,1):         #to get the line number
            lineXAxis.append(lineNum)
            if "Latency of this message: " in line:
                firstSplit = line.split("Latency of this message: 0:")
                #print(str(firstSplit[1]))
                #print(str(firstSplit[1][0:2]))
                #print(str(firstSplit[1][3:5]))
                #print(float(firstSplit[1][0:2])*60.0 + float(firstSplit[1][3:5]))
                latencyYAxis.append(float(firstSplit[1][0:2])*60.0 + float(firstSplit[1][3:5]))

    plt.scatter(lineXAxis, latencyYAxis)
    plt.xlabel('Message')
    plt.ylabel('Latency(s)')
    plt.title("Latency Measurement")

    plt.savefig(logDir+"latency Plot",bbox_inches="tight")
                            


def plotConsTopicLactencyScatter(switches):
    consumersLatency = []
    timesSent = []
    prodIDs = []
    topicIDs = []

    os.makedirs(logDir+"cons-latency-plots", exist_ok=True)
    os.makedirs(logDir+"topic-latency-plots", exist_ok=True)

    for consId in range(switches):
        consumersLatency.append({})
        timesSent.append({})
        prodIDs.append({})
        topicIDs.append({})
    
    with open(logDir+"/latency-log.txt", "r") as f:
        for lineNum, line in enumerate(f,1):         #to get the line number            
            if "Latency of this message: " in line:                
                lineSplit = line.split(" ")                
                prodID = lineSplit[2]
                topicID = lineSplit[8]
                consID = lineSplit[11]
                timeSent = lineSplit[15]
                
                firstSplit = line.split("Latency of this message: 0:")
                latency = float(firstSplit[1][0:2])*60.0 + float(firstSplit[1][3:5])
                
                key = int(consID)-1
                timesSent[key][lineNum] = timeSent.replace(",", ".")                
                consumersLatency[key][lineNum] = latency
                prodIDs[key][lineNum] = prodID
                topicIDs[key][lineNum] = topicID  
                    
    # Sort based on time sent
    for i in range(len(timesSent)):
        y = []
        x = []
        prodIdSorted = []
        topicIdSorted = []
        j=0
        for k in sorted(timesSent[i].items(), key=lambda x: x[1]) :
            prodID = int(prodIDs[i][k[0]]) 
            prodIdSorted.append(prodID)
            topicID = int(topicIDs[i][k[0]]) 
            topicIdSorted.append(topicID)
            lat = consumersLatency[i][k[0]]
            y.append(lat)
            x.append(j)
            j = j+1
        
        # Create Producer plot
        # Set colors
        fig, ax = plt.subplots()
        colors = cm.rainbow(np.linspace(0, 1, len(prodIdSorted)))
        scatter = ax.scatter(x, y, c=prodIdSorted)

        # Create rest of plot
        plt.xlabel('Message')
        plt.ylabel('Latency(s)')
        plt.title("Consumer " + str(i+1) + " Latency Measurement")
        plt.legend(*scatter.legend_elements(), loc='upper center', bbox_to_anchor=(0.5, -0.05),
            fancybox=True, shadow=True, ncol=5, markerscale=2, title="Producer")
        plt.savefig(logDir+"cons-latency-plots/latency-plot-prod-cons-"+str(i+1),bbox_inches="tight")
        clearExistingPlot()

        # Create topic plot
        # Set colors
        fig, ax = plt.subplots()
        colors = cm.rainbow(np.linspace(0, 1, len(topicIdSorted)))
        scatter = ax.scatter(x, y, c=topicIdSorted)

        # Create rest of plot
        plt.xlabel('Message')
        plt.ylabel('Latency(s)')
        plt.title("Consumer " + str(i+1) + " Latency Measurement")
        plt.legend(*scatter.legend_elements(), loc='upper center', bbox_to_anchor=(0.5, -0.05),
            fancybox=True, shadow=True, ncol=5, markerscale=2, title="Topic")
        plt.savefig(logDir+"topic-latency-plots/latency-plot-topic-cons-"+str(i+1),bbox_inches="tight")
        clearExistingPlot()


def plotLatencyScatterSorted(switches):
    consumersLatency = []
    timesSent = []
    for consId in range(switches):
        consumersLatency.append({})
        timesSent.append({})
    
    with open(logDir+"/latency-log.txt", "r") as f:
        for lineNum, line in enumerate(f,1):         #to get the line number            
            if "Latency of this message: " in line:                
                lineSplit = line.split(" ")                
                consID = lineSplit[11]
                timeSent = lineSplit[15]
                
                firstSplit = line.split("Latency of this message: 0:")
                latency = float(firstSplit[1][0:2])*60.0 + float(firstSplit[1][3:5])
                
                timesSent[int(consID)-1][lineNum] = timeSent.replace(",", ".")                
                consumersLatency[int(consID)-1][lineNum] = latency
                    
    # Sort based on time sent
    yAxisList= []
    xAxisList = []
    for i in range(len(timesSent)):
        y = []
        x = []
        j=0
        for k in sorted(timesSent[i].items(), key=lambda x: x[1]) :                    
            lat = consumersLatency[i][k[0]]
            y.append(lat)
            x.append(j)
            j = j+1
        xAxisList.append(x)
        yAxisList.append(y)

    # Add colors and plot each scatter
    colors = cm.rainbow(np.linspace(0, 1, len(yAxisList)))
    k = 0
    for dataset,color in zip(yAxisList,colors):        
        plt.scatter(xAxisList[k],dataset,color=color, label=str(k+1), s=4.0)
        k = k+1

    # Create rest of plot
    plt.xlabel('Message')
    plt.ylabel('Latency(s)')
    plt.title("Sorted Latency Measurement")
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05),
          fancybox=True, shadow=True, ncol=5, markerscale=4, title="Consumer")
    plt.savefig(logDir+"latency-plot-sorted",bbox_inches="tight")


def plotLatencyPDF():
    global latencyYAxis

    #sns.distplot(latencyYAxis, hist=True, kde=True, 
    #         bins=28, color = 'darkblue', 
    #         hist_kws={'edgecolor':'black'},
    #         kde_kws={'linewidth': 4})
             
    sns.kdeplot(latencyYAxis)

    # Add labels
    plt.title('PDF of Latency')
    plt.xlabel('Latency(s)')
    plt.ylabel('Density')
    
    plt.savefig(logDir+"PDF",bbox_inches="tight")
    
def plotLatencyCDF():
    global latencyYAxis
    hist_kwargs = {"linewidth": 2,
                  "edgecolor" :'salmon',
                  "alpha": 0.4, 
                  "color":  "w",
                  "label": "Histogram",
                  "cumulative": True}
    kde_kwargs = {'linewidth': 3,
                  'color': "blue",
                  "alpha": 0.7,
                  'label':'Kernel Density Estimation Plot',
                  'cumulative': True}
    #sns.distplot(latencyYAxis, hist_kws=hist_kwargs, kde_kws=kde_kwargs)
    sns.ecdfplot(latencyYAxis)
    
    # Add labels
    plt.title('CDF of Latency')
    plt.xlabel('Latency(s)')
    plt.ylabel('Density')
    
    plt.savefig(logDir+"CDF",bbox_inches="tight")
      
parser = argparse.ArgumentParser(description='Script for measuring latency for each message.')
parser.add_argument('--number-of-switches', dest='switches', type=int, default=0, help='Number of switches')
parser.add_argument('--log-dir', dest='logDir', type=str, help='Producer log directory')

args = parser.parse_args()

switches = args.switches
logDir = args.logDir

os.system("sudo rm "+logDir+"latency-log.txt"+"; sudo touch "+logDir+"latency-log.txt")  
os.makedirs(logDir+"cons-latency-logs", exist_ok=True)

print(datetime.now())

initConsStruct(switches)
readConsumerData()

for prodId in range(switches):
    getProdDetails(prodId+1)

plotLatencyScatter()
clearExistingPlot()

plotLatencyScatterSorted(switches)
clearExistingPlot()

plotLatencyPDF()
clearExistingPlot()

plotLatencyCDF()
clearExistingPlot()

plotConsTopicLactencyScatter(switches)
clearExistingPlot()

# latencyLog = open(logDir+"/latency-log.txt", "a")
# latencyLog.write("Produced messages: " + str(prodCount) + "\n")
# latencyLog.write("Consumed messages: " + str(consCount) + "\n")
# latencyLog.write("Percentage of extra latency message: " + str( "{:.2f}".format((extraLatencyMessage/consCount) * 100)) + "%")
# latencyLog.close()
