#!/usr/bin/python3

import os
import logging

import argparse
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

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
    
    with open(logDir+'/prod/prod-'+str(prodId)+'.log') as f:
        for line in f:
            if "topic-" in line:
#                 msgProdTime = line.split(",")[0]
                msgProdTime = line.split(" INFO:Topic:")[0]
                topicSplit = line.split("topic-")
                topicId = topicSplit[1].split(";")[0]
                msgIdSplit = line.split("Message ID: ")
                msgId = msgIdSplit[1].split(";")[0]
                
#                 print("producer: "+str(prodId)+" time: "+msgProdTime+" topic: "+topicId+" message ID: "+msgId)
                prodCount+=1
                
                for consId in range(switches):
                    getConsDetails(consId+1, prodId, msgProdTime, topicId, msgId)
                    

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
                firstSplit = line.split("Latency of this message: 0:00:")
                latencyYAxis.append(float(firstSplit[1]) )

    plt.scatter(lineXAxis, latencyYAxis)
    plt.xlabel('Message')
    plt.ylabel('Latency(s)')
    plt.title("Latency Measurement")

    plt.savefig(logDir+"latency Plot",bbox_inches="tight")
                            

def plotLatencyPDF():
    global latencyYAxis

    sns.distplot(latencyYAxis, hist=True, kde=True, 
             bins=28, color = 'darkblue', 
             hist_kws={'edgecolor':'black'},
             kde_kws={'linewidth': 4})

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
    sns.distplot(latencyYAxis, hist_kws=hist_kwargs, kde_kws=kde_kwargs)
    
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

os.system("sudo rm "+logDir+"/latency-log.txt"+"; sudo touch "+logDir+"/latency-log.txt")  

for prodId in range(switches):
    getProdDetails(prodId+1)

plotLatencyScatter()

clearExistingPlot()

plotLatencyPDF()

clearExistingPlot()

plotLatencyCDF()

# latencyLog = open(logDir+"/latency-log.txt", "a")
# latencyLog.write("Produced messages: " + str(prodCount) + "\n")
# latencyLog.write("Consumed messages: " + str(consCount) + "\n")
# latencyLog.write("Percentage of extra latency message: " + str( "{:.2f}".format((extraLatencyMessage/consCount) * 100)) + "%")
# latencyLog.close()