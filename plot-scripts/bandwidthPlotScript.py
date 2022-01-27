#!/usr/bin/python3

import os
import logging

import argparse

import math
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties

interval = 5
inputBarDraw = 0

font = FontProperties()
font.set_family('serif')
font.set_name('Liberation Serif')
font.set_size(12)

matplotlib.rcParams['pdf.fonttype'] = 42   # for ACM submission
matplotlib.rcParams['ps.fonttype'] = 42
    
def clearExistingPlot():
    # clear the previous figure
    plt.close()
    plt.cla()
    plt.clf()    
    
def processMessageInput():
    mSizeParams = args.mSizeString.split(',')
    msgSize = 0
    if mSizeParams[0] == 'fixed':
        msgSize = int(mSizeParams[1])
    elif mSizeParams[0] == 'gaussian':
        msgSize = int(gauss(float(mSizeParams[1]), float(mSizeParams[2])))

    if msgSize < 1:
        msgSize = 1
    
    return msgSize

# plot input data rate in one horizontal line for all/individual host ports
def plotInputDataRate(msgSize, mRate, countX, switches):
    dataRate = msgSize * mRate * switches
    dataRateList = [dataRate] * countX
    return dataRateList

#to get bandwidth list for a specific port in a specific switch
def getStatsValue(switch,portNumber, portFlag):
    count=0
    dataList = []
    bandwidth =  [0]
    txFlag = 0
    maxBandwidth = -1.0
    
    with open(logDirectory+'bandwidth-log'+str(switch)+'.txt') as f:
        
        for line in f:
            if portNumber >= 10:
                spaces = " "
            else:
                spaces = "  "
            if "port"+spaces+str(portNumber)+":" in line: 
                
                if portFlag == 'tx pkts':
                    line = f.readline()
                    
                elif portFlag == 'tx bytes':
                    line = f.readline()
                    txFlag = 1           
                if txFlag == 1:
                    newPortFlag = "bytes"
                    data = line.split(newPortFlag+"=")
                else:
                    data = line.split(portFlag+"=")

                data = data[1].split(",")
                dataList.append(int(data[0]))
                if count>0: 
                    individualBandwidth = (dataList[count]-dataList[count-1])/interval
                    bandwidth.append(individualBandwidth)
                    if individualBandwidth > maxBandwidth:
                        maxBandwidth = individualBandwidth
                count+=1

    return bandwidth,count, maxBandwidth
      
#drawing single plot for a single flag of each port for one switch           
def drawPlot(switchNo,portNo,portFlag,x,y,yLabel,occurrence): 
    global inputBarDraw
    if inputBarDraw == 0:
        msgSize = processMessageInput()
        dataRateList = plotInputDataRate(msgSize, args.mRate, occurrence, 1)
        dataRateList = [x / 1000000 for x in dataRateList]

#         plt.figure(figsize=(3,3))
        if portFlag != "rx pkts" or portFlag != "tx pkts":
            plt.plot(x,dataRateList,label = "input")
            inputBarDraw = 1

    plt.plot(x,y, label = "S-" +str(switchNo)+" P-"+str(portNo))
    plt.xlabel('Time (s)', fontproperties=font)
    if portFlag == "rx pkts" or portFlag == "tx pkts":
        plt.ylabel('Throughput (pkts/s)', fontproperties=font)
    else:
        plt.ylabel('Throughput (Mbytes/s)', fontproperties=font)


    plt.xticks(fontproperties=font)
    plt.yticks(fontproperties=font)

    plt.plot(x,y)

#     plt.ylim([0,3.5])           # to limit the Y-axis value

    if portFlag=="bytes":
        plt.title(args.portType+" rx bytes("+str(args.switches)+" nodes "+str(args.nTopics)+" topics "+str(args.replication)+" replication)")
    else:
        plt.title(args.portType+" " + portFlag+"("+str(args.switches)+" nodes "+str(args.nTopics)+" topics "+str(args.replication)+" replication)") 

    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='xx-small')

    
# aggregated plot for all switches
def aggregatedPlot(portFlag,x,y, yLeaderLess, yLabel, msgSize, countX):      
    plt.plot(x,y, label = "output (with leader)")
#     plt.plot(x,yLeaderLess, label = "output (without leader)")
#     if portFlag != "rx pkts" or portFlag != "tx pkts":
    dataRateList = plotInputDataRate(msgSize, args.mRate, countX, args.switches)
    dataRateList = [x / 1000000 for x in dataRateList]
#     plt.plot(x,dataRateList , label = "input")

    plt.xlabel('Time (sec)')
    plt.ylabel(yLabel)
    
#     plt.ylim([0, 35])
    
    if portFlag=="bytes":
        plt.title("Aggregated Bandwidth for rx bytes("+str(args.switches)+" nodes "+str(args.nTopics)+" topics "+str(args.replication)+" replication)")
    else:
        plt.title("Aggregated Bandwidth for " + portFlag+"("+str(args.switches)+" nodes "+str(args.nTopics)+" topics "+str(args.replication)+" replication)") 

    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    
#checking input vs output to measure control traffic overhead
def overheadCheckPlot(portFlag, msgSize):
    clearExistingPlot()  
    
    allBandwidth = []
    countX = 0
    
    portParams = args.switchPorts.split(',')
    for ports in portParams:
        portId, switchId = parseInput(ports)
    
        bandwidth, occurrence, maxBandwidth = getStatsValue(switchId,portId, portFlag)
        
        if countX == 0:
            countX = occurrence
        
        if len(bandwidth)<countX:
            for k in range(countX-len(bandwidth)):
                bandwidth.append(0)                    #filling with 0's to match up the length
            
        allBandwidth.append(bandwidth)

    bandwidthSum = []
    bandwidthSumLeaderLess = []
    for i in range(countX):
        valWithLeader = 0
#         valWithoutLeader = 0
        for j in range(args.switches):
            valWithLeader = valWithLeader+allBandwidth[j][i]
#             if (j+1) not in leaderReplicaList:         #to skip the leader replica curves
#                 valWithoutLeader = valWithoutLeader+allBandwidth[j][i]
        
        bandwidthSum.append(valWithLeader)
#         bandwidthSumLeaderLess.append(valWithoutLeader)
        
    timeList = list(range(0,countX*interval,interval))

    if portFlag=="rx pkts" or portFlag=="tx pkts":
        aggregatedPlot(portFlag,timeList, bandwidthSum, bandwidthSumLeaderLess, "Throughput (pkts/sec)", msgSize, countX)
    else:
        newBandwidthSum = [x / 1000000 for x in bandwidthSum]
        newBandwidthSumLeaderLess = [x / 1000000 for x in bandwidthSumLeaderLess]
        aggregatedPlot(portFlag,timeList, newBandwidthSum, newBandwidthSumLeaderLess, "Throughput (Mbytes/sec)", msgSize, countX)    
    

    if portFlag=="bytes":
        plt.savefig(logDirectory+args.portType+" aggregated rx bytes("+str(args.switches)+" nodes "+str(args.nTopics)+" topics "+str(args.replication)+" replication)",bbox_inches="tight")
    else:    
        plt.savefig(logDirectory+args.portType+" aggregated "+portFlag+"("+str(args.switches)+" nodes "+str(args.nTopics)+" topics "+str(args.replication)+" replication)",bbox_inches="tight")         

#for aggregated plot of all host entry ports
def plotAggregatedBandwidth():   
    msgSize = processMessageInput()
    overheadCheckPlot("bytes", msgSize)
#     clearExistingPlot()
#     overheadCheckPlot("rx pkts", msgSize)

       
#parsing the sigle port        
def parseInput(portSwitchId):
    portParams = portSwitchId.split('-')
    switchId = portParams[0].split('S')[1]
    portId = portParams[1].split('P')[1]

    return int(portId), int(switchId)
 
def drawIndividualBandwidth(portId, switchId, portFlag):
    bandwidth, occurrence, maxBandwidth = getStatsValue(switchId,portId, portFlag)        # Here portId=1 is the fixed entry port of hosts
    timeList = list(range(0,occurrence*interval,interval))
    
    if portFlag=="rx pkts" or portFlag=="tx pkts":
        drawPlot(switchId,portId,portFlag,timeList, bandwidth, "Bandwidth (pkts/sec)", occurrence)
    elif portFlag=="bytes":
        newBandwidth = [x / 1000000 for x in bandwidth]
        drawPlot(switchId,portId,portFlag,timeList, newBandwidth, "Received Bandwidth (Mbytes/s)", occurrence)
    else:    
        newBandwidth = [x / 1000000 for x in bandwidth]
        drawPlot(switchId,portId,portFlag,timeList, newBandwidth, "Transmitted Bandwidth (Mbytes/s)", occurrence)
        
    
    if portFlag=="bytes":
        plt.savefig(logDirectory+args.portType+" rx bytes("+str(args.switches)+" nodes "+str(args.nTopics)+" topics "+str(args.replication)+" replication).png",bbox_inches="tight")
    else:    
        plt.savefig(logDirectory+args.portType+" "+portFlag+"("+str(args.switches)+" nodes "+str(args.nTopics)+" topics "+str(args.replication)+" replication).png",bbox_inches="tight") 
        

def plotIndividualPortBandwidth():
    portParams = args.switchPorts.split(',')
    for ports in portParams:
        portId, switchId = parseInput(ports)
#         if switchId not in leaderReplicaList:                #to skip plotting the leader replicas
#         print("S"+str(switchId)+"-P"+str(portId))
        drawIndividualBandwidth(portId, switchId, "bytes")
    
    clearExistingPlot()
    
    for ports in portParams:
        portId, switchId = parseInput(ports)
        drawIndividualBandwidth(portId, switchId, "tx bytes")

    clearExistingPlot()
            
    for ports in portParams:
        portId, switchId = parseInput(ports)
        drawIndividualBandwidth(portId, switchId, "rx pkts")
        
    clearExistingPlot()
            
    for ports in portParams:
        portId, switchId = parseInput(ports)
        drawIndividualBandwidth(portId, switchId, "tx pkts")        
        
    clearExistingPlot()



#getting leader data from file
def getLeaderList():
    leaderReplicaList = []
#     if args.nZk == 0:
#         folderPath = 'amnis-data-sync/logs/kraft/bandwidth/'
#     else:
#         folderPath = 'amnis-data-sync/logs/kafka/bandwidth/'
#     leaderFilePath = folderPath+"nodes:" +str(args.switches)+ "_mSize:"+ args.mSizeString+ "_mRate:"+ str(args.mRate)+ "_topics:"+str(args.nTopics) +"_replication:"+str(args.replication)+"/leader-list.txt"
    leaderFilePath = logDirectory+"/leader-list.txt"
    with open(leaderFilePath, "r") as f:
        for line in f:
            leaderReplicaList.append(int(line.strip()))
    return leaderReplicaList        
#     print ("Leader replicas: " + str(leaderReplicaList)[1:-1])

def createHist():

    new_list = range(math.floor(min(leaderReplicaList)), math.ceil(max(leaderReplicaList))+1)
    plt.figure(figsize=(3,3))
    plt.hist(leaderReplicaList, args.nTopics)
#     plt.xticks(new_list)
#     plt.title('Leader Frequency Histogram for '+str(args.nTopics)+' topics')    
    
    plt.xlabel('Broker ID', fontproperties=font)
    plt.ylabel('Frequency', fontproperties=font)
    
    plt.xticks(range(0,int(max(new_list)+5), 5))
    plt.xticks(fontproperties=font)
    
    plt.yticks(range(0,10, 1))
    plt.yticks(fontproperties=font)
    
    
    plt.axhline(y=(args.nTopics/args.switches), color='r', linestyle='-')
    
    
    plt.savefig(logDirectory+"leader-frequency-histogram("+str(args.switches)+" nodes "+str(args.nTopics)+" topics "+str(args.replication)+" replication).pdf",bbox_inches="tight") 
    
# if __name__ == '__main__': 
parser = argparse.ArgumentParser(description='Script for plotting individual port log.')
parser.add_argument('--number-of-switches', dest='switches', type=int, default=0,
                help='Number of switches')
parser.add_argument('--switch-ports', dest='switchPorts', type=str, help='Plot bandwidth vs time in a port wise and aggregated manner')
parser.add_argument('--port-type', dest='portType', default="access-port", type=str, help='Plot bandwidth for access/trunc ports')
parser.add_argument('--message-size', dest='mSizeString', type=str, default='fixed,10', help='Message size distribution (fixed, gaussian)')
parser.add_argument('--message-rate', dest='mRate', type=float, default=1.0, help='Message rate in msgs/second')
# parser.add_argument('--folder-path', dest='path', type=str, default='amnis-data-sync/logs/bandwidth/', help='Switch bandwidth log directory')   
parser.add_argument('--ntopics', dest='nTopics', type=int, default=1, help='Number of topics')
parser.add_argument('--replication', dest='replication', type=int, default=1, help='Replication factor')
parser.add_argument('--nzk', dest='nZk', type=int, default=0, help='Kafka/Kraft')
parser.add_argument('--log-dir', dest='logDir', type=str, help='Producer log directory')

args = parser.parse_args()

# if args.nZk == 0:                      # for KRaft
#     folderPath = 'amnis-data-sync/logs/kraft/'
# else:
#     folderPath = 'amnis-data-sync/logs/kafka/'
    
# logDirectory = folderPath+"nodes:" +str(args.switches)+ "_mSize:"+ args.mSizeString+ "_mRate:"+ str(args.mRate)+ "_topics:"+str(args.nTopics) +"_replication:"+str(args.replication)+"/bandwidth/" 
logDirectory = args.logDir + "/bandwidth/"

# leaderReplicaList = getLeaderList()

plotIndividualPortBandwidth()           #for individual entry port plots
print("Individual "+args.portType+" bandwidth consumption plot created")

clearExistingPlot()
plotAggregatedBandwidth()      #for aggregated plot    
print("Aggregated plot created.")

clearExistingPlot()                                       
# createHist()    
# print("Histogram created")