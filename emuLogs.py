#!/usr/bin/python3

import os
import logging

import matplotlib.pyplot as plt

interval = 5


def configureKafkaLogDir():
	os.system("sudo mkdir -p logs/kafka/bandwidth/")
	os.system("sudo mkdir -p logs/kafka/prod/")
	os.system("sudo mkdir -p logs/kafka/cons/")

	logging.basicConfig(filename='logs/kafka/events.log',
						format='%(levelname)s:%(message)s',
 						level=logging.INFO)


def cleanKafkaLogs():
	os.system("sudo rm -rf logs/kafka/")
    

def configureKraftLogDir():
	os.system("sudo mkdir -p logs/kraft/bandwidth/")
	os.system("sudo mkdir -p logs/kraft/prod/")
	os.system("sudo mkdir -p logs/kraft/cons/")

	logging.basicConfig(filename='logs/kraft/events.log',
						format='%(levelname)s:%(message)s',
 						level=logging.INFO)


def cleanKraftLogs():
	os.system("sudo rm -rf logs/kraft/")    


# def getPorts(switch):
#     port_list = []
#     line_flag = 0
    
#     #reading log for each switch
#     with open('logs/bandwidth/bandwidth-log'+str(switch)+'.txt') as f:
#         for line in f:
#             if "port LOCAL" in line:            
#                 f.readline()        #skipping two lines
#                 continue
#             elif "OFPST_PORT" in line:
#                 if line_flag == 0:
#                     number_of_port = line.split(":")
#                     number_of_port = number_of_port[1].split("ports")
#                     number_of_port = int(number_of_port[0])
#                     line_flag +=1
#                 else:
#                     break
#             elif "port" in line:
#                 split_line = line.split(":")
#                 split_line = split_line[0].split("port")            
#                 port_list.append(int(split_line[1]))
                
#     return port_list, number_of_port

# def getStatsValue(switch,portNumber, portFlag):
#     count=0
#     dataList = []
#     bandwidth =  [0]
#     txFlag = 0
#     with open('logs/bandwidth/bandwidth-log'+str(switch)+'.txt') as f:
#         for line in f:
#             if "port  "+str(portNumber)+":" in line:  
#                 if portFlag == 'tx pkts':
#                     line = f.readline()
                    
#                 elif portFlag == 'tx bytes':
#                     line = f.readline()
#                     txFlag = 1                    
#                 if txFlag == 1:
#                     newPortFlag = "bytes"
#                     data = line.split(newPortFlag+"=")
#                 else:
#                     data = line.split(portFlag+"=")

#                 data = data[1].split(",")
#                 dataList.append(int(data[0]))
#                 if count>0: 
#         	        bandwidth.append((dataList[count]-dataList[count-1])/interval)
#                 count+=1

#     return bandwidth,count
      
# #drawing single plot for a single flag of each port for one switch           
# def drawPlot(switchNo,portNo,portFlag,x,y,yLabel):      
#     plt.plot(x,y, label = "S-" +str(switchNo)+" P-"+str(portNo))
#     plt.xlabel('Time (sec)')
#     plt.ylabel(yLabel)
#     if portFlag=="bytes":
#         plt.title("Bandwidth for rx bytes")
#     else:
#         plt.title("Bandwidth for " + portFlag) 

#     plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='xx-small')
    

# #saving all lines in a single figure for a single Flag    
# def singlePlotDraw(switches, portFlag):
#     # clear the previous figure
#     plt.close()
#     plt.cla()
#     plt.clf()  
    
#     for s in range(switches):
#         port_list, number_of_port= getPorts(s+1)

#         for i in port_list:
#             bandwidth, occurrence = getStatsValue(s+1,i, portFlag)	
#             timeList = list(range(0,occurrence*interval,interval))
#             if portFlag=="rx pkts" or portFlag=="tx pkts":
#                 drawPlot(s+1,i,portFlag,timeList, bandwidth, "Bandwidth (pkts/sec)")
#             else:
#                 drawPlot(s+1,i,portFlag,timeList, bandwidth, "Bandwidth (bytes/sec)")
#     if portFlag=="bytes":
#         plt.savefig('logs/bandwidth/rx bytes',bbox_inches="tight")
#     else:    
#         plt.savefig('logs/bandwidth/'+portFlag,bbox_inches="tight")        


# def plotBandwidth(switches):
#     singlePlotDraw(switches, "rx pkts")
#     singlePlotDraw(switches, "bytes")
#     singlePlotDraw(switches, "tx pkts")
#     singlePlotDraw(switches, "tx bytes")
