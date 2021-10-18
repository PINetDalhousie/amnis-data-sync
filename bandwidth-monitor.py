#!/usr/bin/python3

import subprocess
import os
import time
import sys

interval = 5

#get and process the passed arguments
inputDetails = sys.argv[1]
firstSplit = inputDetails.split('mSize:')
switches = int(firstSplit[0])
secondSplit = firstSplit[1].split('mRate:')
mSizeString = secondSplit[0]
thirdSplit = secondSplit[1].split('nTopics:')
mRate = float(thirdSplit[0])
fourthSplit = thirdSplit[1].split('replication:')
nTopics = int(fourthSplit[0])
# replication = int(fourthSplit[1])
fifthSplit = fourthSplit[1].split('nZk:')
replication = int(fifthSplit[0])
nZk = int(fifthSplit[1])

# overwrite the directory if exists
if nZk == 0:
	os.system("sudo rm -rf logs/kraft/bandwidth/"+"nodes:" +str(switches)+ "_mSize:"+ mSizeString+ "_mRate:"+ str(mRate)+ "_topics:"+str(nTopics) +"_replication:"+str(replication)+"/"+"; sudo mkdir logs/kraft/bandwidth/"+"nodes:" +str(switches)+ "_mSize:"+ mSizeString+ "_mRate:"+ str(mRate)+ "_topics:"+str(nTopics) +"_replication:"+str(replication)+"/")
else:
	os.system("sudo rm -rf logs/kafka/bandwidth/"+"nodes:" +str(switches)+ "_mSize:"+ mSizeString+ "_mRate:"+ str(mRate)+ "_topics:"+str(nTopics) +"_replication:"+str(replication)+"/"+"; sudo mkdir logs/kafka/bandwidth/"+"nodes:" +str(switches)+ "_mSize:"+ mSizeString+ "_mRate:"+ str(mRate)+ "_topics:"+str(nTopics) +"_replication:"+str(replication)+"/")
    

while True:

	for i in range(switches):
		if nZk == 0:        
			bandwidthLog = open("logs/kraft/bandwidth/"+"nodes:" +str(switches)+ "_mSize:"+ mSizeString+ "_mRate:"+ str(mRate)+ "_topics:"+str(nTopics) +"_replication:"+str(replication)+"/bandwidth-log" + str(i+1) + ".txt", "a")
		else:
			bandwidthLog = open("logs/kafka/bandwidth/"+"nodes:" +str(switches)+ "_mSize:"+ mSizeString+ "_mRate:"+ str(mRate)+ "_topics:"+str(nTopics) +"_replication:"+str(replication)+"/bandwidth-log" + str(i+1) + ".txt", "a")

		statsProcess = subprocess.Popen("sudo ovs-ofctl dump-ports s"+str(i+1), shell=True, stdout=subprocess.PIPE)
		stdout = statsProcess.communicate()[0]
		bandwidthLog.write(stdout.decode("utf-8"))  #converting bytes to string
		bandwidthLog.close()

	time.sleep(interval)










