#!/usr/bin/python3

import subprocess
import os
import time
import sys

interval = 5

brokers = int(sys.argv[1])
mSizeString = sys.argv[2]
mRate = float(sys.argv[3])
nTopics = int(sys.argv[4])
replication = int(sys.argv[5])


while True:

	for i in range(brokers):
        
		bandwidthLog = open("logs/kafka/"+"nodes:" +str(brokers)+ "_mSize:"+ mSizeString+ "_mRate:"+ str(mRate)+ "_topics:"+str(nTopics) +"_replication:"+str(replication)+"/bandwidth/bandwidth-log" + str(i+1) + ".txt", "a")

		statsProcess = subprocess.Popen("sudo ovs-ofctl dump-ports s"+str(i+1), shell=True, stdout=subprocess.PIPE)
		stdout = statsProcess.communicate()[0]
		bandwidthLog.write(stdout.decode("utf-8"))  #converting bytes to string
		bandwidthLog.close()

	time.sleep(interval)










