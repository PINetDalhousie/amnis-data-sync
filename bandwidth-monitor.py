#!/usr/bin/python3

import subprocess
import os
import time
import sys

interval = 5

os.system("sudo mkdir logs/bandwidth/")
switches = int(sys.argv[1]) #get the passed argument

while True:

	for i in range(switches):
		bandwidthLog = open("logs/bandwidth/bandwidth-log" + str(i+1) + ".txt", "a")

		statsProcess = subprocess.Popen("sudo ovs-ofctl dump-ports s"+str(i+1), shell=True, stdout=subprocess.PIPE)
		stdout = statsProcess.communicate()[0]
		bandwidthLog.write(stdout.decode("utf-8"))  #converting bytes to string
		bandwidthLog.close()

	time.sleep(interval)










