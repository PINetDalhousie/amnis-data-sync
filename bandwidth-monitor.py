#!/usr/bin/python

import subprocess
import os
import time


if os.path.isfile("bandwidth-log.txt"):
	print("Erase bandwidth log")
	os.system("sudo rm bandwidth-log.txt")


while True:
	bandwidthLog = open("bandwidth-log.txt", "a")

	#TODO: change for any topology
	for i in range(2):
		bandwidthLog.write(">>>> S" + str(i+1) + " <<<<\n")
		statsProcess = subprocess.Popen("sudo ovs-ofctl dump-ports s"+str(i+1), shell=True, stdout=subprocess.PIPE)
		stdout = statsProcess.communicate()[0]
		bandwidthLog.write(stdout)

	bandwidthLog.close()
	time.sleep(5)
