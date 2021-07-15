#!/usr/bin/python3

from mininet.net import Mininet
from mininet.cli import CLI
from mininet.node import RemoteController

import os
import sys
import subprocess
import time

import argparse

import emuNetwork
import emuKafka
import emuZk
import emuLoad

pID=0
popens = {}


# Kill all subprocesses
def killSubprocs(brokerPlace, zkPlace):	
	os.system("sudo pkill -9 -f bandwidth-monitor.py")

	for bID in brokerPlace:
		os.system("sudo pkill -9 -f server"+str(bID)+".properties") 

	os.system("sudo pkill -9 -f zookeeper") 



if __name__ == '__main__': 

	parser = argparse.ArgumentParser(description='Emulate data sync in mission critical networks.')
	parser.add_argument('topo', type=str, help='Network topology')
	parser.add_argument('--nbroker', dest='nBroker', type=int, default=0,
                    help='Number of brokers')
	parser.add_argument('--nzk', dest='nZk', type=int, default=0, help='Number of Zookeeper instances')

	args = parser.parse_args()

	print(args)
	
	#Clean up mininet state
	cleanProcess = subprocess.Popen("sudo mn -c", shell=True)
	time.sleep(2)

	#Instantiate network
	emulatedTopo = emuNetwork.CustomTopo(args.topo)

	net = Mininet(topo = emulatedTopo,
			controller=RemoteController,
			autoSetMacs = True,
			autoStaticArp = True)

	brokerPlace, zkPlace = emuKafka.placeKafkaBrokers(net, args.nBroker, args.nZk)

	#TODO: remove debug code
	killSubprocs(brokerPlace, zkPlace)
	emuKafka.cleanKafkaState(brokerPlace)
	emuZk.cleanZkState(zkPlace)

	emuZk.configureZkCluster(zkPlace)
	emuKafka.configureKafkaCluster(brokerPlace, zkPlace)
	
	#Start network
	net.start()

	emuNetwork.configureNetwork(args.topo)
	time.sleep(1)

	print("Testing network connectivity")
	net.pingAll()
	print("Finished network connectivity test")
		
	#Start monitoring tasks
	popens[pID] = subprocess.Popen("sudo python bandwidth-monitor.py &", shell=True)
	pID += 1

	emuZk.runZk(net, zkPlace)
	emuKafka.runKafka(net, brokerPlace)

	emuLoad.runLoad(net)
	print("Simulation complete")

	# to kill all the running subprocesses
	killSubprocs(brokerPlace, zkPlace)

	net.stop()

	#Need to clean both kafka and zookeeper state before a new simulation
	emuKafka.cleanKafkaState(brokerPlace)
	emuZk.cleanZkState(zkPlace)














