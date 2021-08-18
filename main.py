#!/usr/bin/python3

from mininet.net import Mininet
from mininet.cli import CLI
from mininet.node import RemoteController
from mininet.link import TCLink

import os
import sys
import subprocess
import time

import argparse
import logging

import emuNetwork
import emuKafka
import emuZk
import emuLoad
import emuLogs

pID=0
popens = {}


# Kill all subprocesses
def killSubprocs(brokerPlace, zkPlace):	
	os.system("sudo pkill -9 -f bandwidth-monitor.py")
	os.system("sudo pkill -9 -f producer.py")
	os.system("sudo pkill -9 -f consumer.py")

	for bID in brokerPlace:
		os.system("sudo pkill -9 -f server"+str(bID)+".properties") 

	os.system("sudo pkill -9 -f zookeeper") 


def validateInput(args):

	#Check duration
	if args.duration < 1:
		print("ERROR: Time should be greater than zero.")
		sys.exit(1)

	#Check traffic classes
	tClassString = args.tClassString
	tClasses = tClassString.split(',')

	for tClass in tClasses:
		if(float(tClass) <= 0.1):
			print("ERROR: All traffic classes should have a weight greater than 0.1.")
			sys.exit(1)

	#Check message size
	mSizeString = args.mSizeString
	mSizeParams = mSizeString.split(',')

	mSizeDistList = ['fixed', 'gaussian']

	if not( mSizeParams[0] in mSizeDistList ):
		print("ERROR: Message size distribution not allowed.")
		sys.exit(1)

	if mSizeParams[0] == 'fixed':
		if len(mSizeParams) != 2:
			print("ERROR: Should specify a size for fixed size messages.")
			sys.exit(1)
		elif int(mSizeParams[1]) < 1:
			print("ERROR: Message size should be equal to or greater than 1.")
			sys.exit(1)
	elif mSizeParams[0] == 'gaussian':
		if len(mSizeParams) != 3:
			print("ERROR: Should specify mean and standard deviation for gaussian-sized messages.")
			sys.exit(1)
		elif float(mSizeParams[1]) < 1.0:
			print("ERROR: Mean message size should be equal to or greater than 1.0.")
			sys.exit(1)
		elif int(mSizeParams[2]) < 0.0:
			print("ERROR: Standard deviation for message size should be greater than zero.")
			sys.exit(1)

	#Check message rate
	mRate = args.mRate

	if mRate > 100:
		print("ERROR: Message rate should be less than 100 msg/second.")
		sys.exit(1)

	#Check consumer rate
	if args.consumerRate <= 0.0 or args.consumerRate > 100.0:
		print("ERROR: Consumer rate should be between 0 and 100 checks/second")
		sys.exit(1)



if __name__ == '__main__': 

	parser = argparse.ArgumentParser(description='Emulate data sync in mission critical networks.')
	parser.add_argument('topo', type=str, help='Network topology')
	parser.add_argument('--nbroker', dest='nBroker', type=int, default=0,
                    help='Number of brokers')
	parser.add_argument('--nzk', dest='nZk', type=int, default=0, help='Number of Zookeeper instances')
	parser.add_argument('--ntopics', dest='nTopics', type=int, default=1, help='Number of topics')
	parser.add_argument('--message-size', dest='mSizeString', type=str, default='fixed,10', help='Message size distribution (fixed, gaussian)')
	parser.add_argument('--message-rate', dest='mRate', type=float, default=1.0, help='Message rate in msgs/second')
	parser.add_argument('--traffic-classes', dest='tClassString', type=str, default='1', help='Number of traffic classes')
	parser.add_argument('--consumer-rate', dest='consumerRate', type=float, default=0.5, help='Rate consumers check for new messages in checks/second')
	parser.add_argument('--time', dest='duration', type=int, default=10, help='Duration of the simulation (in seconds)')
	parser.add_argument('--create-plots', dest='createPlots', action='store_true')

	args = parser.parse_args()

	print(args)
	validateInput(args)
	
	#Clean up mininet state
	cleanProcess = subprocess.Popen("sudo mn -c", shell=True)
	time.sleep(2)

	#Instantiate network
	emulatedTopo = emuNetwork.CustomTopo(args.topo)

	net = Mininet(topo = emulatedTopo,
			controller=RemoteController,
			link = TCLink,
			autoSetMacs = True,
			autoStaticArp = True)

	brokerPlace, zkPlace = emuKafka.placeKafkaBrokers(net, args.nBroker, args.nZk)

	#TODO: remove debug code
	killSubprocs(brokerPlace, zkPlace)
	emuLogs.cleanLogs()
	emuKafka.cleanKafkaState(brokerPlace)
	emuZk.cleanZkState(zkPlace)

	emuLogs.configureLogDir()
	emuZk.configureZkCluster(zkPlace)
	emuKafka.configureKafkaCluster(brokerPlace, zkPlace)
	
	#Start network
	net.start()
	logging.info('Network started')

	emuNetwork.configureNetwork(args.topo)
	time.sleep(1)

	print("Testing network connectivity")
	net.pingAll()
	print("Finished network connectivity test")
		
	#Start monitoring tasks
	popens[pID] = subprocess.Popen("sudo python3 bandwidth-monitor.py "+str(args.nBroker)+" &", shell=True)
	pID += 1

	emuZk.runZk(net, zkPlace)
	emuKafka.runKafka(net, brokerPlace)

	emuLoad.runLoad(net, args.nTopics, args.mSizeString, args.mRate, args.tClassString, args.consumerRate, args.duration)
	print("Simulation complete")

	# to kill all the running subprocesses
	killSubprocs(brokerPlace, zkPlace)

	net.stop()
	logging.info('Network stopped')

	if args.createPlots:
		emuLogs.plotBandwidth(args.nBroker)
		print("Plots created")

	#Need to clean both kafka and zookeeper state before a new simulation
	emuKafka.cleanKafkaState(brokerPlace)
	emuZk.cleanZkState(zkPlace)














