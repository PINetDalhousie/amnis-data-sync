#!/usr/bin/python3

from ast import arg
from re import I
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
import emuSpark
import emuMySQL

pID=0
popens = {}

GROUP_MIN_SESSION_TIMEOUT_MS = 6000
GROUP_MAX_SESSION_TIMEOUT_MS = 1800000

REPLICA_LAG_TIME_MAX_MS = 30000

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
	if (args.duration < 1):
		print("ERROR: Time should be greater than zero.")
		sys.exit(1)

	if(args.topicCheckInterval < 0):
		print("ERROR: Topic check interval should be greater than zero.")
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

	if args.acks < 0 or args.acks >= 3:
		print("ERROR: Acks should be 0, 1 or 2 (which represents all)")
		sys.exit(1)

	compressionList = ['gzip', 'snappy', 'lz4']
	if not (args.compression in compressionList) and args.compression != 'None':
		print("ERROR: Compression should be None or one of the following:")
		print(*compressionList, sep = ", ") 
		sys.exit(1)

	# Note that the value must be in the allowable range as configured in the broker configuration by group.min.session.timeout.ms and group.max.session.timeout.ms
	if args.sessionTimeout < GROUP_MIN_SESSION_TIMEOUT_MS or args.sessionTimeout > GROUP_MAX_SESSION_TIMEOUT_MS:
		print("ERROR: Session timeout must be in the allowable range as configured in the broker configuration by group.min.session.timeout.ms value of " + str(GROUP_MIN_SESSION_TIMEOUT_MS) + " and group.max.session.timeout.ms value of " + str(GROUP_MAX_SESSION_TIMEOUT_MS))
		sys.exit(1)

	# This value should always be less than the replica.lag.time.max.ms at all times to prevent frequent shrinking of ISR for low throughput topics
	if args.replicaMaxWait >= REPLICA_LAG_TIME_MAX_MS:
		print("ERROR: replica.fetch.wait.max.ms must be less than the replica.lag.time.max.ms value of " +  str(REPLICA_LAG_TIME_MAX_MS) + " at all times")
		sys.exit(1)
	
	if(args.topicCheckInterval * args.nTopics) > args.duration:
		print("WARNING: Not all topics will be checked within the given duration of the simulation. Simulation Time:" +  str(args.duration) + " seconds. Time Required to Check All Topics at Least Once: "+  str(args.topicCheckInterval * args.nTopics) + " seconds.")

if __name__ == '__main__': 

	parser = argparse.ArgumentParser(description='Emulate data sync in mission critical networks.')
	parser.add_argument('topo', type=str, help='Network topology')
	parser.add_argument('--nbroker', dest='nBroker', type=int, default=0,
                    help='Number of brokers')
	parser.add_argument('--nzk', dest='nZk', type=int, default=0, help='Number of Zookeeper instances')
	parser.add_argument('--ntopics', dest='nTopics', type=int, default=1, help='Number of topics')
	parser.add_argument('--replication', dest='replication', type=int, default=1, help='Replication factor')
	parser.add_argument('--message-size', dest='mSizeString', type=str, default='fixed,10', help='Message size distribution (fixed, gaussian)')
	parser.add_argument('--message-rate', dest='mRate', type=float, default=1.0, help='Message rate in msgs/second')
	parser.add_argument('--traffic-classes', dest='tClassString', type=str, default='1', help='Number of traffic classes')
	parser.add_argument('--consumer-rate', dest='consumerRate', type=float, default=0.5, help='Rate consumers check for new messages in checks/second')
	parser.add_argument('--time', dest='duration', type=int, default=10, help='Duration of the simulation (in seconds)')
	
	parser.add_argument('--acks', dest='acks', type=int, default=1, help='Controls how many replicas must receive the record before producer considers successful write')
	parser.add_argument('--compression', dest='compression', type=str, default='None', help='Compression algorithm used to compress data sent to brokers')
	parser.add_argument('--batch-size', dest='batchSize', type=int, default=16384, help='When multiple records are sent to the same partition, the producer will batch them together (bytes)')
	parser.add_argument('--linger', dest='linger', type=int, default=0, help='Controls the amount of time (in ms) to wait for additional messages before sending the current batch')
	parser.add_argument('--request-timeout', dest='requestTimeout', type=int, default=30000, help='Controls how long producer waits for a reply from server when sending data')

	parser.add_argument('--fetch-min-bytes', dest='fetchMinBytes', type=int, default=1, help='Minimum amount of data consumer needs to receive from the broker when fetching records (bytes)')
	parser.add_argument('--fetch-max-wait', dest='fetchMaxWait', type=int, default=500, help='How long the broker will wait (in ms) before sending data to consumer')
	parser.add_argument('--session-timeout', dest='sessionTimeout', type=int, default=10000, help='Time (in ms) a consumer can be out of contact with brokers while still considered alive')
	
	parser.add_argument('--replica-max-wait', dest='replicaMaxWait', type=int, default=500, help='Max wait time for each fetcher request issued by follower replicas')
	parser.add_argument('--replica-min-bytes', dest='replicaMinBytes', type=int, default=1, help='Minimum bytes expected for each fetch response')

	parser.add_argument('--create-plots', dest='createPlots', action='store_true')

	parser.add_argument('--message-file', dest='messageFilePath', type=str, default='None', help='Path to a file containing the message to be sent by producers')
	parser.add_argument('--topic-check', dest='topicCheckInterval', type=float, default=1.0, help='Minimum amount of time (in seconds) the consumer will wait between checking topics')

	parser.add_argument('--only-kafka', dest='onlyKafka', type=int, default=0, help='To run Kafka only')
	parser.add_argument('--only-spark', dest='onlySpark', type=int, default=0, help='To run Spark application only')
	  
	args = parser.parse_args()

	# print(args)
	validateInput(args)
	
	#Clean up mininet state
	cleanProcess = subprocess.Popen("sudo mn -c", shell=True)
	time.sleep(2)

	#Instantiate network
	emulatedTopo = emuNetwork.CustomTopo(args.topo)

	net = Mininet(topo = emulatedTopo,
			# controller=RemoteController,
			link = TCLink,
			autoSetMacs = True,
			autoStaticArp = True)

	brokerPlace, zkPlace, topicPlace, prodDetailsList, consDetailsList, isDisconnect, \
		dcDuration, dcLinks = emuKafka.placeKafkaBrokers(net, args.topo, args.onlySpark)

	# if args.onlyKafka == 0:
	#Add dependency to connect kafka & Spark
	emuSpark.addSparkDependency()

	#Get Spark configuration
	sparkDetailsList, mysqlPath = emuSpark.getSparkDetails(net, args.topo)
	
	#TODO: remove debug code
	killSubprocs(brokerPlace, zkPlace)
	emuLogs.cleanLogs()
	emuMySQL.cleanMysqlState()
	emuKafka.cleanKafkaState(brokerPlace)
	emuZk.cleanZkState(zkPlace)
        
	if mysqlPath != "":
		print("MySQL path: "+mysqlPath)
		emuMySQL.configureKafkaMysqlConnection(brokerPlace)
		# Add NAT connectivity
		net.addNAT().configDefault()  

	emuLogs.configureLogDir(args.nBroker, args.mSizeString, args.mRate, args.nTopics, args.replication)
	emuZk.configureZkCluster(zkPlace)
	emuKafka.configureKafkaCluster(brokerPlace, zkPlace, args)

	#Start network
	net.start()
	logging.info('Network started')

	emuNetwork.configureNetwork(args.topo)
	time.sleep(1)

	print("Testing network connectivity")
	net.pingAll()
	print("Finished network connectivity test")
    		
	#Start monitoring tasks
	popens[pID] = subprocess.Popen("sudo python3 bandwidth-monitor.py "+str(args.nBroker)+" " +args.mSizeString+" "+str(args.mRate) +" " +str(args.nTopics) +" "+ str(args.replication) + " "+ str(args.nZk) +" &", shell=True)
	pID += 1

	emuZk.runZk(net, zkPlace)
	emuKafka.runKafka(net, brokerPlace)
    
	emuLoad.runLoad(net, args, topicPlace, prodDetailsList, consDetailsList, sparkDetailsList,\
		 mysqlPath, brokerPlace, isDisconnect, dcDuration, dcLinks)

	# CLI(net)
	print("Simulation complete")

	# to kill all the running subprocesses
	killSubprocs(brokerPlace, zkPlace)

	net.stop()
	logging.info('Network stopped')

	# Clean kafka-MySQL connection state before new simulation
	if mysqlPath != "":
		emuMySQL.cleanMysqlState()

	#Need to clean both kafka and zookeeper state before a new simulation
	emuKafka.cleanKafkaState(brokerPlace)
	emuZk.cleanZkState(zkPlace)

	#Need to clean spark dependency before a new simulation
	emuSpark.cleanSparkDependency()
