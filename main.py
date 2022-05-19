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

GROUP_MIN_SESSION_TIMEOUT_MS = 6000
GROUP_MAX_SESSION_TIMEOUT_MS = 1800000

REPLICA_LAG_TIME_MAX_MS = 30000


# Kill all subprocesses
def killSubprocs(brokerPlace, zkPlace):	
	os.system("sudo pkill -9 -f bandwidth-monitor.py")
	os.system("sudo pkill -9 -f producer.py")
	os.system("sudo pkill -9 -f consumer.py")
	os.system("sudo pkill -9 -f consumerSingle.py")

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

	# Check disconnect duration
	if (args.disconnectDuration >= args.duration):
		print("ERROR: Disconnect duration should be less than simulation duration.")
		sys.exit(1)				

if __name__ == '__main__': 

	parser = argparse.ArgumentParser(description='Emulate data sync in mission critical networks.')
	parser.add_argument('--topo', dest='topo', type=str, default='tests/simple.graphml', help='Network topology')
	#parser.add_argument('topo', type=str, help='Network topology')
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

	parser.add_argument('--single-consumer', dest='singleConsumer', action='store_true', help='Use a single, always connected consumer (per node) for the entire simulation')
	parser.add_argument('--relocate', dest='relocate', action='store_true', help='Relocate a random node during the simulation')
	parser.add_argument('--disconnect', dest='disconnectDuration', type=int, default=0, help='Duration of the disconnection (in seconds)')
	parser.add_argument('--latency-after-setup', dest='latencyAfterSetup', action='store_true', help='Lower the network latency before setting up Kafka, then set it back once Kafka is set up.')	

	args = parser.parse_args()

	# TODO: TEMP - hardcode for testing
	args.topo = 'tests/input/simple-three-node.graphml'
	args.nBroker = 3
	args.nZk = 3
	args.nTopics = 3
	args.replication = 3
	args.mSizeString = 'fixed,1000'
	args.mRate = 1.0
	args.consumerRate = 0.5
	args.messageFilePath = 'message-data/xml/Cars103.xml'
	args.topicCheckInterval = 0.1	
	args.duration = 100
	args.compression = 'gzip'
	args.replicaMaxWait = 5000
	args.replicaMinBytes = 200000
	args.disconnectDuration = 0
	args.relocate = False
	args.singleConsumer = True
	args.setNetworkDelay = False
	# END

	print(args)	
	validateInput(args)
	
	#Clean up mininet state
	cleanProcess = subprocess.Popen("sudo mn -c", shell=True)
	time.sleep(2)

	# Log the test args
	logging.info("Test args:\n %s", args)

	#Instantiate network
	emulatedTopo = emuNetwork.CustomTopo(args.topo)	

	# Create network
	net = Mininet(topo = None,
			controller=RemoteController,
			link = TCLink,
			autoSetMacs = True,
			autoStaticArp = True,
			build=False)

	# Add topo to network
	net.topo = emulatedTopo
	net.build()

	brokerPlace, zkPlace = emuKafka.placeKafkaBrokers(net, args.nBroker, args.nZk)

	#TODO: remove debug code
	killSubprocs(brokerPlace, zkPlace)
	emuLogs.cleanLogs()
	emuKafka.cleanKafkaState(brokerPlace)
	#emuZk.cleanZkState(zkPlace)

	logDir = emuLogs.configureLogDir(args.nBroker, args.mSizeString, args.mRate, args.nTopics, args.replication)
	#emuZk.configureZkCluster(zkPlace)
	emuKafka.configureKafkaCluster(brokerPlace, zkPlace, args)
	
	#Start network
	print("Starting Network")
	net.start()
	for switch in net.switches:
		net.get(switch.name).start([])

	logging.info('Network started')
	#emuNetwork.configureNetwork(args.topo)
	#time.sleep(5)

	# Set network delay to a low value to allow Kafka to set up properly
	if args.latencyAfterSetup:
		emuLoad.setNetworkDelay(net, '1ms')

	print("Testing network connectivity")
	net.pingAll()
	print("Finished network connectivity test")
		
	#Start monitoring tasks
	#popens[pID] = subprocess.Popen("sudo python3 bandwidth-monitor.py "+str(args.nBroker)+" " +args.mSizeString+" "+str(args.mRate) +" " +str(args.nTopics) +" "+ str(args.replication) + " "+ str(args.nZk) +" &", shell=True)
	#pID += 1

	#emuZk.runZk(net, zkPlace)
	emuKafka.runKafka(net, brokerPlace, logDir)
					
	emuLoad.runLoad(net, args.nTopics, args.replication, args.mSizeString, args.mRate, args.tClassString, args.consumerRate, args.duration, args)
	print("Simulation complete")

	# to kill all the running subprocesses
	killSubprocs(brokerPlace, zkPlace)

	net.stop()
	logging.info('Network stopped')

	#Need to clean both kafka and zookeeper state before a new simulation
	emuKafka.cleanKafkaState(brokerPlace)
	#emuZk.cleanZkState(zkPlace)

	#TODO: Temp hardcode to run plotting
	#os.system(f"sudo python3 modifiedLatencyPlotScript.py --number-of-switches {args.nBroker} --log-dir logs/kafka/nodes:{args.nBroker}_mSize:fixed,1000_mRate:30.0_topics:{args.nBroker}_replication:{args.nBroker}/")
	#os.system(f"sudo python3 bandwidthPlotScript.py --number-of-switches {args.nBroker} --port-type access-port --message-size fixed,1000 --message-rate 30.0 --ntopics {args.nBroker} --replication {args.nBroker} --log-dir logs/kafka/nodes:{args.nBroker}_mSize:fixed,1000_mRate:30.0_topics:{args.nBroker}_replication:{args.nBroker}/ --switch-ports S1-P1,S2-P1,S3-P1,S4-P1,S5-P1,S6-P1")	














