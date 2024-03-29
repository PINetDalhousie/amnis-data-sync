#!/usr/bin/python3

from mininet.net import Mininet
from mininet.cli import CLI
from mininet.node import RemoteController
from mininet.link import TCLink

from datetime import datetime

import os
import sys
import subprocess
import time

import argparse
import logging

import emuNetwork
import emuKafka
import emuKafkaKraft
import emuZk
import emuLoad
import emuLoadKraft
import emuLogs

pID=0
popens = {}

GROUP_MIN_SESSION_TIMEOUT_MS = 6000
GROUP_MAX_SESSION_TIMEOUT_MS = 1800000

REPLICA_LAG_TIME_MAX_MS = 30000


# Kill all subprocesses
def killSubprocs(brokerPlace):	
	os.system("pkill -9 -f bandwidth-monitor.py")
	os.system("pkill -9 -f producer.py")
	os.system("pkill -9 -f consumer.py")
	os.system("pkill -9 -f consumerSingle.py")
	os.system("pkill -9 -f java/target/amnis-java-1.0-SNAPSHOT.jar")

	for bID in brokerPlace:
		os.system("pkill -9 -f server"+str(bID)+".properties") 

	os.system("pkill -9 -f zookeeper") 


def validateInput(args):

	if (args.nBroker < 0):
		print("ERROR: Number of brokers should not be negative.")
		sys.exit(1)

	if (args.nZk < 0):
		print("ERROR: Number of Zookeeper instances should not be negative.")
		sys.exit(1)

	if (args.nTopics < 1):
		print("ERROR: Number of topics should be greater than zero.")
		sys.exit(1)

	if (args.replication < 1):
		print("ERROR: Replication factor should be greater than zero.")
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
			print("ERROR: Standard deviation for message size should be equal to or greater than zero.")
			sys.exit(1)

	#Check message rate
	if args.mRate > 100:
		print("ERROR: Message rate should be less than or equal to 100 messages/second.")
		sys.exit(1)

	#Check traffic classes
	tClassString = args.tClassString
	tClasses = tClassString.split(',')

	for tClass in tClasses:
		if(float(tClass) <= 0.1):
			print("ERROR: All traffic classes should have a weight greater than 0.1.")
			sys.exit(1)

	#Check consumer rate
	if args.consumerRate <= 0.0 or args.consumerRate > 100.0:
		print("ERROR: Consumer rate should be in the range (0, 100] checks/second")
		sys.exit(1)

	#Check duration
	if (args.duration < 1):
		print("ERROR: Time should be greater than zero.")
		sys.exit(1)

	if args.acks < 0 or args.acks >= 3:
		print("ERROR: Acks should be 0, 1 or 2 (which represents all)")
		sys.exit(1)

	compressionList = ['gzip', 'snappy', 'lz4']
	if not (args.compression in compressionList) and args.compression != 'None':
		print("ERROR: Compression should be None or one of the following:")
		print(*compressionList, sep = ", ") 
		sys.exit(1)

	if (args.batchSize < 0):
		print("ERROR: Batch Size should not be negative.")
		sys.exit(1)

	if (args.linger < 0):
		print("ERROR: Linger should not be negative.")
		sys.exit(1)
	
	if (args.requestTimeout < 0):
		print("ERROR: Request Timeout should not be negative.")
		sys.exit(1)

	if (args.fetchMinBytes < 0):
		print("ERROR: Fetch Min Bytes should not be negative.")
		sys.exit(1)
	
	if (args.fetchMaxWait < 0):
		print("ERROR: Fetch Max Wait should not be negative.")
		sys.exit(1)

	# Note that the value must be in the allowable range as configured in the broker configuration by group.min.session.timeout.ms and group.max.session.timeout.ms
	if args.sessionTimeout < GROUP_MIN_SESSION_TIMEOUT_MS or args.sessionTimeout > GROUP_MAX_SESSION_TIMEOUT_MS:
		print("ERROR: Session timeout must be in the allowable range as configured in the broker configuration by group.min.session.timeout.ms value of " + str(GROUP_MIN_SESSION_TIMEOUT_MS) + " and group.max.session.timeout.ms value of " + str(GROUP_MAX_SESSION_TIMEOUT_MS))
		sys.exit(1)

	# This value should always be less than the replica.lag.time.max.ms at all times to prevent frequent shrinking of ISR for low throughput topics
	if args.replicaMaxWait >= REPLICA_LAG_TIME_MAX_MS or args.replicaMaxWait < 0:
		print("ERROR: replica.fetch.wait.max.ms must be less than the replica.lag.time.max.ms value of " +  str(REPLICA_LAG_TIME_MAX_MS) + " and greater than zero at all times")
		sys.exit(1)

	if(args.replicaMinBytes < 0):
		print("ERROR: Replica Min Bytes should not be negative.")
		sys.exit(1)

	if(args.topicCheckInterval <= 0):
		print("ERROR: Topic Check Interval should greater than zero.")
		sys.exit(1)

	if(args.topicCheckInterval * args.nTopics) > args.duration:
		print("WARNING: Not all topics will be checked within the given duration of the simulation. Simulation Time:" +  str(args.duration) + " seconds. Time Required to Check All Topics at Least Once: "+  str(args.topicCheckInterval * args.nTopics) + " seconds.")

	if (args.relocate) and (args.disconnectRandom > 0 or args.disconnectZkLeader or args.disconnectTopicLeaders > 0 or args.disconnectHosts or args.disconnectKraftLeader):
		print("ERROR: Relocate can not be combined with Disconnection attributes.")
		sys.exit(1)

	# Check disconnect duration
	if(args.disconnectDuration < 0):
		print("ERROR: Disconnect Duration should not be negative.")
		sys.exit(1)

	if (args.disconnectDuration >= args.duration):
		print("ERROR: Disconnect duration should be less than simulation duration.")
		sys.exit(1)		

	# Check random disconnect
	if (args.disconnectRandom < 0 or args.disconnectRandom > args.nBroker):
		print("ERROR: Disconnect nodes should not be negative but less than the number of broker nodes.")
		sys.exit(1)	

	if (args.disconnectRandom > 0) and (args.disconnectTopicLeaders > 0 or args.disconnectHosts):
		print("ERROR: Disconnect Random can not be combined with Disconnect Topic Leaders or Disconnect Hosts.")
		sys.exit(1)

	if (args.disconnectTopicLeaders < 0):
		print("ERROR: Disconnect Random should not be negative.")
		sys.exit(1)

	if (args.disconnectTopicLeaders > 0) and (args.disconnectRandom > 0 or args.disconnectHosts):
		print("ERROR: Disconnect Topic Leaders can not be combined with Disconnect Random or Disconnect Hosts.")
		sys.exit(1)

	if (args.disconnectHosts) and (args.disconnectTopicLeaders > 0 or args.disconnectRandom > 0):
		print("ERROR: Disconnect Hosts can not be combined with Disconnect Random or Disconnect Topic Leaders.")
		sys.exit(1)

	if (args.nZk > 0 or args.disconnectZkLeader) and (args.kraft):
		print("ERROR: Zookeeper attributes can not be used with a KRaft deployment.")
		sys.exit(1)		

	if (not args.kraft) and args.disconnectKraftLeader:
		print("ERROR: KRaft attribute must be specified to Disconnect KRaft Leader.")
		sys.exit(1)

	if(args.consumerSetupSleep < 0):
		print("ERROR: Consumer Setup Sleep should not be negative.")
		sys.exit(1)

	if(args.offsetsTopicReplication < 0):
		print("ERROR: Offsets Topic Replication should not be negative.")
		sys.exit(1)

	if (not args.kraft) and (args.ssl or args.auth):
		print("ERROR: KRaft attribute must be specified to enable SSL or Authentication.")
		sys.exit(1)

	if (args.java) and (args.ssl):
		print("ERROR: SSL not supported by Java Consumer.")
		sys.exit(1)

	if (not args.java) and (args.localReplica):
		print("ERROR: Java attribute must be specified to run with Local Replica.")
		sys.exit(1)

if __name__ == '__main__': 

	parser = argparse.ArgumentParser(description='Emulate data sync in mission critical networks.')
	#parser.add_argument('--topo', dest='topo', type=str, default='tests/simple.graphml', help='Network topology')
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

	#parser.add_argument('--create-plots', dest='createPlots', action='store_true')

	parser.add_argument('--message-file', dest='messageFilePath', type=str, default='None', help='Path to a file containing the message to be sent by producers')
	parser.add_argument('--topic-check', dest='topicCheckInterval', type=float, default=1.0, help='Minimum amount of time (in seconds) the consumer will wait between checking topics')

	parser.add_argument('--single-consumer', dest='singleConsumer', action='store_true', help='Use a single, always connected consumer (per node) for the entire simulation')
	parser.add_argument('--relocate', dest='relocate', action='store_true', help='Relocate a random node during the simulation')
	parser.add_argument('--dc-duration', dest='disconnectDuration', type=int, default=60, help='Duration of the disconnection (in seconds)')
	parser.add_argument('--dc-random', dest='disconnectRandom', type=int, default=0, help='Disconnect a number of random hosts')
	parser.add_argument('--dc-zk-leader', dest='disconnectZkLeader', action='store_true', help='Disconnect the zookeeper leader')
	parser.add_argument('--dc-topic-leaders', dest='disconnectTopicLeaders', type=int, default=0, help='Disconnect a number of topic leader nodes')
	parser.add_argument('--dc-hosts', dest='disconnectHosts', type=str, help='Disconnect a list of hosts (h1,h2..hn)')
	parser.add_argument('--dc-kraft-leader', dest='disconnectKraftLeader', action='store_true', help='Disconnect the kraft leader')	
	parser.add_argument('--latency-after-setup', dest='latencyAfterSetup', action='store_true', help='Lower the network latency before setting up Kafka, then set it back once Kafka is set up.')	
	parser.add_argument('--consumer-setup-sleep', dest='consumerSetupSleep', type=int, default=120, help='Duration to sleep between setting up consumers and producers (in seconds).')

	parser.add_argument('--capture-all', dest='captureAll', action='store_true', help='Capture the traffic of all the hosts')
	parser.add_argument('--offsets-replication', dest='offsetsTopicReplication', type=int, default=1, help='The replication factor for the offsets topic')

	parser.add_argument('--kraft', dest='kraft', action='store_true', help='Run using KRaft consensus protocol instead of Zookeeper')	
	parser.add_argument('--ssl', dest='ssl', action='store_true', help='Enable encryption using SSL')
	parser.add_argument('--auth', dest='auth', action='store_true', help='Enable authentication ')

	parser.add_argument('--java', dest='java', action='store_true', help='Run with java consumers')
	parser.add_argument('--local-replica', dest='localReplica', action='store_true', help='Run with local replica fetch')



	args = parser.parse_args()

	# TODO: TEMP - hardcode for testing
	# args.topo = 'tests/input/star-no-latency/star-ten-node-topo.graphml'
	# args.nBroker = 10
	# args.nZk = 10
	# args.nTopics = 2
	# args.replication = 10
	# args.mSizeString = 'fixed,1000'
	# args.mRate = 30.0
	# args.consumerRate = 0.5
	# args.messageFilePath = 'message-data/xml/Cars103.xml'
	# args.topicCheckInterval = 0.1	
	# args.duration = 300
	# args.compression = 'gzip'
	# args.replicaMaxWait = 5000
	# args.replicaMinBytes = 200000
	# args.disconnectDuration = 60
	# args.disconnectRandom = 0
	# args.disconnectZkLeader = False
	# args.disconnectKraftLeader = False
	# args.disconnectHosts = None
	# args.disconnectTopicLeaders = 0
	# args.relocate = False
	# args.singleConsumer = False	
	# args.kraft = True
	# args.ssl = False
	# args.auth = False
	# args.java = True
	# args.localReplica = True
	# END

	print(args)	
	validateInput(args)
	kraft = args.kraft
	
	#Clean up mininet state
	cleanProcess = subprocess.Popen("sudo mn -c", shell=True)
	time.sleep(2)

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

	if kraft:
		brokerPlace = emuKafkaKraft.placeKafkaBrokers(net, args.nBroker, args.nZk)
	else:	
		brokerPlace, zkPlace = emuKafka.placeKafkaBrokers(net, args.nBroker, args.nZk)

	#TODO: remove debug code
	killSubprocs(brokerPlace)
	emuLogs.cleanLogs()

	if kraft:		
		emuKafkaKraft.cleanKafkaState(brokerPlace)		
	else:		
		emuKafka.cleanKafkaState(brokerPlace)
		emuZk.cleanZkState(zkPlace)
	logDir = emuLogs.configureLogDir(args.nBroker, args.mSizeString, args.mRate, args.nTopics, args.replication)
	
	if kraft:		
		emuKafkaKraft.configureKafkaCluster(brokerPlace, args)
	else:		
		emuZk.configureZkCluster(zkPlace)
		emuKafka.configureKafkaCluster(brokerPlace, zkPlace, args)
	
	# Log the test args
	logging.info("Test args:\n %s", args)

	#Start network
	print("Starting Network")
	net.start()
	for switch in net.switches:
		net.get(switch.name).start([])

	logging.info('Network started at ' + str(datetime.now()))
	#emuNetwork.configureNetwork(args.topo)
	#time.sleep(5)

	# Set network delay to a low value to allow Kafka to set up properly
	if args.latencyAfterSetup:
		if kraft:
			emuLoadKraft.setNetworkDelay(net, '1ms')
		else:	
			emuLoad.setNetworkDelay(net, '1ms')

	print("Testing network connectivity")
	net.pingAll()
	print("Finished network connectivity test")
		
	#Start monitoring tasks
	popens[pID] = subprocess.Popen("sudo python3 bandwidth-monitor.py "+str(args.nBroker)+" " +args.mSizeString+" "+str(args.mRate) +" " +str(args.nTopics) +" "+ str(args.replication) + " "+ str(args.nZk) +" &", shell=True)
	pID += 1

	if kraft:		
		emuKafkaKraft.runKafka(net, brokerPlace, logDir, args)
		emuLoadKraft.runLoad(net, args.nTopics, args.replication, args.mSizeString, args.mRate, args.tClassString, args.consumerRate, args.duration, logDir, args)
	else:
		emuZk.runZk(net, zkPlace, logDir)
		emuKafka.runKafka(net, brokerPlace, logDir)					
		emuLoad.runLoad(net, args.nTopics, args.replication, args.mSizeString, args.mRate, args.tClassString, args.consumerRate, args.duration, logDir, args)

	print("Simulation complete\r")
	logging.info('Simulation complete at ' + str(datetime.now()))


	# to kill all the running subprocesses
	killSubprocs(brokerPlace)


	net.stop()
	logging.info('Network stopped at ' + str(datetime.now()))

	#Need to clean both kafka and zookeeper state before a new simulation
	if kraft:	
		emuKafkaKraft.cleanKafkaState(brokerPlace)				
	else:
		emuKafka.cleanKafkaState(brokerPlace)
		emuZk.cleanZkState(zkPlace)





