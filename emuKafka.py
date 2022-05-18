#!/usr/bin/python3

from mininet.net import Mininet

import os
import sys
import subprocess
import time
import multiprocessing

import emuLogs


def configureKafkaCluster(brokerPlace, zkPlace, args):
    print("Configure kafka cluster")

    propertyFile = open("kafka/config/server.properties", "r")
    serverProperties = propertyFile.read()

    for bID in brokerPlace:
        os.system("sudo mkdir kafka/kafka" + str(bID) + "/")

        bProperties = serverProperties
        bProperties = bProperties.replace("broker.id=0", "broker.id="+str(bID))
        bProperties = bProperties.replace(
            "#advertised.listeners=PLAINTEXT://your.host.name:9092",
            "advertised.listeners=PLAINTEXT://10.0.0." + str(bID) + ":9092")
        bProperties = bProperties.replace("log.dirs=/tmp/kafka-logs",
                                          "log.dirs=./kafka/kafka" + str(bID))

        bProperties = bProperties.replace(
            "#replica.fetch.wait.max.ms=500", "replica.fetch.wait.max.ms="+str(args.replicaMaxWait))
        bProperties = bProperties.replace(
            "#replica.fetch.min.bytes=1", "replica.fetch.min.bytes="+str(args.replicaMinBytes))

        # Specify zookeeper addresses to connect
        zkAddresses = ""
        zkPort = 2181

        for i in range(len(zkPlace)-1):
            zkAddresses += "localhost:"+str(zkPort)+","
            zkPort += 1

        zkAddresses += "localhost:"+str(zkPort)

        bProperties = bProperties.replace(
            "zookeeper.connect=localhost:2181",
            "zookeeper.connect="+zkAddresses)

        # bProperties = bProperties.replace(
        #	"zookeeper.connection.timeout.ms=18000",
        #	"zookeeper.connection.timeout.ms=30000")

        bFile = open("kafka/config/server" + str(bID) + ".properties", "w")
        bFile.write(bProperties)
        bFile.close()

    propertyFile.close()


def placeKafkaBrokers(net, nBroker, nZk):

    brokerPlace = []
    zkPlace = []

    if nBroker < 0 or nBroker > len(net.hosts):
        print("ERROR: Cannot support specified number of broker instances.")
        sys.exit(1)
    elif nZk < 0 or nZk > len(net.hosts):
        print("ERROR: Cannot support specified number of Zookeeper instances.")
        sys.exit(1)

    if nBroker == len(net.hosts):
        for i in range(nBroker):
            brokerPlace.append(i+1)
    else:
        print("ERROR: Support for broker placement will be added in the future. Please consider setting the number of brokers to the number of end hosts in your network.")
        sys.exit(1)

    if nZk == len(net.hosts):
        for i in range(nZk):
            zkPlace.append(i+1)
    else:
        print("ERROR: Support for zookeeper placement will be added in the future. Please consider setting the number of zookeeper instances to the number of end hosts in your network.")
        sys.exit(1)

    return brokerPlace, zkPlace


def runKafka(net, brokerPlace, brokerWaitTime=200):

    netNodes = {}

    for node in net.hosts:
        netNodes[node.name] = node

    startTime = time.time()
    popens = {}
    for bNode in brokerPlace:
        bID = "h"+str(bNode)

        startingHost = netNodes[bID]

        print("Creating Kafka broker at node "+str(bNode))

        popens[startingHost] = startingHost.popen(
            "kafka/bin/kafka-server-start.sh kafka/config/server"+str(bNode)+".properties &", shell=True)

        time.sleep(1)

    process = multiprocessing.Process(target=emuLogs.logMininetProcesses, args=(popens,emuLogs.BROKER_LOG_FILE))
    process.start()

    brokerWait = True
    totalTime = 0
    for bNode in brokerPlace:
        while brokerWait:
            print("Testing Connection to Broker " + str(bNode) + "...")
            out, err, exitCode = startingHost.pexec(
                "nc -z -v 10.0.0." + str(bNode) + " 9092")
            stopTime = time.time()
            totalTime = stopTime - startTime
            if(exitCode == 0):
                brokerWait = False
            # elif(totalTime > brokerWaitTime):
            #    print("ERROR: Timed out waiting for Kafka brokers to start")
            #    sys.exit(1)
            else:
                print("Waiting for Broker " + str(bNode) + " to Start...")
                time.sleep(10)
        brokerWait = True
    print("Successfully Created Kafka Brokers in " + str(totalTime) + " seconds")

def cleanKafkaState(brokerPlace):
    for bID in brokerPlace:
        os.system("sudo rm -rf kafka/kafka" + str(bID) + "/")
        os.system("sudo rm -f kafka/config/server" + str(bID) + ".properties")
