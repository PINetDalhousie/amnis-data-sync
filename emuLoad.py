#!/usr/bin/python3

from mininet.net import Mininet

import time

def runLoad(net):

	print("Start workload")

	h1=net.hosts[0]
	h2=net.hosts[1]

	h1.cmd("kafka/bin/kafka-topics.sh --create --bootstrap-server 10.0.0.1:9092 --replication-factor 1 --partitions 1 --topic example-topic", shell=True)
	time.sleep(10)
	print("Created a topic")

	out = h1.cmd("kafka/bin/kafka-topics.sh --list --bootstrap-server 10.0.0.1:9092", shell=True)
	print("#######################")
	print(out)

	h1.cmd("cat tests/produced-data.txt | kafka/bin/kafka-console-producer.sh --broker-list 10.0.0.1:9092 --topic example-topic", shell=True)
	time.sleep(5)
	print("Data produced")

	h2.cmd("python3 kafka-python-consumer.py > consumed-data.txt", shell=True)
	print("Data consumed")

	print("Workload finished")











