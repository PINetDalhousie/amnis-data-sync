# command to run this script: sh sparkScript.sh <interfaceName> <delay> <inputTopic> <outputTopic>
#!/bin/bash

# cleaning previous logs
sudo rm -rf logs/
sudo mkdir logs/

# To delete existing rule on that interface
sudo tc qdisc del dev $1 root 

# set bandwidth and delay on that interface
sudo tc qdisc add dev $1 root netem delay $2ms rate 1mbit

# To check current status of the interface
tc qdisc show  dev $1

# run spark SS applicaiton
sudo /home/grad/ifath/.local/lib/python3.8/site-packages/pyspark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 wordCount.py 10.50.0.5:9092 $3 $4 