# command to run this script: sh consumerScript.sh <interfaceName> <delay> <consumeTopic> 

#!/bin/bash

# remove previous logs
sudo rm -r logs/
sudo mkdir logs/

# To delete existing rule on that interface
sudo tc qdisc del dev $1 root 

# set bandwidth and delay on that interface
sudo tc qdisc add dev $1 root netem delay $2ms rate 1mbit 

# To check current status of the interface
tc qdisc show  dev $1

# run consumer command
# sudo python3 consumer.py <topicName> <brokerIP:port>
sudo python3 consumer.py $3 10.50.0.5:9092

