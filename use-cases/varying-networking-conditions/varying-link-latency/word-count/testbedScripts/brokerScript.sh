# command to run this script: sh brokerScript.sh <interfaceName> <delay> <inputTopic> <outputTopic>
## interface for 10.50.0.0/24 : enp101s0f0
# interface for 10.50.1.0/24 : enp101s0f1

#!/bin/bash

# # to discard delay in ingress
# # sudo tc qdisc del dev $1 handle ffff: ingress
# sudo modprobe -r ifb

# # inress delay add to the interface
# sudo modprobe ifb
# sudo ip link set dev ifb0 up
# sudo tc qdisc add dev $1 ingress
# sudo tc filter add dev $1 parent ffff: protocol ip u32 match u32 0 0 flowid 1:1 action mirred egress redirect dev ifb0
# sudo tc qdisc add dev ifb0 root netem delay $2ms
# sudo tc qdisc show dev ifb0
 
# to kill running kafka, zookeeper instances
# sudo /users/grad/ifath/amnis-data-sync/kafka/bin/kafka-server-stop.sh
# sudo /users/grad/ifath/amnis-data-sync/kafka/bin/zookeeper-server-stop.sh
pkill -9 -f server1.properties
pkill -9 -f zookeeper

sleep 5

jps

# cleaning previous logs
rm -rf /users/grad/ifath/amnis-data-sync/kafka/kafka1/ /users/grad/ifath/amnis-data-sync/kafka/zookeeper1/

# To delete existing rule on that interface
tc qdisc del dev enp101s0f0 root 

# set bandwidth and delay on that interface
tc qdisc add dev $1 root netem delay $2ms rate 1mbit

# To check current status of the interface
tc qdisc show  dev enp101s0f0

# starting zookeeper instance
/users/grad/ifath/amnis-data-sync/kafka/bin/zookeeper-server-start.sh /users/grad/ifath/amnis-data-sync/kafka/config/zookeeperTofino1.properties &
#/users/grad/ifath/amnis-data-sync/kafka/bin/zookeeper-server-start.sh /users/grad/ifath/amnis-data-sync/kafka/config/zookeeperTofino2.properties &

sleep 5

# Start the Kafka broker service
~/amnis-data-sync/kafka/bin/kafka-server-start.sh ~/amnis-data-sync/kafka/config/serverTofino1.properties &
# ~/amnis-data-sync/kafka/bin/kafka-server-start.sh ~/amnis-data-sync/kafka/config/serverTofino2.properties &

sleep 5

# Create input topic
~/amnis-data-sync/kafka/bin/kafka-topics.sh --create --topic $3 --bootstrap-server 10.50.0.5:9092

# Create output topic
~/amnis-data-sync/kafka/bin/kafka-topics.sh --create --topic $4 --bootstrap-server 10.50.0.5:9092

# list all topics
~/amnis-data-sync/kafka/bin/kafka-topics.sh --list --bootstrap-server 10.50.0.5:9092

# # To delete existing rule on that interface
# sudo tc qdisc del dev $1 root
# # To check current status of the interface
# tc qdisc show  dev $1

# # to discard delay in ingress
# sudo tc qdisc del dev $1 handle ffff: ingress
# sudo modprobe -r ifb

exit