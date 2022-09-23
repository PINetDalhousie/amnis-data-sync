# command to run this script: sh producerScript.sh <interfaceName> <delay> <inputTopic>
# interface for 10.50.0.0/24 : enp1s0np0
# interface for 10.50.1.0/24 : enp1s0np1

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

# remove previous logs
sudo rm -r logs/
sudo mkdir logs/

# To delete existing rule on that interface's egress
sudo tc qdisc del dev $1 root 

# set bandwidth and delay on that interface's egrees
sudo tc qdisc add dev $1 root netem delay $2ms rate 1mbit 

# To check current status of the interface
tc qdisc show  dev $1

# run producer command
# sudo python3 producer.py <inputFilePath> <topicName> <brokerIP:port>
sudo python3 producer.py /users/grad/ifath/amnis-data-sync/dataDir/input.txt $3 10.50.0.5:9092 &

sleep 5

# # To delete existing rule on that interface
# sudo tc qdisc del dev $1 root
# # To check current status of the interface
# tc qdisc show  dev $1

# # to discard delay in ingress
# sudo tc qdisc del dev $1 handle ffff: ingress
# sudo modprobe -r ifb



