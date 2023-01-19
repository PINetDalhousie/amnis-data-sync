RESULT_DIR=logs/kafka/$1/
SWITCHES=5
DIR="nodes:5_mSize:fixed,10_mRate:30.0_topics:5_replication:5"

cp ./plot-scripts/bandwidthPlotScript.py ./
cp ./plot-scripts/modifiedLatencyPlotScript.py ./

echo "Result directory is $RESULT_DIR"

echo "********Running sim"
sudo python3 main.py tests/input/star/star-five-node-topo.graphml --nbroker $SWITCHES --nzk $SWITCHES --message-rate 30.0 --replication $SWITCHES --message-file message-data/xml/Cars103.xml --time 1200 --replica-min-bytes 200000 --replica-max-wait 5000 --ntopics $SWITCHES --topic-check 0.1 --consumer-rate 0.5 --compression gzip --single-consumer --batch-size 16384 --linger 5000 --dc-topic-leaders 1 --dc-duration 600 --capture-all

echo "********Renaming logs folder"
sudo mv logs/kafka/$DIR $RESULT_DIR

echo "********Running bandwidth plot script"
sudo python3 bandwidthPlotScript.py --number-of-switches $SWITCHES --port-type access-port --message-size fixed,10 --message-rate 30.0 --ntopics $SWITCHES --replication $SWITCHES --log-dir $RESULT_DIR --switch-ports S1-P1,S2-P1,S3-P1,S4-P1,S5-P1,S6-P1,S7-P1,S8-P1,S9-P1,S10-P1

echo "********Running latency plot script"
sudo python3 modifiedLatencyPlotScript.py --number-of-switches $SWITCHES --log-dir $RESULT_DIR

echo "********Moving pcap files"
sudo chmod ugo+rwx /tmp/*pcap 
PCAPS=logs/kafka/$1/pcaps/
sudo mkdir $PCAPS
sudo mv /tmp/*pcap $PCAPS

rm ./bandwidthPlotScript.py
rm ./modifiedLatencyPlotScript.py