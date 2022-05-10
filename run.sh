NEW_DIR=logs/kafka/$1/

DIR="nodes:10_mSize:fixed,10_mRate:30.0_topics:10_replication:10"

SWITCHES=10

echo "New dir is"
echo $NEW_DIR

echo "********Running sim"
sudo python3 main.py tests/input/star/star-ten-node-topo.graphml --nbroker $SWITCHES --nzk $SWITCHES --message-rate 30.0 --replication $SWITCHES --message-file message-data/xml/Cars103.xml --time 300 --replica-min-bytes 200000 --replica-max-wait 5000 --ntopics $SWITCHES --topic-check 0.1 --consumer-rate 0.5 --compression gzip --latency-after-setup --single-consumer


echo "********Renaming"
sudo mv logs/kafka/$DIR $NEW_DIR


echo "********Running bandwidth"
#sudo python3 bandwidthPlotScript.py --number-of-switches $SWITCHES --port-type access-port --message-size fixed,10 --message-rate 30.0 --ntopics $SWITCHES --replication $SWITCHES --log-dir $NEW_DIR --switch-ports S1-P1,S2-P1,S3-P1,S4-P1,S5-P1,S6-P1,S7-P1,S8-P1,S9-P1,S10-P1,S11-P1,S12-P1,S13-P1,S14-P1,S15-P1,S16-P1,S17-P1,S18-P1,S19-P1,S20-P1
sudo python3 bandwidthPlotScript.py --number-of-switches $SWITCHES --port-type access-port --message-size fixed,10 --message-rate 30.0 --ntopics $SWITCHES --replication $SWITCHES --log-dir $NEW_DIR --switch-ports S1-P1,S2-P1,S3-P1,S4-P1,S5-P1,S6-P1,S7-P1,S8-P1,S9-P1,S10-P1


echo "********Running latency"
sudo python3 modifiedLatencyPlotScript.py --number-of-switches $SWITCHES --log-dir $NEW_DIR
