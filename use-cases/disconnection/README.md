## Input details
1. datadir: contains file(s) to produce
2. topicConfiguration.txt : associated topic name(s), broker ID, number of partition(s) in each line for each toppic
3. input.graphml:
   - contains topology description
     - node details (switch, host)
     - edge details (bandwidth, latency, source port, destination port)
   - contains component(s) configurations 
     - topicConfig: path to the topic configuration file
     - zookeeper: 1 = hostnode contains a zookeeper instance
     - broker: 1 = hostnode contains a zookeeper instance
     - producerType: producer type can be SFST/MFMT/ELTT; SFST denotes from Single File to Single Topic. ELTT is defined when Each line To Topic i.e. each line of the file is produced to the topic as a single message
     - producerConfig: for SFST/ELTT, one pair of filePath, topicName, number of files.
     - consumerConfig: a pair containing information of topic name for consumption and number of consumer instances in this node

## Running
   
 ```sudo python3 main.py use-cases/disconnection/input.graphml --nzk 1 --nbroker 1 --only-kafka 1 --time 50```
