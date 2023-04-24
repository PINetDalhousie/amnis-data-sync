# amnis-data-sync

Tool for emulating data synchronization in mission critical networks.

The tool was tested on Ubuntu 18.04.1 and is based on Python 3.6 with Kafka 2.13-2.8.0 (ZooKeeper) or Kafka 3.2.0 (KRaft).

## Getting started

1. Clone the repository, then enter into it.

```git clone https://github.com/PINetDalhousie/amnis-data-sync```

```cd amnis-data-sync```

2. Install dependencies. Our tool depends on the following software:

  - pip3
  - Java 11
  - maven
  - Xterm
  - Mininet 2.3.0.dev6
  - Networkx 3.0
  - Kafka-python 2.0.2
  - Matplotlib 3.5.1
  - python-snappy 0.6.1
  - lz4 4.0.0
  - Seaborn 0.12.2
  - Pandas 1.5.2

  Most dependencies can be installed using `apt install` & `pip3 install`:
  
  ```bash
  $ sudo apt install python3-pip mininet default-jdk xterm netcat maven
  
  $ python3 -m pip install --upgrade pip
  
  $ sudo pip3 install mininet==2.3.0.dev6 networkx==3.0 kafka-python==2.0.2 matplotlib==3.5.1 python-snappy==0.6.1 lz4==4.0.0 seaborn==0.12.2 pandas==1.5.2
  ```
  
  3. Build the custom Java Consumer whih can be used in conjunction with a KRaft Kafka deployment:

  ```cd java```

  ```./build.sh```

  4. You are ready to go! Should be able to get help using:

  ```sudo python3 main.py -h```

  The following table provides an overview of the arguments available for configuring the simulation:
  |     Simulation Argument     |     Description                                                                                                                              |     Acceptable Value(s)                                                                                                                                                                                            |     Usage Restrictions                                                                                           |
|-----------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|
|     topo                    |     Required. Network topology graphml file                                                                                                  |     String representing the path to any valid graphml file. See tests/input/   for examples                                                                                                                        |     N/A                                                                                                          |
|     nbroker                 |     Number of brokers.                                                                                                                       |     Integer value equal to the number of end hosts specified in the   graphml topology file. Default=0                                                                                                             |     N/A                                                                                                          |
|     nzk                     |     Number of Zookeeper instances.                                                                                                           |     Integer value equal to the number of end hosts specified in the   graphml topology file. Default=0                                                                                                             |     Kafka with Zookeeper only, not supported with KRaft                                                          |
|     ntopics                 |     Number of topics                                                                                                                         |     Integer value >0. Default=1                                                                                                                                                                                       |     N/A                                                                                                          |
|     replication             |     Replication factor                                                                                                                       |     Integer value >0. Default=1                                                                                                                                                                                       |     N/A                                                                                                          |
|     message-size            |     Message size distribution (fixed, gaussian).                                                                                             |     String value. Need to specify a message size (>=1) for fixed size   messages or specify a mean (>=1) and standard deviation (>0) for   gaussian-sized messages. Default=‘fixed,10’                             |     Used if message-file attribute is not specified                                                              |
|     message-rate            |     Message rate in msgs/second                                                                                                              |     Numerical value <=100. Default=1.0                                                                                                                                                                             |     N/A                                                                                                          |
|     traffic-classes         |     Number of traffic classes                                                                                                                |     String value. All traffic classes should have a weight greater than   0.1. Default=’1’                                                                                                                         |     N/A                                                                                                          |
|     consumer-rate           |     Rate consumers check for new messages in checks/second                                                                                   |     Float value in the range (0, 100]. Default=0.5                                                                                                                                                                 |     N/A                                                                                                          |
|     time                    |     Duration of the simulation (in seconds)                                                                                                  |     Integer value > 0. Default=10                                                                                                                                                                                  |     N/A                                                                                                          |
|     acks                    |     Controls how many replicas must receive the record before producer   considers successful write.                                         |     Acceptable integer values include 0, 1, 2 (2=all). Default=1                                                                                                                                                   |     N/A                                                                                                          |
|     compression             |     Compression algorithm used to compress data sent to brokers.                                                                             |     Acceptable string values include gzip, snappy, lz4, None. Default=‘None’                                                                                                                                       |     N/A                                                                                                          |
|     batch-size              |     When multiple records are sent to the same partition, the producer   will batch them together (bytes)                                    |     Integer value >=0. Default=16384                                                                                                                                                                                   |     N/A                                                                                                          |
|     linger                  |     Controls the amount of time (in ms) to wait for additional messages   before sending the current batch                                   |     Integer value >=0. Default=0                                                                                                                                                                                       |     N/A                                                                                                          |
|     request-timeout         |     Controls how long producer waits for a reply from server when sending   data                                                             |     Integer value >=0. Default=30000                                                                                                                                                                                   |     N/A                                                                                                          |
|     fetch-min-bytes         |     Minimum amount of data consumer needs to receive from the broker when   fetching records (bytes)                                         |     Integer value >=0. Default=1                                                                                                                                                                                       |     N/A                                                                                                          |
|     fetch-max-wait          |     How long the broker will wait (in ms) before sending data to consumer                                                                    |     Integer value >=0. Default=500                                                                                                                                                                                     |     N/A                                                                                                          |
|     session-timeout         |     Time (in ms) a consumer can be out of contact with brokers while   still considered alive.                                               |     Integer value  in the allowable   range as configured in the broker configuration by   group.min.session.timeout.ms (default  6000ms) and group.max.session.timeout.ms (default   1800000ms). Default=10000    |     N/A                                                                                                          |
|     replica-max-wait        |     Max wait time for each fetcher request issued by follower replicas     |     Integer value >=0 but less than the replica.lag.time.max.ms (default 30000ms). Default=500                                                                                                                                                                                     |     N/A                                                                                                          |
|     replica-min-bytes       |     Minimum bytes expected for each fetch response                                                                                           |     Integer value >=0. Default=1                                                                                                                                                                                       |     N/A                                                                                                          |
|     message-file            |     Path to a file containing the message to be sent by producers                                                                            |     String representing the path to any valid data file. Default=‘None’                                                                                                                                            |     N/A                                                                                                          |
|     topic-check             |     Minimum amount of time (in seconds) the consumer will wait between   checking topics                                                     |     Numerical value > 0. Default=1                                                                                                                                                                                 |     Not supported by Java Consumer                                                                               |
|     single-consumer         |     Use a single, always connected consumer (per node) for the entire   simulation                                                           |     N/A. Specify attribute to enable                                                                                                                                                                               |     N/A                                                                                                          |
|     relocate                |     Relocate a random node during the simulation                                                                                             |     N/A. Specify attribute to enable                                                                                                                                                                               |     Cannot be used in combination with dc-random, dc-zk-leader,   dc-topic-leader, dc-hosts, dc-kraft-leader     |
|     dc-duration             |     Duration of the disconnection (in seconds), should be less than   simulation duration time                                               |     Integer value >= 0. Default = 60                                                                                                                                                                               |     Used with the following attributes: dc-random, dc-zk-leader,   dc-topic-leader, dc-hosts, dc-kraft-leader    |
|     dc-random               |     Disconnect a number of random hosts                                                                                                      |     Integer value >= 0 but less than the number of broker nodes.   Default = 0                                                                                                                                     |     Cannot be used with the following attributes: dc-topic-leader,   dc-hosts, relocate                          |
|     dc-zk-leader            |     Disconnect the zookeeper leader                                                                                                          |     N/A. Specify attribute to enable                                                                                                                                                                               |     Kafka with Zookeeper only not supported with Kraft. Cannot be used   with relocate attribute                 |
|     dc-topic-leaders        |     Disconnect a number of topic leader nodes                                                                                                |     Integer value >= 0. Default = 0                                                                                                                                                                                |     Cannot be used with the following attributes: dc-random, dc-hosts,   relocate                                |
|     dc-hosts                |     Disconnect a list of hosts (h1,h2..hn)                                                                                                   |     String value. No default. Example: h1,h2,h5                                                                                                                                                                    |     Cannot be used with the following attributes: dc-topic-leader, dc-random,   relocate                         |
|     dc-kraft-leader         |     Disconnect the kraft leader                                                                                                              |     N/A. Specify attribute to enable                                                                                                                                                                               |     KRaft only, not supported for Kafka with Zookeeper . Cannot be used   with relocate attribute                |
|     latency-after-setup     |     Lower the network latency before setting up Kafka, then set it back   once Kafka is set up                                               |     N/A. Specify attribute to enable                                                                                                                                                                               |     N/A                                                                                                          |
|     consumer-setup-sleep    |     Duration to sleep between setting up consumers and producers (in   seconds)                                                              |     Integer value >= 0. Default = 120                                                                                                                                                                              |     N/A                                                                                                          |
|     capture-all             |     Capture the traffic of all the hosts                                                                                                     |     N/A. Specify attribute to enable                                                                                                                                                                               |     N/A                                                                                                          |
|     offsets-replication     |     The replication factor for the offsets topic                                                                                             |     Integer value >=0. Default = 1                                                                                                                                                                                 |     Kafka with Zookeeper only, no support for KRaft                                                              |
|     kraft                   |     Run using KRaft consensus protocol instead of Zookeeper                                                                                  |     N/A. Specify attribute to enable                                                                                                                                                                               |     Use KRaft instead of default Kafka with Zookeeper                                                            |
|     ssl                     |     Enable encryption using SSL                                                                                                              |     N/A. Specify attribute to enable                                                                                                                                                                               |     KRaft only, no support for Kafka with Zookeeper. Not supported by   Java Consumer                            |
|     auth                    |     Enable authentication                                                                                                                    |     N/A. Specify attribute to enable                                                                                                                                                                               |     KRaft only, no support for  Kafka   with Zookeeper                                                           |
|     java                    |     Run with java consumers                                                                                                                  |     N/A. Specify attribute to enable                                                                                                                                                                               |     Use Java based Consumer instead of default Python based consumer                                             |
|     local-replica           |     Run with local replica fetch                                                                                                             |     N/A. Specify attribute to enable                                                                                                                                                                               |     Java Consumer only, not supported by default Python based Consumer                                           |
  
  ## Quick start

  Run a quick test with the run.sh script. This will run the simulation with standard arguments, then run the plotting scripts. Pass in a test name as the only argument, which will be the resulting directory name under ./logs/kafka/ where the results and plots from the simulation will be located.

  ```sudo ./run.sh quick-test```

  ## Sample simulation commands
  
  1) Emulate and sync data in a small network.
  
  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2```
  
  2) Create custom load. This example assumes a Gaussian distribution for the message sizes and two traffic classes (one sending messages at half the specified rate, i.e., 0.4, and another at twice).

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --message-size gaussian,2,5 --message-rate 0.4 --traffic-classes 0.5,2```
  
  3) Create custom load with fixed message sizes.

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --message-size fixed,50 --message-rate 0.4 --traffic-classes 0.5,2```
  
  4) Set the number of topics

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --ntopics 4```
  
  5) Set a replication factor for each topic (e.g., store two copies of each message)
  
  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --ntopics 4 --replication 2```
  
  6) Set the consumer rate (e.g., check new messages every 2 seconds)

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --consumer-rate 0.5```
  
  7) Set a duration for the simulation. OBS.: this is the time the workload will run, not the total simulation time

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --time 60```
  
  8) Include a disconnect of 3 random hosts for a specified duration (in seconds) during the simulation (default 60s).

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --time 60 --dc-random 3 --dc-duration 20```

  9) Include a disconnect of specific hosts for a specified duration (in seconds) during the simulation.

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --time 60 --dc-hosts h1,h2,h5```

  10) Disconnect the Zookeeper contoller (leader) for a specified duration (in seconds) during the simulation. May or not disconnect an extra node when used in combination with ```-dc-hosts```.

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --time 60 --dc-zk-leader```

  11) Disconnect a specific number of nodes that are topic leaders for a specified duration (in seconds) during the simulation. Do not disconnect the ZK leader unless ```--dc-zk-leader``` is specified. 

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --time 60 --dc-topic-leaders 3```
  
  12) Run the simulation using the single consumer behaviour - one consumer per host that subscribes to all topics and is always connected.

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --time 60 --single-consumer```
  
  13) Set the network latency between switches to apply after kafka has set up. This is to used to let kafka get set up on a low latency, then run the simulation using high latency.

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --time 60 --latency-after-setup```
  
  14) Capture the network traffic of all nodes.

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --time 60 --capture-all```
  
  15) Set the offsets topic replication factor.

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --time 60 --offsets-replication 3```
  
  16) Run with Kraft consensus protocol instead of Zookeeper.

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --time 60 --kraft```

  17) Run with SSL authentication and encryption
  Note if this is your first time running you need to run the following first:  

  ```cd certs && ./create-certs.sh```
  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --time 60 --kraft --ssl```

  18) Run with Java Consumers instead of Python.

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --time 60 --kraft --java```

  19) Run with local replica fetch.

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --time 60 --kraft --java --local-replica```

  ## Sample plotting script commands

  Before processing, navigate to the plotting script directory:

  ```cd ./plot-scripts```

  1) Bandwidth plot script:

  ```sudo python3 bandwidthPlotScript.py --number-of-switches 10 --port-type access-port --message-size fixed,10 --message-rate 30.0 --ntopics 2 --replication 10 --log-dir ../logs/kafka/quick-test --switch-ports S1-P1,S2-P1,S3-P1,S4-P1,S5-P1,S6-P1,S7-P1,S8-P1,S9-P1,S10-P1```

  2) Latency plot script:

  ```sudo python3 modifiedLatencyPlotScript.py --number-of-switches 10 --log-dir ../logs/kafka/quick-test```

  3) Running heat map plot script:
  
  ```sudo python3 messageHeatMap.py --log-dir .../logs/kafka/quick-test --prod 10 --cons 10 --topic 2```
  ```sudo mv msg-delivery/ .../logs/kafka/quick-test/```
