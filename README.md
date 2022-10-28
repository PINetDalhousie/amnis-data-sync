# amnis-data-sync

Tool for emulating data synchronization in mission critical networks.

The tool was tested on Ubuntu 18.04.1 and is based on Python 3.6 and Kafka 2.13-2.8.0.

## Getting started

1. Clone the repository, then enter into it.

```git clone https://github.com/PINetDalhousie/amnis-data-sync```

```cd amnis-data-sync```

2. Install dependencies. Our tool depends on the following software:

  - pip3
  - Mininet 2.3.0
  - Networkx 2.5.1
  - Java 11
  - Xterm
  - Kafka-python 2.0.2
  - Matplotlib 3.3.4
  - Seaborn 0.11.2
  - Pandas 1.5.0

  Most dependencies can be installed using `apt install`:
  
  ```bash
  $ sudo apt install python3-pip mininet default-jdk xterm netcat
  
  $ python3 -m pip install --upgrade pip
  
  $ sudo pip3 install mininet networkx kafka-python matplotlib python-snappy lz4 seaborn pandas
  ```
  3. You are ready to go! Should be able to get help using:

  ```sudo python3 main.py -h```
  
  ## Quick start

  Run a quick test with the run.sh script. This will run the simulation with standard arguments, then run the plotting scripts. Pass in a test name as the only argument, which will be the resulting directory name under ./logs/kafka/.

  ```sudo ./run.sh quick-test```

  ## Sample command lines
  
  1) Emulate and sync data in a small network.
  
  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2```
  
  2) Create plots for metrics of interest (e.g., bandwidth consumption). Navigate through the `logs/` folder after the simulation finishes to check them.

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --create-plots```
  
  3) Create custom load. This example assumes a Gaussian distribution for the message sizes and two traffic classes (one sending messages at half the specified rate, i.e., 0.4, and another at twice).

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --create-plots --message-size gaussian,2,5 --message-rate 0.4 --traffic-classes 0.5,2```
  
  4) Create custom load with fixed message sizes.

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --create-plots --message-size fixed,50 --message-rate 0.4 --traffic-classes 0.5,2```
  
  5) Set the number of topics

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --create-plots --ntopics 4```
  
  6) Set a replication factor for each topic (e.g., store two copies of each message)
  
  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --create-plots --ntopics 4 --replication 2```
  
  7) Set the consumer rate (e.g., check new messages every 2 seconds)

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --create-plots --consumer-rate 0.5```
  
  8) Set a duration for the simulation. OBS.: this is the time the workload will run, not the total simulation time

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --create-plots --time 60```
  
  9) Include a disconnect of 3 random hosts for a specified duration (in seconds) during the simulation (default 60s).

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --create-plots --time 60 --dc-random 3 --dc-duration 20```

  10) Include a disconnect of specific hosts for a specified duration (in seconds) during the simulation.

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --create-plots --time 60 --dc-hosts h1,h2,h5```

  11) Disconnect the Zookeeper contoller (leader) for a specified duration (in seconds) during the simulation. May or not disconnect an extra node when used in combination with ```-dc-hosts```.

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --create-plots --time 60 --dc-zk-leader```

  12) Disconnect a specific number of nodes that are topic leaders for a specified duration (in seconds) during the simulation. Do not disconnect the ZK leader unless ```--dc-zk-leader``` is specified. 

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --create-plots --time 60 --dc-topic-leaders 3```
  
  13) Run the simulation using the single consumer behaviour - one consumer per host that subscribes to all topics and is always connected.

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --create-plots --time 60 --single-consumer```
  
  14) Set the network latency between switches to apply after kafka has set up. This is to used to let kafka get set up on a low latency, then run the simulation using high latency.

  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --create-plots --time 60 --latency-after-setup```
  
  15) Capture the network traffic of all nodes.
  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --create-plots --time 60 --capture-all```
  
  16) Set the offsets topic replication factor.
  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --create-plots --time 60 --offsets-replication 3```
  
  17) Run with Kraft consensus protocol instead of Zookeeper.
  Note if this is your first time running you may need to run the following first:
  ```cd kafka-3.1.0 && ./gradlew jar -PscalaVersion=2.13.6```
  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2 --create-plots --time 60 --kraft```