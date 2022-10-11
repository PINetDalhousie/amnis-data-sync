# amnis-data-sync

Tool for emulating data synchronization in mission critical networks.

The tool was tested on Ubuntu 18.04.1 and is based on Python 3.6, Kafka 2.13-2.8.0 and PySpark 3.2.1.

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

  Most dependencies can be installed using `apt install`:
  
  ```bash
  $ sudo apt install python3-pip mininet default-jdk xterm netcat
  
  $ sudo pip3 install mininet networkx kafka-python matplotlib python-snappy lz4 seaborn
  ```
  3. You are ready to go! Should be able to get help using:

  ```sudo python3 main.py -h```
  
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

  9) Run only Kafka/Spark application (default setup is running Kafka and Spark applications along side)

  ```sudo python3 main.py use-cases/reproducibility/input.graphml --nzk 1 --nbroker 1 --only-kafka 1```
  ```sudo python3 main.py use-cases/reproducibility/input.graphml --nzk 1 --nbroker 1 --only-spark 1```

