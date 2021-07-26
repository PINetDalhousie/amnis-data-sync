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

  Most dependencies can be installed using `apt install`:
  
  ```bash
  $ sudo apt install python3-pip mininet default-jdk xterm
  
  $ pip3 install mininet networkx kafka-python matplotlib
  ```
  3. You are ready to go! Should be able to get help using:

  ```sudo python3 main.py -h```
  
  ## Sample command lines
  
  Emulate and sync data in a small network. If emulation completes successfully, you will be able to see consumed data on `consumed-data.txt`.
  
  ```sudo python3 main.py tests/input/simple.graphml --nbroker 2 --nzk 2```
  
  
  
  
  
  
  
  
  
  
  
  
  
