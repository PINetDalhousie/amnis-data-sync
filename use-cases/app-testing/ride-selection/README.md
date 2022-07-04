# Ride Selection

In this application, we present a use-case where the taxi driver can yield higher tips using real-time data processing. We use original data from New York Manhattan neighbourhoods. These data contains both geographical and financial information of the ride. Taxi ride data are ingested to the event streaming application. Stream processing engine consume that data and process it in near real-time. To do that, at first, we clean the data. Then, we introduce stream-stream joining with watermarking between geospatial and financial streams. Later, we then take the leverage of geographic coordinates to find out the rides relevant to the Manhattan neighbourhood. Finally, we take a window of thirty minutes to aggregate the average tip in Manhattan locality. The application will inform the taxi driver which area the driver should choose to get a higher tip.

## Architecture

### Application Chain


### Topology



## Queries  
  
  select(explode(split(lines.value, "t_end")).alias("word"))
  
  polarity_detection
  
  subjectivity_detection
  
## Operations
  
  Selection
  
  User-defined function
  
## Input details
1. nycTaxiRidesdata.csv, nycTaxiFaresdata.csv : contains input data
2. topicConfiguration.txt : associated topic names in each line
3. rideSelection.py : Spark SS application
4. input.graphml:
   - contains topology description
     - node details (switch, host)
     - edge details (bandwidth, latency, source port, destination port)
   - contains component(s) configurations 
     - topicConfig : path to the topic configuration file
     - zookeeper : 1 = hostnode contains a zookeeper instance
     - broker : 1 = hostnode contains a zookeeper instance
     - producerType: producer type can be SFST/MFMT/RND; SFST denotes from Single File to Single Topic. MFMT,RND not supported right now.
     - producerConfig: for SFST, one pair of filePath, topicName
     - consumerConfig: contains the topic name(s) from where the consumer will consume
     - sparkConfig: sparkConfig will contain the input source, spark application path and output sink. Input source is a kafka topic, output sink can be kafka topic/a file directory.
     
## Running
   
 ```sudo python3 main.py use-cases/app-testing/ride-selection/input.graphml --nzk 1 --nbroker 2```
