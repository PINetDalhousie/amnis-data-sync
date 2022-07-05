# Ride Selection

In this application, we present a use-case where the taxi driver can yield higher tips using real-time ride selection. We use original data from New York City Taxi and Limyousine Commission [TLC dataset] (https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and customize it according to our need. These data contains both geographical and financial information of the ride. Taxi ride data are ingested to the event streaming application. Stream processing engine consume that data and process it in near real-time. To do that, at first, we clean the data. Then, we introduce stream-stream joining with watermarking between geospatial and financial streams. Later, we then take the leverage of geographic coordinates of Manhattan neighbourhoods only (for simlicity)  to find out the relevant rides. Finally, we take a window of thirty minutes to aggregate the average tip in Manhattan locality. The application will inform the taxi driver which area the driver should choose to get a higher tip.

## Architecture

### Application Chain
![image](https://user-images.githubusercontent.com/6629591/177374196-819feec8-dd77-4cc5-a414-f91865eef497.png)


### Topology
![image](https://user-images.githubusercontent.com/6629591/177374238-355f4be9-fcc5-4945-8c2c-3c71b10c942a.png)



## Queries  
  
  sdfFaresWithWatermark = sdfFares.selectExpr("rideId AS rideId_fares", "startTime", "totalFare", "tip").withWatermark("startTime", "30 minutes")
  
  sdfFaresWithWatermark.join(sdfRidesWithWatermark, \
      expr(""" 
       rideId_fares = rideId AND 
        endTime > startTime AND
        endTime <= startTime + interval 2 hours
        """))
  
  sdf.groupBy(window("endTime", "30 minutes", "10 minutes"),"stopNbhd").agg(avg("tip"))
  
## Operations
  
  Selection
  
  Watermarking
  
  Stream-stream join
  
  Broadcasting
  
  Windowed grouped aggegation
  
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
     - sparkConfig: sparkConfig will contain the input source, spark application path and output sink. Input source is a kafka topic, output sink can be kafka topic/a file directory.
 5. nbhd.jsonl: contains all Manhattan neighborhoods coordinates one per line.
 
## Running
   
 ```sudo python3 main.py use-cases/app-testing/ride-selection/input.graphml --nzk 1 --nbroker 2```
