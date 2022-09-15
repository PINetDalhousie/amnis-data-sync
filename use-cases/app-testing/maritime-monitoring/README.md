# Maritime monitoring
This application uses AIS data to determine the types of ships heading to each destination in a give timeframe, as well as the number and names of such ships.

Our aim is to use AIS data to get insights that can be useful for maritime monitoring. We wanted to see if Kafka and Spark Structured Streaming can be used to get such insights, especially in real-time. Hence, we developed this application.

We start out by feeding real-time AIS data from a JSON file to a Kafka topic. Next, we have the application read this data from the Kafka topic using Spark Structured Streaming. Using this data, the application determines the types of ships heading towards each destination in every minute, along with the names and numbers of such ships. Finally, the application then gets the results of this query, changes it into our required JSON format, and feeds this result to a Kafka topic. Specifying Kafka-MySQL connector configurations, query result can also be inserted from Kafka topilike external datastore table.

## Dataset
Our dataset is a collection of records obtained by collecting live AIS data. This is live data broadcast from ships.

The ships send messages of various types containing various types of information. For example, message type 3 contains latitutde and longitude information, message type 5 contains ship names, destinations etc.

For our application, we used messages of type 5, which contained information including ship types, ship names, and destinations.

To know more about the dataset, you can explore this excellent resource on AIS data which contains data under Norwegian license for public data (NLOD) made available by the Geological Survey of Norway (NGU): https://www.kystverket.no/en/navigation-and-monitoring/ais/access-to-ais-data/

We obtained the JSON file with the following command

nc 153.44.253.27 5631 | gpsdecode >> data.json

We let this command run for 3 minutes. After that, we opened data.json and deleted the last line as it was an incomplete json record.

## Architecture

### Application Chain
![image](https://user-images.githubusercontent.com/6629591/183961868-de56360c-9dd3-4ccf-96ce-9d7145cdec28.png)

### Topology
![image](https://user-images.githubusercontent.com/6629591/184164640-4bc89443-258c-430a-a14b-317001d3a818.png)



## Queries  
    vessel = vessel.filter( col("type") == 5).select("timestamp", "destination", "shiptype","shiptype_text", "mmsi")

    query = vessel.groupBy(window("timestamp", "1 minute"), "destination", "shiptype")\
            .agg(approx_count_distinct("mmsi").alias("numberOfShips"),\
            collect_set("mmsi").alias("shipIDs"))

  
## Operations
    Selection
    Projection
    Windowed aggregation
    Foreach implementation

  
## Input details
1. data.json : contains input data
2. topicConfiguration.txt : associated topic names in each line
3. maritime.py : Spark SS application
4. maritime-mysql-bulk-sink.properties: contains detailed MySQL configurations and topic name where MySQL will connect to.
5. Kafka-MySQL-user manual.odt: Configurations manual for setting up Kafka-MySQL connection.
6. input.graphml:
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
     - sparkConfig: sparkConfig will contain the spark application path and output sink. Output sink can be kafka topic/a file directory.
     - myslqConfig: mysqlConfig contains the file path of the MySQL configuration.

## Running
```sudo python3 main.py use-cases/app-testing/maritime-monitoring/input.graphml --nzk 1 --nbroker 2```

## Output
![image](https://user-images.githubusercontent.com/6629591/190420311-8053f116-874f-425a-add5-a1b46204f0ed.png)

