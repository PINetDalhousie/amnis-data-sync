# Sentiment analysis

In this application, we use python's specialized NLP library 'Textblob' to analyse the subjectivity and objectivity of those tweets. We first connect to Twitter using developer API and store tweets on a specific topic into a CSV file. As part of the pre-processing, we clean the unnecessary details (e.g., links, usernames, hashtags, re-tweets). Data is ingested to Kafka topic and then to the Spark Structured Streaming (SS) dataframe for real-time analysis. Using user-defined functions in Apache Spark, we imply text classification rules on received data and finally get a score of subjectivity and polarity. Subjectivity will be in the floating range of [0.0,1.0] where 0.0 denotes as very subjective and 1.0 denotes very objective. Polarity varies within [0.0,-1.0]. For efficient reading, we collect all tweets of a specific minute in a single file and process those in a single batch.

## Architecture

#### Topology
![image](https://user-images.githubusercontent.com/6629591/176445094-7222ba18-619e-4d91-a805-272dfe57c100.png)

#### Application Chain
![image](https://user-images.githubusercontent.com/6629591/176445449-13c86e17-037e-4650-9165-c241afa79b86.png)



## Queries  
  
  select(explode(split(lines.value, "t_end")).alias("word"))
  
  polarity_detection
  
  subjectivity_detection
  
## Operations
  
  Selection
  
  User-defined function
  
## Input details
1. data.csv : contains input data
2. topicConfiguration.txt : associated topic names in each line
3. sentimentAnalysis.py : Spark SS application
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
   
 ```sudo python3 main.py use-cases/app-testing/sentiment-analysis/input.graphml --nzk 1 --nbroker 1```