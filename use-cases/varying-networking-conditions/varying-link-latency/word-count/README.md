# Word count

In this application, we facilated the running word-count application using a two Spark structured streaming(SS) applications chain. We use two Kafka topic respectively for input source and output sink. In the first application, textual data is ingested from one kafka topic to another via spark structured stream. The second application splits each line into words, generates running word count on those words and lastly, store the calculated value at a local file.


## Architecture

### Application Chain

![image](https://user-images.githubusercontent.com/6629591/185228018-2c9f9701-ff7e-42e0-9df2-d5042b49a8bb.png)


### Topology

![image](https://user-images.githubusercontent.com/6629591/185228142-f6256cf9-4e13-4e1c-a1b6-2c137382ea83.png)


## Queries  
  
      lines.select(explode(split(lines.value, ' ')).alias('word'))
      
      words.groupBy('word').count()
  
## Operations
  
  Selection
  
  Aggegation
  
## Input details
1. randomText1.txt: contains text data.
2. topicConfiguration.txt : associated topic name(s) in each line
3. Spark SS application
   - sparkApp1.py: first spark application
   - sparkApp2.py: second spark application
4. input.graphml:
   - contains topology description
     - node details (switch, host)
     - edge details (bandwidth, latency, source port, destination port)
   - contains component(s) configurations 
     - topicConfig: path to the topic configuration file
     - zookeeper: 1 = hostnode contains a zookeeper instance
     - broker: 1 = hostnode contains a zookeeper instance
     - producerType: producer type can be SFST/MFMT/RND; SFST denotes from Single File to Single Topic. MFMT,RND not supported right now.
     - producerConfig: for SFST, one pair of filePath, topicName
     - sparkConfig: sparkConfig will contain the input source, spark application path and output sink. Input source is a kafka topic, output sink can be kafka topic/a file directory.
 
## Running
   
 ```sudo python3 main.py use-cases/varying-networking-conditions/varying-link-latency/word-count/input.graphml --nzk 1 --nbroker 1```
