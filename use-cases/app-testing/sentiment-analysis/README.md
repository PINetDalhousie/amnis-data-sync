# Sentiment analysis

In this application, we use python's specialized NLP library 'Textblob' to analyse the subjectivity and objectivity of those tweets. We first connect to Twitter using developer API and store tweets on a specific topic into a CSV file. As part of the pre-processing, we clean the unnecessary details (e.g., links, usernames, hashtags, re-tweets). Data is ingested to Kafka topic and then to the Spark streaming dataframe for real-time analysis. Using user-defined functions in Apache Spark, we imply text classification rules on received data and finally get a score of subjectivity and polarity. Subjectivity will be in the floating range of [0.0,1.0] where 0.0 denotes as very subjective and 1.0 denotes very objective. Polarity varies within [0.0,-1.0]. For efficient reading, we collect all tweets of a specific minute in a single file and process those in a single batch.

## Architecture

TODO: add a figure depicting the chain

## Queries  
  
  select(explode(split(lines.value, "t_end")).alias("word"))
  
  polarity_detection
  
  subjectivity_detection
  
## Operations
  
  Selection
  
  User-defined function
  
## Running
   
 ```sudo python3 main.py use-cases/app-testing/sentiment-analysis/input.graphml --nzk 1 --nbroker 2```