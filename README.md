# San Francisco Crime Statistic with Spark

The aim of this project is to get closer to Spark Streaming ecosystem. It also has a goal to show a good collaboration between Spark and Kafka ecosystems.- 

## Project Overview
In this project, a real-world dataset on San Francisco crime incidents, extracted from Kaggle is used and the task is 
to provide statistical analyses of the data using Apache Spark Structured Streaming. 
At first Kafka server should be created in order to produce data and ingest the data through Spark Structured Streaming.

##Development Environment
The project could be developed in the workspace Udacity provides or it could be developed locally. In any case,
the development environment requirements are:

* Spark 2.4.3
* Scala 2.11.x
* Java 1.8.x
* Kafka build with Scala 2.11.x
* Python 3.6.x or 3.7.x

## Project execution

#### Start zookeeper
```
/usr/bin/zookeeper-server-start config/zookeeper.properties
```
#### Start kafka
```
/usr/bin/kafka-server-start config/server.properties
```
#### Start kafka producer
```
python kafka_server.py
```
#### Submit Spark job
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --master local[*] data_stream.py
```
## Steps in the task
### Step 1
* The first step is to build a simple Kafka server
* Complete the code for the server in producer_server.py and kafka_server.py
### Step 2
* Apache Spark already has an integration with Kafka brokers, so we would not normally need a separate Kafka consumer. However, we are going to ask you to create one anyway. Why? We'd like you to create the consumer to demonstrate your understanding of creating a complete Kafka Module (producer and consumer) from scratch. In production, you might have to create a dummy producer or consumer to just test out your theory and this will be great practice for that
* Implement all the TODO items in data_stream.py
* Take a screenshot of your progress reporter after executing a Spark job
* Take a screenshot of the Spark Streaming UI as the streaming continues
### Step 3
1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?
1. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?
