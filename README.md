# twitter-topics
Topic extraction from Streaming Twitter API, implementing some Lambda Architecture with Spark.

## Installation
* Python libraries
* InfluxDB and Elasticsearch
* Kafka
* Kibana and Grafana (optional)
* Spark (version 2.0 or above)

This tutorial will explain how to install the project in a local environment, but this should be easily extendible in a cluster. In the future it will be interesting to use Docker to provision the environment, but currently is a work in progress.

First, we should download all the libraries and dependencies of the project. For the Python side, we have to install the libraries listed in the requirements.txt file. Just type the following in the terminal:
```
pip install -r requirements.txt
```

We also must install Elasticsearch and InfluxDB for the database side.

Then, install Kafka and create some topics:
> Apache Kafka is an open-source stream processing platform developed by the Apache Software Foundation written in Scala and Java. The project aims to provide a unified, high-throughput, low-latency platform for handling real-time data feeds.

```kafka-topics --zookeeper localhost:2181 --create --topic raw_tweets --partitions 1 --replication-factor 1```
```kafka-topics --zookeeper localhost:2181 --create --topic processed_tweets --partitions 1 --replication-factor 1```

raw_tweets will store the tweets retrieved from the Streaming API.
processed_tweets will store the tweets with the extra information, this is, topics and sentiment.

Now, it's time to download and install Spark. This project has been developed with Spark 2.0.3, so it is just a matter of following its documentation.

And finally, we could install Grafana and Kibana as an easy way to visualize the results, but it's not required.

## Initializing all services
We need to be sure that all services are running:

1. Kafka: 
```kafka-server-start /usr/local/etc/kafka/server.properties```
2. Elasticsearh:
```elasticsearch```
3. InfluxDB:
```influxd```

## Twitter Streaming API
We receive the data from the Twitter Streaming API, and then we store it raw in S3. We use the Twython Python library, which it's pretty easy to use, and send the data to some Kafka topic.

We can start this process by typing the following in the terminal:
```
python /kafka_utils/kafka_producer.py
```

## LDA Model
Launch lda_batch.py will create the LDA model using the texts of the tweets previously stored in S3. The result model is saved again in S3.

The LDA model is created using the ML library from Spark 2.0, using the LDA class provided by ml.clustering. As we said, this model is saved in S3:
```python
    lda_model.save("s3a://current-models/LDAModel")
```

In order to run this process, type the following command in the terminal:
```
spark-submit --jars jars/spark-streaming-kafka-0-8-assembly_2.11-2.0.2.jar,aws-java-sdk-1.7.4.jar,hadoop-aws-2.7.3.jar,elasticsearch-spark-20_2.10-5.2.2.jar ./lda_batch.py
```

## Clasifying the new tweets
When we have created our LDA model, it's time to use it to classify the new incoming tweets! The script stream.py does just this. It enrichs the new data with its topics and another useful metrics, such as sentiment.

Then, we store this info into Elasticsearch and into another Kafka topic, which will be used later to aggregate this data into InfluxDB.

The streaming process can be started using this command:
```
spark-submit --jars jars/spark-streaming-kafka-0-8-assembly_2.11-2.0.2.jar,aws-java-sdk-1.7.4.jar,hadoop-aws-2.7.3.jar,elasticsearch-spark-20_2.10-5.2.2.jar ./stream.py
```

On the other hand, this script also stores the new tweets directly into S3 in raw format. As we saw before, the LDA model reads from S3 in order to obtain the texts which allow to create the model.

## Storing tweets in Elastic and InfluxDB
Saving the RDDs into Elasticsearch is pretty straighforward once you have configured it properly (for more information about it, check this out: https://gist.github.com/adrianva/0bc59ba3bb24f0c7d9b4e89e8c621af9). As for InfluxDB, we use the Python Client, which makes saving the data a piece of cake.

## Aggregating data
Elasticsearch is used to store the most recent data, something like the last 14 days or so, but for historical data we have chosen InfluxDB, a time series database which scales pretty well.

At the moment, the data is aggregated each hour, using Spark Streaming for this task. We use a window of 3600s (1 hour) with a slide duration of 10s. Every time we write into InfluxDB we store the number of tweets for a given topic each hour. 

## Visualizing it
