# twitter-topics
Topic extraction from Streaming Twitter API, implementing some Lambda Architecture with Spark.

## Installation
* Python libraries
* InfluxDB and Elasticsearch
* Kafka
* Kibana and Grafana (optional)

First, we should download all the libraries and dependencies of the project. For the Python side, we have to install the libraries listed in the requirements.txt file. Just type the following in the terminal:
```
pip install -r requirements.txt
```

We also must install Elasticsearch and InfluxDB for the database side.

> Apache Kafka is an open-source stream processing platform developed by the Apache Software Foundation written in Scala and Java. The project aims to provide a unified, high-throughput, low-latency platform for handling real-time data feeds.

And finally, we could install Grafana and Kibana as an easy way to visualize the results, but it's not required.

## Twitter Streaming API
We receive the data from the Twitter Streaming API, and then we store it raw in S3. We use the Twython Python library, which it's pretty easy to use, and send the data to some Kafka topic.

We can start this process by typing the following in the terminal:
```
```

## LDA Model
Launch lda_batch.py will create the LDA model using the texts of the tweets previously stored in S3. The result model is saved again in S3.

The LDA model is created using the ML library from Spark 2.0, using the LDA class provided by ml.clustering. As we said, this model is saved in S3:
```python
    lda_model.save("s3a://current-models/LDAModel")
```

In order to run this process, type the following command in the terminal:
```
```

## Clasifying the new tweets
When we have created our LDA model, it's time to use it to classify the new incoming tweets! The script stream.py does just this. It enrichs the new data with its topics and another useful metrics, such as sentiment.

Then, we store this info into Elasticsearch and into another Kafka topic, which will be used later to aggregate this data into InfluxDB.

## Storing tweets in Elastic and InfluxDB
Saving the RDDs into Elasticsearch is pretty straighforward once you have configured it properly (for more information about it, check this out: https://gist.github.com/adrianva/0bc59ba3bb24f0c7d9b4e89e8c621af9). As for InfluxDB, we use the Python Client, which makes saving the data a piece of cake.

## Visualizing it
