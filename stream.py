# -*- coding: utf-8 -*-
import os
import datetime
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext

from kafka import KafkaProducer

from tweet import Tweet


AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]


def parse_tweets(tweets):
    tweets = tweets.map(lambda tweet: Tweet.to_row(tweet))
    return tweets


def save_stream(rdd):
    rdd.saveAsTextFile("s3a://twitter-topics-tweets-streaming/" + datetime.datetime.now().strftime("%Y%m%d%H%M%S"))


def save_to_elastic(rdd):
    es_write_conf = {
        "es.nodes": "localhost",
        "es.port": "9200",
        "es.resource": "twitter/tweet",
        "es.mapping.id": "id_str",
        "es.mapping.timestamp": "timestamp_ms"
    }

    rdd_to_elastic = rdd.map(lambda row: (None, row.asDict()))
    rdd_to_elastic.saveAsNewAPIHadoopFile(
        path='-',
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=es_write_conf
    )


def write_to_kafka(elements):
    kafka_config = {
        "host": "localhost:9092",
        "topic": "processed_tweets"
    }

    kafka_sink = KafkaSink(kafka_config)
    for element in elements:
        kafka_sink.send(element.asDict())

    kafka_sink.producer.close()


class KafkaSink:
    DEFAULT_CONFIG = {"host": "localhost:9092"}

    def __init__(self, config=DEFAULT_CONFIG):
        self.producer = KafkaProducer(
            bootstrap_servers=config["host"],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.config = config

    def send(self, value):
        try:
            self.producer.send(self.config["topic"], value)
        except AttributeError:
            raise AttributeError("topic not defined")


if __name__ == "__main__":
    sc = SparkContext(appName="Stream Layer", master="local[2]")
    ssc = StreamingContext(sc, 10)
    sql_context = SQLContext(sc)
    ssc.checkpoint("checkpoint_stream")

    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', AWS_SECRET_ACCESS_KEY)

    # Kafka connection
    brokers = 'localhost:9092'
    topics = ["raw_tweets"]

    kvs = KafkaUtils.createDirectStream(ssc, topics, {"metadata.broker.list": brokers})
    # Kafka emits tuples, so we need to acces to the second element
    tweets = kvs.map(lambda tweet: tweet[1]).cache()

    # save to HDFS
    tweets.foreachRDD(save_stream)

    tweets = tweets.map(lambda tweet: json.loads(tweet))  # Convert strings to dicts
    tweets = parse_tweets(tweets)
    tweets.foreachRDD(save_to_elastic)
    tweets.foreachRDD(lambda rdd: rdd.foreachPartition(write_to_kafka))

    tweets.pprint(5)

    ssc.start()
    ssc.awaitTermination()
