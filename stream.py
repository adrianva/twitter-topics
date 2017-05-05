# -*- coding: utf-8 -*-
import os
import datetime
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, Row

from kafka import KafkaProducer
from textblob import TextBlob

AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]


def parse_tweets(tweets):
    tweets = tweets.map(lambda tweet: to_row(tweet))
    return tweets


def to_row(tweet_json):
    text_blob = TextBlob(tweet_json["text"])
    word_counts = text_blob.word_counts
    sentiment = text_blob.sentiment
    tweet_json["sentiment"] = {"polarity": sentiment.polarity, "subjectivity": sentiment.subjectivity}
    tweet_json["word_counts"] = dict(word_counts)
    coordinates = tweet_json["coordinates"].get("coordinates")

    return Row(
        id_str=tweet_json["id_str"],
        text=tweet_json["text"],
        timestamp_ms=tweet_json["timestamp_ms"],
        created_at=tweet_json["created_at"],
        user={
            "screen_name": tweet_json["user"]["screen_name"],
            "time_zone": tweet_json["user"]["time_zone"]
        },
        sentiment=tweet_json["sentiment"],
        coordinates=coordinates
    )


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
        "topic": "aggregated_tweets"
    }

    kafka_sink = KafkaSink(kafka_config)
    for element in elements:
        kafka_sink.send(element)

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
    ssc.checkpoint("checkpoint")

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

    ssc.start()
    ssc.awaitTermination()
