# -*- coding: utf-8 -*-
import sys
import os
import datetime
import threading

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, Row
from pyspark.ml.feature import RegexTokenizer

AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]


class StreamClass(threading.Thread):
    # Kafka connection
    DEFAULT_BROKER = 'localhost:9092'
    DEFAULT_TOPIC = ['test']

    def __init__(self, spark_context=None, batch_duration=5, brokers=DEFAULT_BROKER, topics=DEFAULT_TOPIC):
        super(StreamClass, self).__init__()
        self.spark_context = spark_context
        self.streaming_context = StreamingContext(spark_context, batchDuration=batch_duration)
        self.sql_context = SQLContext(spark_context)
        self.streaming_context.checkpoint("checkpoint")

        self.kvs = KafkaUtils.createDirectStream(self.streaming_context, topics, {"metadata.broker.list": brokers})

    def run(self):
        print "Starting Stream Layer: " + self.name
        # Kafka emits tuples, so we need to acces to the second element
        lines = self.kvs.map(lambda line: line[1]).cache()

        # save to HDFS
        lines.foreachRDD(save_stream)

        self.streaming_context.start()
        self.streaming_context.awaitTermination()


def parse_tweet(tweet_json):
    return Row(
        id_str=tweet_json["id_str"],
        text=tweet_json['text'].encode('utf-8')
    )


def save_stream(rdd):
    rdd.saveAsTextFile("s3a://twitter-topics-tweets-streaming/" + datetime.datetime.now().strftime("%Y%m%d%H%M%S"))


def get_words(lines):
    words = lines.flatMap(lambda line: line.split())
    return words


def word_tokenize(line):
    import nltk
    return nltk.word_tokenize(line)


if __name__ == "__main__":
    sc = SparkContext(appName="Stream Layer", master="local[2]")
    ssc = StreamingContext(sc, 10)
    sql_context = SQLContext(sc)
    ssc.checkpoint("checkpoint")

    print AWS_ACCESS_KEY_ID
    print AWS_SECRET_ACCESS_KEY
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', AWS_SECRET_ACCESS_KEY)

    # Kafka connection
    brokers = 'localhost:9092'
    topics = ["test"]

    kvs = KafkaUtils.createDirectStream(ssc, topics, {"metadata.broker.list": brokers})
    # Kafka emits tuples, so we need to acces to the second element
    tweets = kvs.map(lambda tweet: tweet[1]).cache()

    # save to HDFS
    tweets.foreachRDD(save_stream)

    ssc.start()
    ssc.awaitTermination()
