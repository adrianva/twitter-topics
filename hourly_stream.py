# -*- coding: utf-8 -*-
import json
from datetime import datetime

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext

from tweet import Tweet


def get_hour(timestamp_ms):
    d = datetime.fromtimestamp(int(timestamp_ms) / 1000.0)
    # String format: YYMMDDHH00 (e.g: 20170504120000 -> 2017-05-04 12:00:00)
    hour_str = d.strftime('%Y')+d.strftime('%m')+d.strftime('%d')+d.strftime('%H')+"0000"
    return hour_str


def get_word_count(texts):
    return texts.flatMap(lambda line: line.split(" "))\
        .filter(lambda line: line != "")\
        .map(lambda word: (word, 1))\
        .reduceByKey(lambda a, b: a + b)


def sum_tweets(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)


def update_state_by_key(elements, update_function):
    return elements\
        .updateStateByKey(update_function)\
        .transform(lambda x: x.sortBy(lambda (x, v): -v))


def reduce_by_key_and_window(elements, window_duration, slide_duration):
    return elements\
        .reduceByKeyAndWindow(lambda a, b: a + b,
                              lambda a, b: a - b,
                              windowDuration=window_duration,
                              slideDuration=slide_duration
                              )\
        .transform(lambda x: x.sortBy(lambda (x, v): -v))


if __name__ == "__main__":
    sc = SparkContext(appName="Hourly Aggregation", master="local[2]")
    ssc = StreamingContext(sc, 10)
    sql_context = SQLContext(sc)
    ssc.checkpoint("checkpoint_hourly_stream")

    # Kafka connection
    brokers = 'localhost:9092'
    topics = ["processed_tweets"]

    kvs = KafkaUtils.createDirectStream(ssc, topics, {"metadata.broker.list": brokers})
    # Kafka emits tuples, so we need to access to the second element
    tweets_raw = kvs.map(lambda tweet: tweet[1]).cache()
    kvs.pprint(5)

    # The key is the topic and the hour of the tweet
    tweets_raw = tweets_raw.map(lambda tweet: json.loads(tweet))  # Convert strings to dicts
    tweets = tweets_raw.map(lambda tweet: Tweet.to_row(tweet)) \
        .map(lambda tweet: (get_hour(tweet.timestamp_ms), 1)) \

    tweets_by_date = update_state_by_key(tweets, sum_tweets)
    tweets_last_hour = reduce_by_key_and_window(
            tweets, window_duration=3600, slide_duration=10
    )

    tweets_last_hour.pprint(5)

    ssc.start()
    ssc.awaitTermination()
