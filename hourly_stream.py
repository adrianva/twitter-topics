# -*- coding: utf-8 -*-
import json
from datetime import datetime
import calendar

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, Row

from influxdb import InfluxDBClient


def get_hour(timestamp_ms):
    d = datetime.fromtimestamp(int(timestamp_ms) / 1000.0)
    # String format: YYMMDDHH0000 (e.g: 20170504120000 -> 2017-05-04 12:00:00)
    hour_str = d.strftime('%Y')+d.strftime('%m')+d.strftime('%d')+d.strftime('%H')+"0000"
    return hour_str


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


def save_to_influx(iter):
    client = InfluxDBClient("localhost", 8086, database="hourly_tweets")

    for record in iter:
        datetime_object = datetime.strptime(record[0][1], '%Y%m%d%H%M%S')
        timestamp_ns = calendar.timegm(datetime_object.timetuple()) * 1000000  # nanoseconds

        json_body = [
            {
                "measurement": "number_of_tweets",
                "tags": {
                    "topic": record[0][0],
                },
                "time": int(timestamp_ns),
                "fields": {
                    "value": record[1],
                }
            }
        ]
        #print json_body
        client.write_points(json_body)


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

    tweets_raw = tweets_raw.map(lambda tweet: json.loads(tweet))  # Convert strings to dicts
    tweets = tweets_raw.map(lambda tweet: Row(
        timestamp_ms=tweet["timestamp_ms"],
        key=tweet["key"],
        value=tweet["value"]
     )).map(lambda tweet: ((tweet.key, get_hour(tweet.timestamp_ms)), tweet.value))

    tweets_by_date = update_state_by_key(tweets, sum_tweets)
    tweets_last_hour = reduce_by_key_and_window(
            tweets, window_duration=3600, slide_duration=10
    )

    tweets_last_hour.pprint(5)

    tweets_last_hour.foreachRDD(lambda rdd: rdd.foreachPartition(save_to_influx))

    ssc.start()
    ssc.awaitTermination()
