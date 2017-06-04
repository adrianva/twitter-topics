# -*- coding: utf-8 -*-
import os
import sys
import datetime
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import StructType, StructField, FloatType, StringType, MapType
from pyspark.ml.clustering import LocalLDAModel

import utils
import ml_utils
from tweet import Tweet
from kafka_utils.kafka_sink import KafkaSink

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
        "es.mapping.timestamp": "timestamp_ms",
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


def classify_tweets(time, rdd):
    # Get the singleton instance of SparkSession
    spark = utils.get_spark_session_instance(rdd.context.getConf())
    sql_context = SQLContext(spark.sparkContext)

    # Filter tweets without text
    row_rdd = rdd.map(lambda tweet: Row(
            id_str=tweet["id_str"],
            text=tweet["text"],
            timestamp_ms=tweet["timestamp_ms"],
            created_at=tweet["created_at"],
            user=tweet["user"],
            sentiment=tweet["sentiment"]
    )).filter(lambda tweet: tweet["text"])
    print row_rdd.take(5)

    schema = StructType([
        StructField("id_str", StringType(), True),
        StructField("text", StringType(), True),
        StructField("timestamp_ms", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("user", MapType(StringType(), StringType()), True),
        StructField("sentiment", MapType(StringType(), FloatType()), True),
    ])
    tweets_df = spark.createDataFrame(row_rdd, schema=schema)

    # Fit the texts in the LDA model and get the topics
    try:
        custom_stop_words = []
        pipeline = ml_utils.set_pipeline(custom_stop_words)
        model = pipeline.fit(tweets_df)

        result = model.transform(tweets_df)

        lda_model = LocalLDAModel.load("s3a://current-models/LDAModel")

        prediction = lda_model.transform(result)
        prediction.show(truncate=True)

        tweets_with_prediction = prediction.rdd.map(lambda tweet: Row(
            id_str=tweet["id_str"],
            text=tweet["text"],
            timestamp_ms=int(tweet["timestamp_ms"]),
            created_at=tweet["created_at"],
            user=tweet["user"],
            sentiment=tweet["sentiment"],
            topic_distribution=topic_distibution_to_dict(tweet["topicDistribution"])
        ))
        print tweets_with_prediction.take(5)

        save_to_elastic(tweets_with_prediction)

        tweets_with_prediction_df = sql_context.createDataFrame(tweets_with_prediction)
        tweets_with_prediction_df.registerTempTable("tweets")
        exploded_tweets = sql_context.sql("select id_str, timestamp_ms, explode(topic_distribution) from tweets")
        print exploded_tweets.take(5)
        exploded_tweets.foreachPartition(write_to_kafka)
    except Exception:
        print sys.exc_info()


def topic_distibution_to_dict(topic_distribution):
    topic_distribution_dict = {}
    for count, topic_probability in enumerate(topic_distribution.toArray().tolist()):
        topic_distribution_dict["topic_{}".format(count)] = topic_probability

    return topic_distribution_dict


if __name__ == "__main__":
    sc = SparkContext(appName="Stream Layer", master="local[2]")
    ssc = StreamingContext(sc, 10)
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

    tweets.pprint(5)

    tweets.foreachRDD(classify_tweets)

    ssc.start()
    ssc.awaitTermination()
