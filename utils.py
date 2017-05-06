# -*- coding: utf-8 -*-
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, LongType, StringType


def get_spark_session_instance(spark_conf):
    if 'sparkSessionSingletonInstance' not in globals():
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=spark_conf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']


def load_texts_from_s3(sc):
    try:
        tweets = sc.textFile("s3a://twitter-topics-tweets-streaming/*/*")
        if tweets:
            tweets = tweets.map(lambda tweet: json.loads(tweet))
            texts = tweets.map(
                lambda tweet: Row(
                    id=int(tweet["id_str"]),
                    text=tweet["text"]
                )
            )
            print texts.take(10)
            return texts
        return None
    except OSError:
        print "Directory is empty..."


def load_stop_words(sc):
    stop_words_text = sc.textFile("resources/stop_words")
    stop_words = stop_words_text.flatMap(lambda text: text.strip().split(","))
    return stop_words.collect()


def load_texts(spark):
    """
    :return: A DataFrame with all documents plus their ids
    """
    sc = spark.sparkContext
    texts = load_texts_from_s3(sc)
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("text", StringType(), True),
    ])
    texts_df = spark.createDataFrame(texts, schema=schema)
    return texts_df
