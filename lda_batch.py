# -*- coding: utf-8 -*-
import sys
import os
import codecs

from pyspark.ml import Pipeline
from pyspark.ml.clustering import LDA, LDAModel
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import CountVectorizer, RegexTokenizer, StopWordsRemover
from pyspark.sql import SparkSession


import utils

AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]

NUMBER_OF_TOPICS = 3


def set_pipeline(custom_stop_words=None):
    re_tokenizer = RegexTokenizer(inputCol="text", outputCol="raw_tokens", pattern="\\W")
    stop_words_remover = StopWordsRemover(inputCol="raw_tokens", outputCol="words")
    stop_words_remover.setStopWords(stop_words_remover.getStopWords() + custom_stop_words)
    cv = CountVectorizer(inputCol="words", outputCol="vectors")
    pipeline = Pipeline(stages=[re_tokenizer, stop_words_remover, cv])
    return pipeline


def print_topics(topics):
    print "{} topics:".format(NUMBER_OF_TOPICS)
    topics_with_index = topics.zipWithIndex()
    topics_with_index.foreach(lambda (topic, i): print_topic(topic, i))


def print_topic(topic, i):
    print "TOPIC {}".format(i)
    for term in topic:
        print "{0}-{1}".format(term[0], term[1])


if __name__ == "__main__":
    sys.stdout = codecs.getwriter('utf8')(sys.stdout)
    sys.stderr = codecs.getwriter('utf8')(sys.stderr)

    spark = SparkSession.builder.appName("LDA Batch Model").getOrCreate()
    sc = spark.sparkContext

    print AWS_ACCESS_KEY_ID
    print AWS_SECRET_ACCESS_KEY
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', AWS_SECRET_ACCESS_KEY)

    custom_stop_words = utils.load_stop_words(sc)
    texts_df = utils.load_texts(spark)

    pipeline = set_pipeline(custom_stop_words)
    model = pipeline.fit(texts_df)

    result = model.transform(texts_df)

    # Index documents with unique IDs
    corpus = result.select("id", "vectors").rdd.map(lambda (x, y): [x, Vectors.fromML(y)]).cache()

    # Cluster the documents into three topics using LDA
    lda = LDA(k=NUMBER_OF_TOPICS, maxIter=5, featuresCol="vectors")
    lda_model = lda.fit(result)

    # Describe topics
    topics = lda_model.describeTopics(3)
    print("The topics described by their top-weighted terms:")
    topics.show(truncate=False)

    # Shows the result
    transformed = lda_model.transform(result)
    transformed.show(truncate=False)

    # Save and load model
    lda_model.save(sc, "s3a://current-models/LDAModel")
    same_model = LDAModel.load(sc, "s3a://current-models/LDAModel")

    sc.stop()
