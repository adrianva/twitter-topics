import os
import threading
import json

from pyspark.ml import Pipeline
from pyspark.mllib.clustering import LDA, LDAModel
from pyspark.mllib.linalg import Vectors
from pyspark.ml.feature import CountVectorizer, RegexTokenizer, StopWordsRemover
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *


AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]

NUMBER_OF_TOPICS = 3


class BatchClass(threading.Thread):
    def __init__(self, spark_context=None):
        super(BatchClass, self).__init__()
        self.spark_context = spark_context
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark_context._jsc.hadoopConfiguration().set('fs.s3a.access.key', AWS_ACCESS_KEY_ID)
        spark_context._jsc.hadoopConfiguration().set('fs.s3a.secret.key', AWS_SECRET_ACCESS_KEY)

    def run(self):
        print "Starting Batch Layer: " + self.name
        # TODO Batch layer goes here... (copy the main method)


def load_texts_from_s3():
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


def load_stop_words():
    stop_words_text = sc.textFile("resources/stop_words")
    stop_words = stop_words_text.flatMap(lambda text: text.strip().split(","))
    return stop_words.collect()


def load_texts():
    """
    :return: A DataFrame with all documents plus their ids
    """
    texts = load_texts_from_s3()
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("text", StringType(), True),
    ])
    texts_df = spark.createDataFrame(texts, schema=schema)
    return texts_df


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
    spark = SparkSession.builder.appName("LDA Batch Model").getOrCreate()
    sc = spark.sparkContext

    print AWS_ACCESS_KEY_ID
    print AWS_SECRET_ACCESS_KEY
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', AWS_SECRET_ACCESS_KEY)

    custom_stop_words = load_stop_words()
    texts_df = load_texts()

    pipeline = set_pipeline(custom_stop_words)
    model = pipeline.fit(texts_df)

    result = model.transform(texts_df)

    # Index documents with unique IDs
    corpus = result.select("id", "vectors").rdd.map(lambda (x, y): [x, Vectors.fromML(y)]).cache()

    # Cluster the documents into three topics using LDA
    lda_model = LDA.train(corpus, k=NUMBER_OF_TOPICS)

    vocabulary = model.stages[2].vocabulary
    topic_indices = sc.parallelize(lda_model.describeTopics(maxTermsPerTopic=20))

    topics = topic_indices\
        .map(lambda (terms, weights): map(lambda (term, weight): (vocabulary[int(term)], weight), zip(terms, weights)))

    print_topics(topics)

    # Save and load model
    lda_model.save(sc, "s3a://current-models/LDAModel")
    same_model = LDAModel.load(sc, "s3a://current-models/LDAModel")

    sc.stop()
