import os
import threading
import json

from pyspark.mllib.clustering import LDA, LDAModel
from pyspark.mllib.linalg import Vectors
from pyspark.ml.feature import CountVectorizer
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *

from textblob import TextBlob

import utils


AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]


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


def load_texts_from_s3(sc):
    try:
        tweets = sc.textFile("s3a://twitter-topics-tweets-streaming/*/*")
        if tweets:
            tweets = tweets.map(lambda tweet: json.loads(tweet))
            texts = tweets.map(
                lambda tweet: Row(
                    id=int(tweet["id_str"]),
                    words=_converto_to_list_of_strings(TextBlob(tweet["text"]).words)
                )
            )
            print texts.take(10)
            return texts
        return None
    except OSError:
        print "Directory is empty..."


def _converto_to_list_of_strings(words_list):
    return [word.string for word in words_list]


if __name__ == "__main__":
    spark = SparkSession.builder.appName("LDA Batch Model").getOrCreate()
    sc = spark.sparkContext

    print AWS_ACCESS_KEY_ID
    print AWS_SECRET_ACCESS_KEY
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', AWS_SECRET_ACCESS_KEY)

    texts = load_texts_from_s3(sc)
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("words", ArrayType(StringType()), True),
    ])
    texts_df = spark.createDataFrame(texts, schema=schema)
    cv = CountVectorizer(inputCol="words", outputCol="vectors")
    model = cv.fit(texts_df)
    result = model.transform(texts_df)

    # Index documents with unique IDs
    corpus = result.select("id", "vectors").rdd.map(lambda (x, y): [x, Vectors.fromML(y)]).cache()

    # Cluster the documents into three topics using LDA
    lda_model = LDA.train(corpus, k=3)

    # Output topics. Each is a distribution over words (matching word count vectors)
    print("Learned topics (as distributions over vocab of " + str(lda_model.vocabSize()) + " words):")
    topics = lda_model.topicsMatrix()
    for topic in range(3):
        print("Topic " + str(topic) + ":")
        for word in range(0, lda_model.vocabSize()):
            print(" " + str(topics[word][topic]))

    # Save and load model
    lda_model.save(sc, "target/LDAModel")
    sameModel = LDAModel.load(sc, "target/LDAModel")

    sc.stop()
