import os
import threading
import json
from pyspark.sql import SparkSession

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
        texts = load_texts_from_s3(sc)


def load_texts_from_s3(sc):
    try:
        tweets = sc.textFile("s3a://twitter-topics-tweets-streaming/*/*")
        if tweets:
            tweets = tweets.map(lambda tweet: json.loads(tweet))
            texts = tweets.map(lambda tweet: tweet["text"])
            print texts.take(10)
            return texts
        return None
    except OSError:
        print "Directory is empty..."


if __name__ == "__main__":
    spark = SparkSession.builder.appName("LDA Batch Model").getOrCreate()
    sc = spark.sparkContext

    print AWS_ACCESS_KEY_ID
    print AWS_SECRET_ACCESS_KEY
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', AWS_SECRET_ACCESS_KEY)

    texts = load_texts_from_s3(sc)
