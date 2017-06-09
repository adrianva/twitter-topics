# -*- coding: utf-8 -*-
import sys
import os
import codecs

from pyspark.ml.clustering import LDA
from pyspark.sql import SparkSession

import utils
import ml_utils

AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]

NUMBER_OF_TOPICS = 3


def main():
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

    pipeline = ml_utils.set_pipeline(custom_stop_words)
    model = pipeline.fit(texts_df)

    result = model.transform(texts_df)

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
    lda_model.save("s3a://current-models/LDAModel")

    sc.stop()

if __name__ == "__main__":
    main()
