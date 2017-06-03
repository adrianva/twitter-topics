# -*- coding: utf-8 -*-
from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer, RegexTokenizer, StopWordsRemover


def set_pipeline(custom_stop_words=None):
    if not custom_stop_words:
        custom_stop_words = []

    re_tokenizer = RegexTokenizer(inputCol="text", outputCol="raw_tokens", pattern="\\W")
    stop_words_remover = StopWordsRemover(inputCol="raw_tokens", outputCol="words")
    stop_words_remover.setStopWords(stop_words_remover.getStopWords() + custom_stop_words)
    cv = CountVectorizer(inputCol="words", outputCol="vectors")
    pipeline = Pipeline(stages=[re_tokenizer, stop_words_remover, cv])
    return pipeline
