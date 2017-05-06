# -*- coding: utf-8 -*-
from textblob import TextBlob
from pyspark.sql import Row


class Tweet:
    @staticmethod
    def to_row(tweet_json):
        text_blob = TextBlob(tweet_json["text"])
        word_counts = text_blob.word_counts
        sentiment = text_blob.sentiment
        tweet_json["sentiment"] = {"polarity": sentiment.polarity, "subjectivity": sentiment.subjectivity}
        tweet_json["word_counts"] = dict(word_counts)

        return Row(
            id_str=tweet_json["id_str"],
            text=tweet_json["text"],
            timestamp_ms=tweet_json["timestamp_ms"],
            created_at=tweet_json["created_at"],
            user={
                "screen_name": tweet_json["user"]["screen_name"],
                "time_zone": tweet_json["user"]["time_zone"]
            },
            sentiment=tweet_json["sentiment"],
            topics=None
        )
