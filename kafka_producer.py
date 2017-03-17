# -*- coding: utf-8 -*-
import os
import time
from kafka import KafkaProducer


from twython import TwythonStreamer


class TwitterStream(TwythonStreamer):
    def __init__(self, *args, **kwargs):
        super(TwitterStream, self).__init__(*args, **kwargs)
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092')

    def on_success(self, data):
        if 'text' in data:
            tweet_text = data['text'].encode('utf-8')
            self.producer.send("test", tweet_text)
            time.sleep(0.1)

    def on_error(self, status_code, data):
        print status_code
        if status_code == 420:  # Rate Limited
            self.disconnect()


if __name__ == "__main__":
    stream = TwitterStream(
        app_key='kZtpO5JBXcoQCiEDdl72SQQhM',
        app_secret='GNIAUD85GSa6GPiOVGOfuIST4sOv98ianvjZ9qiKNFJvHuaUtj',
        oauth_token='21989305-riNmaa2Z9baFFs1quWhpGbMNJKhFEhFTiSOklIidw',
        oauth_token_secret='NJdcXi3EPCvishtiNt9P1LTmlt1elKVhiQa4cGX0X7QWD'
    )
    stream.statuses.filter(track='#f1', language='en')
