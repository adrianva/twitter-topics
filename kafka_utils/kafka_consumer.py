import json
from kafka import KafkaConsumer

import lda_batch

TWEETS_TO_LDA = 10000

consumer = KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset='earliest')
consumer.subscribe(['signals'])
for message in consumer:
    signal = json.loads(message.value)
    print signal["number_of_tweets"]

    if int(signal["number_of_tweets"]) >= TWEETS_TO_LDA:
        lda_batch.main()
