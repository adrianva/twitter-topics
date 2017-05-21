# -*- coding: utf-8 -*-
import json

from kafka import KafkaProducer


class KafkaSink:
    DEFAULT_CONFIG = {"host": "localhost:9092"}

    def __init__(self, config=DEFAULT_CONFIG):
        self.producer = KafkaProducer(
            bootstrap_servers=config["host"],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.config = config

    def send(self, value):
        try:
            self.producer.send(self.config["topic"], value)
        except AttributeError:
            raise AttributeError("topic not defined")
