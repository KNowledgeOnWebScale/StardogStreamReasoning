# generator/app.py
import os
import time
from time import sleep
import json
from kafka import KafkaProducer
import random

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
SEMANTIC_EVENTS_TOPIC = os.environ.get('SEMANTIC_EVENTS_TOPIC')
SLEEP_TIME = 1/40

if __name__ == '__main__':
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        # Encode all values as JSON
        value_serializer=lambda value: json.dumps(value).encode(),
    )
    index = 1
    while True:
        value = random.randint(18, 35)
        my_time = str(int(time.time()*1000))
        transaction = """
                {
          "@id" : "http://example.com/sensor.temperature/observations/"""+str(my_time)+"""",
          "@type" : [ "http://www.w3.org/ns/sosa/Observation" ],
          "http://www.w3.org/ns/sosa/hasSimpleResult" : [ {
            "@type" : "http://www.w3.org/2001/XMLSchema#float",
            "@value" : """+'"'+str(value)+'"'+"""
          } ],
          "http://www.w3.org/ns/sosa/observedProperty" : [ {
            "@id" :  "http://example.com/metrics/temperature"
          } ],
          "http://www.w3.org/ns/sosa/resultTime" : [ {
            "@type" : "http://www.w3.org/2001/XMLSchema#dateTime",
            "@value" : """+'"'+str(my_time)+'"'+"""
          } ]
        } """
        producer.send(SEMANTIC_EVENTS_TOPIC, value=transaction)
        sleep(SLEEP_TIME)
        index += 1
