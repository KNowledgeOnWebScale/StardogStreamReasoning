import re
import os
import json
import stardog
from kafka import KafkaProducer
import datetime
import time
from functools import lru_cache
import sys
from typing import List
import pytz

GENERAL_STARDOG = "http://10.2.33.230:31869"
USERNAME_STARDOG = "admin"
PASSWORD_STARDOG = "admin"
GENERAL_DB = "demo_renson"
ANOMALY_DB = "demo_renson_anomaly"
LIMIT_LAST_ONES = 20
conn_details = {
  'endpoint': GENERAL_STARDOG,
  'username': USERNAME_STARDOG,
  'password': PASSWORD_STARDOG,
}

ANOMALY_TOPIC = os.environ.get('SEMANTIC_ANOMALIES_TOPIC')
KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')


class AnomalyMatch(object):
    def __init__(self, matching_anomaly_id, match_score):
        self.anomaly_id = matching_anomaly_id
        self.score = float(match_score)

    def __repr__(self) -> str:
        return "<AnomalyMatch: {}>".format(str(self.__dict__))


class AnomalyDetail(object):
    def __init__(self, thing_id, metric_id, from_ts, to_ts) -> None:
        self.thing_id = thing_id
        self.metric_id = metric_id
        self.from_ms = int(from_ts)
        self.to_ms = int(to_ts)

    def __repr__(self) -> str:
        return "<{}: {}>".format(type(self).__name__, str(self.__dict__))


class Anomaly(object):
    def __init__(self, anomaly_uri: str, detector_details: dict, types: List[str], description: str,
                 details: [AnomalyDetail], match: AnomalyMatch=None, detection_time=None) -> None:
        self.uri = anomaly_uri
        self.detector_details = detector_details
        self.types = types
        self.description = description
        self.details = details
        self.match = match
        self.detection_time = detection_time or datetime.datetime.now(pytz.utc).isoformat()

    def __repr__(self) -> str:
        return "<{}: {}>".format(type(self).__name__, str(self.__dict__))


def filter_message(nt_message):
    if 'sensor.temperature' in nt_message:
        return True, 'sensor.temperature'
    return False, None


def _add_differences(message, conn):
    conn.begin()
    obs = list(message[1])
    diff = ""
    for i in range(0, len(obs)):
        for j in range(i, len(obs)):
            if i != j:
                diff += obs[i] + " <http://www.w3.org/2002/07/owl#differentFrom> "+obs[j] + ". "+'\n'

    conn.add(stardog.content.Raw(diff, content_type="application/n-triples"))
    conn.add(stardog.content.Raw(message[0], content_type="application/n-triples"))
    conn.commit()
    return diff


def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    log.error('I am an errback', exc_info=excp)
    # handle exception


def kafka_anomaly_encoder(anomaly: Anomaly) -> bytes:
    if not isinstance(anomaly, Anomaly):
        raise RuntimeError("Can only encode anomaly instances, but received: " + repr(anomaly))

    json_str = anomaly_to_json_string(anomaly)
    return json_str.encode("utf-8")


def anomaly_to_json_string(anomaly: Anomaly, indent=None) -> str:
    parts = []
    for part in anomaly.details:
        parts.append({
            "thing": part.thing_id,
            "property": part.metric_id,
            "from": part.from_ms,
            "to": part.to_ms
        })

    content = {
        "id": anomaly.uri,
        "update": False,
        "generatedBy": anomaly.detector_details,
        "timestamp": anomaly.detection_time,
        "anomaly": {
            "type": list(anomaly.types),
            "description": anomaly.description,
            "parts": parts
        }
    }

    if anomaly.match:
        content['matches'] = {
            "id": anomaly.match.anomaly_id,
            "similarity": anomaly.match.score
        }

    return json.dumps(content, indent=indent)


def report_anomaly(ans, box, conn):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        value_serializer=kafka_anomaly_encoder,
    )
    #print("report temp high")
    values = [(ans[0], ans[1])]
    transaction = generate_json("KnownPatternAnomaly", "temp_raised_high", box, 'sensor.temperature', values, "temperature rasied too fast")
    #print(transaction)
    producer.send(ANOMALY_TOPIC, value=transaction)#.add_callback(on_send_success).add_errback(on_send_error)
    producer.flush()


def generate_json(anomaly_type,description, room, metric, values, explanation):
    SFD_DETECTOR_DETAILS = {
        "id": "http://example.com/semantic-fault-detector/",
        "algo": "SReasoner",
        "version": 1
    }

    fr = int(values[0][0].replace('>', '').split('/')[-1])
    to = int(values[0][1].replace('>', '').split('/')[-1])
    uri = f"http://example.com/semantic-fault-detector/reasoner/{room}/{metric}/{fr}/{to}"
    detail = AnomalyDetail(room, metric, fr, to)
    anomaly = Anomaly(uri, SFD_DETECTOR_DETAILS, ["Anomaly", anomaly_type], description, [detail])
    return anomaly


def reason(conn, message_group, db_i, box, roomtype):
        b = time.time()
        for message in message_group:
            # make sure individuals are different from each other
            diff = _add_differences(message, conn)
            try:
                conn.begin(reasoning=True)
                res = conn.select("""
                    ASK {
                        ?anomaly1 <http://example.com/temp_raised_high> ?type1 .
                    }
                """, reasoning=True)
                if res['boolean']:
                    report_anomaly(message[1], box, conn)

            except Exception as e:
                print(e)

            conn.rollback()
            conn.begin()
            conn.remove(stardog.content.Raw(diff, content_type="application/n-triples"))
            conn.remove(stardog.content.Raw(message[0], content_type="application/n-triples"))
            conn.commit()

        print(db_i, roomtype, (time.time()-b)/len(message_group), )
