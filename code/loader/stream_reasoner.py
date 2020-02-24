import re
import json
import time
import os
from rdflib import Graph
from kafka import KafkaConsumer
from multiprocessing import Manager
from multiprocessing import Process
from datetime import datetime
import task
import stardog

global message_queue, file_base

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
SEMANTIC_EVENTS_TOPIC = os.environ.get('SEMANTIC_EVENTS_TOPIC')
ANOMALY_TOPIC = os.environ.get('SEMANTIC_ANOMALIES_TOPIC')

STARDOG_URL = os.environ.get('STARDOG_URL')
print(STARDOG_URL)
STARDOG_USER = os.environ.get('STARDOG_USER')
STARDOG_PASSWORD = os.environ.get('STARDOG_PASSWORD')

WINDOW_LEN = 2
STEP_SIZE_WINDOW = 1
DATABASES = 20

conn_details = {
  'endpoint': STARDOG_URL,
}


def flatten(l):
    return [item for sublist in l for item in sublist]


def _initialise():
    consumer = KafkaConsumer(
        SEMANTIC_EVENTS_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=lambda value: json.loads(value),
    )
    return consumer


def convert_json_ld(message):
    try:
        rev_m = message.replace('\n', '').replace(' ', '').split(',"@context"')
        if len(rev_m)>1:
            message_a = rev_m[0]+"}"
            context = json.loads('{"@context"'+rev_m[-1].replace('}}', '}') + '}')
            g = Graph().parse(data=message_a, context=context, format='json-ld')
        else:
            g = Graph().parse(data=message, format='json-ld')
    except Exception as e:
        g = Graph().parse(data=message, format='json-ld')
    trans = g.serialize(format='nt').decode()
    return trans


def get_time(msg):
    regex = '"([0-9]+)"\^\^<http://www.w3.org/2001/XMLSchema#dateTime>'
    dates = re.findall(regex, msg)
    return dates[0]


def register_box(box, metric):
    if box not in message_queue:
        message_queue[box] = {}
    if metric not in message_queue[box]:
        message_queue[box][metric] = []
        message_queue[box][metric].append([])


def append_message(message, box, metric, type):
    register_box(box, metric)
    time = get_time(message)

    if len(message_queue[box][metric][-1]) < WINDOW_LEN or WINDOW_LEN == -1:
        message_queue[box][metric][-1].append((message, time))
    else:
        # here: take step_size number of elements from the last window
        # add them to a new window and continue
        # step_size == window_len: nothing is taken into account of prev window
        p_message = message_queue[box][metric][-1][STEP_SIZE_WINDOW:]
        message_queue[box][metric].append([])
        message_queue[box][metric][-1].extend(p_message)
        message_queue[box][metric][-1].append((message, time))


def filter_corresponding_value(time, box, metric):
    while len(message_queue[box][metric]) > 0:
        c_message = message_queue[box][metric].pop(0)
        for message, t in c_message:
            if t == time:
                # add it back, maybe another value in the range is usefull
                message_queue[box][metric].insert(0, c_message)
                return (message, t)
    return None


def room(box):
    metric_t = 'sensor.temperature'
    if len(message_queue[box][metric_t][0]) == WINDOW_LEN:
        return consume(box, 'room')


def flush_group(message_group, box, roomtype):
    db = db_pool.index(True)
    db_pool[db] = False
    p = Process(target=reason_group, args=(message_group, db, box, roomtype))
    p.start()
    return p, db


def reason_group(message_group, db_i, box, roomtype):
    try:
        with stardog.Connection('db'+str(db_i), **conn_details) as conn:
            task.reason(conn, message_group, db_i, box, roomtype)
    except Exception as e:
        print(e)


def consume(box, roomtype):
    message_group = []
    rm_list = {}
    stop = False
    i = 0
    while not stop:
        value = []
        all_obs = []
        for metric in message_queue[box]:
            if len(message_queue[box][metric][i]) == WINDOW_LEN:
                window_values = message_queue[box][metric][i]
                value.extend([transform_time(window_values[j][0], box, metric) for j in [0, -1]])
                all_obs.extend(flatten([v[1] for v in value]))
                if metric not in rm_list:
                    rm_list[metric] = []
                rm_list[metric].append(i)
                if i == len(message_queue[box][metric])-1:
                    stop = True
                    message_queue[box][metric].append([])
                    message_queue[box][metric][-1].extend(window_values[STEP_SIZE_WINDOW:])
            if i == len(message_queue[box][metric])-1:
                stop = True
        message_group.append(('\n'.join([m[0] for m in value]), all_obs))
        i += 1
    for metric in message_queue[box]:
        for index in sorted(rm_list[metric], reverse=True):
                del message_queue[box][metric][index]
    return flush_group(message_group, box, roomtype)


def transform_time(msg, box, metric):
    txt = msg
    #if type(msg) != str:
    #    txt = msg.decode("utf-8")
    regex = '"([0-9]+)"\^\^<http://www.w3.org/2001/XMLSchema#dateTime>'
    dates = re.findall(regex, txt)
    dates_tr = {}
    for d in dates:
        dates_tr[d] = datetime.utcfromtimestamp(int(d)/1000).isoformat()

    obs_uri = []
    for key in dates_tr:
        txt = txt.replace(key+'"^^<http://www.w3.org/2001/XMLSchema#dateTime>', dates_tr[key]+'"^^<http://www.w3.org/2001/XMLSchema#dateTime>')
        observation = '\n <http://example.com/sensor.temperature/observations/'+key+'> <http://IBCNServices.github.io/Folio-Ontology/Folio.owl#hasEpochTime> "'+key + '"^^<http://www.w3.org/2001/XMLSchema#integer> .'
        obs_uri.append('<http://example.com/sensor.temperature/observations/'+key+'>')
        txt += observation
    return txt, obs_uri


def append_rule(message):
    print("new rule received")
    rule_obj = json.loads(message)
    rule = rule_obj['semantic_ruleminer']
    with open("rule_tmp.ttl", "w") as text_file:
        text_file.write(rule)

    for i in range(0, DATABASES):
        with stardog.Connection('db'+str(i), **conn_details) as conn:
            conn.begin()
            conn.add(stardog.content.File('rule_tmp.ttl'))
            conn.commit()


if __name__ == '__main__':

    consumer = _initialise()
    # init time sleep needed
    time.sleep(30)

    manager = Manager()
    db_pool = manager.list([True for x in range(0, DATABASES)])

    # create database pool
    for i in range(0, DATABASES):
        try:
            with stardog.Admin(**conn_details) as admin:
                admin.database('db'+str(i)).drop()
        except:
            print("no database to drop")
        print('db'+str(i))
        with stardog.Admin(**conn_details) as admin:
            admin.new_database('db'+str(i), {'index.type': 'Memory'})
            with stardog.Connection('db'+str(i), **conn_details) as conn:
                conn.begin()
                conn.add(stardog.content.File('rule.ttl'))
                conn.commit()

    message_queue = {}
    proccesses = []
    counter = 0
    start = time.time()
    for message in consumer:
        counter += 1
        plain_message = message.value

        if counter%100==0:
            print("processed: 100 messages in ", time.time()-start)
            start = time.time()

        if "semantic_ruleminer" in plain_message:
            append_rule(plain_message)
        else:
            ok_message, metric = task.filter_message(plain_message)
            if ok_message:
                nt_message = convert_json_ld(plain_message)
                box, type = 1, "room"

                if type is not None:
                    if True not in db_pool:
                        for i in range(max([1, DATABASES//2])):
                            proc, db = proccesses.pop(0)
                            proc.join()
                            db_pool[db] = True

                    append_message(nt_message, box, metric, type)
            ###
                    if type == 'room':
                        r = room(box)
                        if r:
                            proccesses.append(r)
