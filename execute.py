import datetime
import json
import logging
import os
import sys
import time
import zlib
import argparse
from logging.handlers import RotatingFileHandler
from threading import Thread

from confluent_kafka import Consumer, TopicPartition


def namer(name):
    return name + ".gz"


def rotator(source, destination):
    with open(source, "rb") as sf:
        data = sf.read()
        compressed = zlib.compress(data, 9)
        with open(destination, "wb") as df:
            df.write(compressed)

    os.remove(source)


"""
Below section creates a logger for the script to print its current state e.g., to what extent script has completed 
processing etc.
These logs are rolled over at 100 MB and only 1 backup is retained
"""
script_logger = logging.getLogger('script-logger')
script_logger_handler = RotatingFileHandler("helper-script.log", maxBytes=100000000, backupCount=1)
script_logger_handler.rotator = rotator
script_logger_handler.namer = namer
script_logger.addHandler(script_logger_handler)
script_logger.setLevel(logging.DEBUG)

"""
Below section creates a logger to print the SnappyFlow records
These files are rolled over at 5 Gb and ideally all backups should be retained
When file is rotated older file is compressed
"""
record_logger = logging.getLogger('record-logger')
record_logger_handler = RotatingFileHandler("sf-records.log", maxBytes=5000000000, backupCount=100000)
record_logger_handler.rotator = rotator
record_logger_handler.namer = namer
record_logger.addHandler(record_logger_handler)
record_logger.setLevel(logging.INFO)


def send_record_to_op_stream(record_value):

    if args.output_stream == "console":
        print(json.dumps(record_value))
    else:
        record_logger.info(json.dumps(record_value))


def process_message_and_output_to_stream(record_value):

    record_project = record_value.get('_tag_projectName')
    record_app = record_value.get('_tag_appName')
    record_time = record_value.get('time')
    record_plugin = record_value.get('_plugin')
    record_doc_type = record_value.get('_documentType')

    if args.invalid_messages_only is True:

        if (record_project is None or not isinstance(record_project, str) or record_project.strip() == "") or \
                (record_app is None or not isinstance(record_app, str) or record_app.strip() == "") or \
                (record_plugin is None or not isinstance(record_plugin, str) or record_plugin.strip() == "") or \
                (record_doc_type is None or not isinstance(record_doc_type, str) or record_doc_type.strip() == "") or \
                (record_time is None or not isinstance(record_time, int)):

            send_record_to_op_stream(record_value)

    elif (args.project_name is not None and record_project != args.project_name) or \
            (args.app_name is not None and record_app != args.app_name) or \
            (args.plugin is not None and record_plugin != args.plugin) or \
            (args.doc_type is not None and record_doc_type != args.doc_type):

        return

    else:

        send_record_to_op_stream(record_value)


def start_consuming(topic, partition):

    time_counter = int(time.time())

    consumer = Consumer({
        'bootstrap.servers': args.bootstrap_server,
        'group.id': 'debug',
        'session.timeout.ms': 10000,
        'enable.auto.commit': False,
        'auto.offset.reset': 'earliest'
    })

    # Below 2 lines are responsible to set the proper offset based on START_TIME as specified to script
    topic_partn = TopicPartition(topic=topic, partition=partition, offset=args.start_time)
    topic_partn = consumer.offsets_for_times([topic_partn])[0]

    script_logger.debug("Assigning topic-partition-offset object {}".format(topic_partn))
    consumer.assign([topic_partn])

    total_count = 0

    try:

        while True:

            msg = consumer.poll(2)

            if msg is None:
                script_logger.debug("Waiting for messages")
                continue

            if msg.error():
                script_logger.debug('Error while polling: {}'.format(msg.error()))
                continue

            total_count += 1

            record_epoch_millis = msg.timestamp()[1]
            if record_epoch_millis > args.end_time:
                sys.exit(0)

            record_value = json.loads(msg.value())
            process_message_and_output_to_stream(record_value)

            current_time = int(time.time())
            if (current_time - time_counter) < 120:
                continue

            script_logger.debug("Processed {} records so far in {}. Pointer is at {} UTC"
                                .format(total_count, topic_partn,
                                        datetime.datetime.utcfromtimestamp(record_epoch_millis)
                                        .strftime('%A, %B %-d, %Y %H:%M:%S')))

            time_counter = int(time.time())

    except KeyboardInterrupt:

        pass

    finally:

        consumer.close()


def get_cluster_metadata():

    consumer = Consumer({
        'bootstrap.servers': args.bootstrap_server,
        'group.id': 'debug',
        'session.timeout.ms': 10000,
        'enable.auto.commit': False,
        'auto.offset.reset': 'earliest'
    })

    metadata = consumer.list_topics()
    consumer.close()

    return metadata


if __name__ == '__main__':

    script_start_time = int(time.time())

    parser = \
        argparse.ArgumentParser(prog='get-snp-messages-from-kafka',
                                description='It helps to consume messages from Kafka topics '
                                            '(It also helps to determine if message is invalid)',
                                epilog='This uses a consumer group called "debug"')

    parser.add_argument("--bootstrap-server", type=str, required=True)

    parser.add_argument('--topics', nargs='+', required=True)

    parser.add_argument('--output-stream', choices=["log-file", "console"], required=True,
                        help="Determines where the messages will be written")

    parser.add_argument('--start-time', type=int, default=0, help="Epoch millisecond time", required=False)

    parser.add_argument('--end-time', type=int, default=int(time.time() * 1000), help="Epoch millisecond time",
                        required=False)

    parser.add_argument('--invalid-messages-only', action='store_true', default=False, required=False,
                        help="""
                        If set to yes, this script will print the following types of messages
                        1) If _tag_projectName/_tag_appName/_plugin/_documentType is None/Empty/Not-a-string
                        2) If time is None/Not-an-integer
                        """)

    parser.add_argument('--project-name', type=str, required=False, default=None)

    parser.add_argument('--app-name', type=str, required=False, default=None)

    parser.add_argument('--plugin', type=str, required=False, default=None)

    parser.add_argument('--doc-type', type=str, required=False, default=None)

    args = parser.parse_args()

    cluster_metadata = get_cluster_metadata()
    consumer_threads = list()

    for topic_name in args.topics:
        topic_desc = cluster_metadata.topics[topic_name]
        for partn in topic_desc.partitions.keys():
            consumer_threads.append(Thread(target=start_consuming, args=(topic_name, partn)))

    for consumer_thread in consumer_threads:
        consumer_thread.start()

    for consumer_thread in consumer_threads:
        consumer_thread.join()

    script_logger.debug("Completed in {} seconds !!!".format(int(time.time() - script_start_time)))
