"""
This module provides utility functions to work with Kafka producers.

Functions:
- start_producer: Starts a Kafka producer to send messages from a CSV file.
- publish_message: Publishes a message to a Kafka topic.

Author: Cheng Sheng Jie, Chai Ke Rou
Date: 2025-05-25
"""

# Required libraries
from time import sleep
from json import dumps
import datetime as dt
from typing import Dict, Any, Optional
import pandas as pd
from kafka3 import KafkaProducer
from kafka3.errors import KafkaError


def publish_message(
    producer: KafkaProducer,
    topic: str,
    key: str,
    data: Dict,
    header: Dict[str, Any],
) -> None:
    """ "
    Publish a message to a Kafka topic with a key and headers.

    Args:
        producer (KafkaProducer): Kafka producer instance
        topic (str): The Kafka topic to publish
        key (str): The key for the message
        data (Dict): The data to be sent as the message value
        header (Dict[str, Any]): Metadata to be included with the message

    Raises:
        Exception: If there is an error while publishing the message

    Returns:
        None
    """
    try:
        key_bytes = bytes(key, encoding="utf-8")
        header_bytes = [(k, str(v).encode("utf-8")) for k, v in header.items()]
        producer.send(topic, key=key_bytes, value=data, headers=header_bytes)
        producer.flush()
        print(
            f"Data: {data} Header: {header}"
        )
    except KafkaError as e:
        print("Exception in publishing message.")
        print(str(e))


def connect_kafka_producer(host_ip: str) -> Optional[KafkaProducer]:
    """
    Connect to a Kafka producer.

    Args:
        host_ip (str): The IP address of the host.

    Raises:
        KafkaError: If there is an error while connecting to the Kafka producer.

    Returns:
        KafkaProducer: The connected Kafka producer instance.
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=[f"{host_ip}:9092"],
            value_serializer=lambda x: dumps(x).encode("ascii"),
            api_version=(0, 11),  # header enabled in Kafka 0.11 and above
        )
        return producer
    except KafkaError as ex:
        print("Exception while connecting Kafka.")
        print(str(ex))
        return None


def start_producer(
    host_ip: str, path: str, interval: int, topic: str, header: dict
) -> None:
    """
    Start a Kafka producer to send messages from a CSV file in batches.
    This function reads the csv file in the provided path, groups the data by
    the batch_id, and publishes each batch of messages with its header to the
    provided Kafka topic within a specified intervals.

    Args:
        host_ip (str): The IP address of the Kafka host.
        path (str): The path to the CSV file containing the data.
        interval (int): The interval in seconds between publishing messages.
        topic (str): The Kafka topic to publish messages to.
        header (dict): Metadata to be included with each message.

    Raises:
        KafkaError: error while connecting to the Kafka producer.

    Returns:
        None
    """
    event_df = pd.read_csv(path)
    grouped_event = event_df.groupby("batch_id")

    print("Publishing records...")

    producer = connect_kafka_producer(host_ip)

    for batch_id, data in grouped_event:

        print(f"\nPublishing batch id {batch_id}:")

        for _, row in data.iterrows():

            event = row.to_dict()
            event_header = dict(header)
            event_header["processing_timestamp"] = dt.datetime.now(
                dt.timezone.utc
            ).isoformat()

            publish_message(producer, topic, "jsondata", event, event_header)

        sleep(interval)  # await for the specified interval
