"""
This module provides utility functions to read and process streams from Kafka
using PySpark for the streaming application.

It includes functions to read a strea, extreact headers, and parse messages.
"""

from typing import Dict
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import expr, col, from_json, decode, to_timestamp
from pyspark.sql.types import StructType


def parse_header_value(key: str, value_type: str) -> Column:
    """
    Parses the value of a Kafka header based on its type.

    Args:
        key (str): The key of the header.
        value_type (str): The type of the value.

    Returns:
        Union[str, int, TimestampType]: The parsed value of the header.
    """
    encoded = expr(f"filter(headers, h -> h.key = '{key}')[0].value")
    decoded = decode(encoded, "utf-8")  # decode to string
    if value_type == "int":
        return decoded.cast("int")
    if value_type == "timestamp":
        return to_timestamp(decoded)
    return decoded


def read_stream(spark: SparkSession, topic: str, host_ip: str) -> DataFrame:
    """
    Reads a stream from a Kafka topic.

    Args:
        topic (str): The Kafka topic to read.
        host_ip (str): The IP address of the Kafka broker.

    Returns:
        DataFrame: A Spark DataFrame representing the stream from the
        Kafka topic.
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", f"{host_ip}:9092")
        .option("subscribe", topic)
        .option("includeHeaders", "true")
        .option("startingOffsets", "latest")
        .load()
    )


def get_kafka_header(stream: DataFrame, headers: Dict) -> DataFrame:
    """
    Extracts and parse Kafka headers from the stream.

    Args:
        stream (DataFrame): The input stream DataFrame.
        headers (Dict): key and value type of the headers to extract.

    Returns:
        DataFrame: A DataFrame with the parsed headers added as columns.
    """
    for key, value_type in headers.items():
        stream = stream.withColumn(key, parse_header_value(key, value_type))

    return stream


def get_kafka_message(stream: DataFrame, schema: StructType) -> DataFrame:
    """
    Extracts and parses the Kafka message from the stream.
    The kafka message is expected to be in JSON format and is parsed according
    to the provided schema. The headers are also selected as the columns.

    Args:
        stream (DataFrame): The input stream DataFrame.
        schema (StructType): The schema to parse the Kafka message.

    Returns:
        DataFrame: A DataFrame with the parsed message and additional columns.
    """
    return stream.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        "producer_id",
        "speed_limit",
        "dist_next",
        "processing_timestamp",
    ).select(
        "data.*",  # used to flatten the data, else it will in nested format
        "producer_id",
        "speed_limit",
        "dist_next",
        "processing_timestamp",
    )
