"""
This module provides utility functions to process streams from Kafka using
PySpark to detect the average speed violations.
"""

import math
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr


def calculate_time_boundary(distance: int, speed_limit: int) -> int:
    """
    Helper function to calculate the time boundary in seconds based on the
    distance and speed limit

    Args:
        distance (int): The distance in km.
        speed_limit (int): The speed limit in km/h.

    Returns:
        int: The time boundary in seconds.
    """
    hour = distance / speed_limit
    seconds = math.ceil(hour * 3600)
    return seconds


def stream_stream_join_event(
    stream_start: DataFrame, stream_end: DataFrame, window_size: int
) -> DataFrame:
    """
    Online join of two streams to find the events where a car's passing through
    two cameras. Full outer join is used to ensure that we capture all events
    The window size is used to define the time boundary for the join.

    Args:
        stream_start (DataFrame): The starting camera stream DataFrame
        stream_end (DataFrame): The ending stream DataFrame
        window_size (int): The time window size in seconds for the join.

    Returns:
        DataFrame: A DataFrame containing the joined events with relevant
        information.
    """
    return (
        stream_start.alias("s")
        .join(
            stream_end.alias("e"),
            expr(
                f"""
            s.car_plate = e.car_plate AND
            s.timestamp BETWEEN e.timestamp - interval {window_size} seconds AND
            e.timestamp
        """
            ),
            "full_outer",
        )
        .select(
            col("s.car_plate").alias("car_plate_start"),
            col("e.car_plate").alias("car_plate_end"),
            col("s.timestamp").alias("timestamp_start"),
            col("e.timestamp").alias("timestamp_end"),
            col("s.camera_id").alias("camera_id_start"),
            col("e.camera_id").alias("camera_id_end"),
            col("s.event_id").alias("event_id_start"),
            col("e.event_id").alias("event_id_end"),
            "s.dist_next",
            "e.speed_limit",
        )
    )

def get_matched_stream(stream: DataFrame) -> DataFrame:
    """
    Extrect the matched events and select the relevant columns for
    the violation detection.

    Args:
        stream (DataFrame): The joined DataFrame containing events from both
        streams.

    Returns:
        DataFrame: A DataFrame containing the matched events with relevant
        columns for further processing.
    """
    match = stream.filter(
        col("timestamp_start").isNotNull() & col("timestamp_end").isNotNull()
    ).select(
        col("car_plate_start").alias("car_plate"),
        "timestamp_start",
        "timestamp_end",
        "camera_id_start",
        "camera_id_end",
        "dist_next",
        "speed_limit",
    )
    return match


def get_violated_average_stream(stream: DataFrame) -> DataFrame:
    """
    Retrieve the stream of events where the average speed is violated.

    Args:
        stream (DataFrame): The DataFrame containing the matched events with
        relevant columns.

    Returns:
        DataFrame: A DataFrame containing the events where the average speed
        is violated, along with the calculated average speed.
    """
    violated_average_stream = (
        stream.withColumn(
            "duration",
            col("timestamp_end").cast("double")
            - col("timestamp_start").cast("double"),
        )
        .filter(
            col("duration") <= (3600 * col("dist_next")) / col("speed_limit")
        )
        .withColumn("average_speed", 1 / (col("duration") / 3600))
    )

    return violated_average_stream
