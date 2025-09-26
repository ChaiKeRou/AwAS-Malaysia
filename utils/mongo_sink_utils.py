"""
This module provides utility functions to write data to MongoDB from PySpark
DataFrames.
"""

import time
from datetime import datetime
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError
from pyspark.sql import DataFrame, Row

def generate_average_violation(data: Row) -> dict:
    """
    Generate the average speed violation data to be written to MongoDB.

    Args:
        data (Row): A Row object containing the data for the violation.

    Returns:
        dict: A dictionary containing the violation data.
    """
    violation = {}
    violation["camera_id_start"] = data["camera_id_start"]
    violation["camera_id_end"] = data["camera_id_end"]
    violation["timestamp_start"] = data["timestamp_start"]
    violation["timestamp_end"] = data["timestamp_end"]
    violation["speed_reading"] = data["average_speed"]
    violation["type"] = "average"
    violation["updatedAt"] = datetime.utcnow()

    return violation


def generate_instant_violation(data: Row) -> dict:
    """
    Generate the instant speed violation data to be written to MongoDB.

    Args:
        data (Row): A Row object containing the data for the violation.

    Returns:
        dict: A dictionary containing the violation data.
    """
    violation = {}
    violation["camera_id_end"] = data["camera_id"]
    violation["timestamp_end"] = data["timestamp"]
    violation["speed_reading"] = data["speed_reading"]
    violation["type"] = "instant"
    violation["updatedAt"] = datetime.utcnow()

    return violation


def write_dropped_pairs(batch_df: DataFrame, _: int, host_ip: str) -> None:
    """
    Write the dropped pairs to MongoDB in the foreachbatch sink.
    
    Args:
        batch_df (DataFrame): The DataFrame containing the dropped pairs data.
        _ (int): The batch ID, not used in this function.
        host_ip (str): The IP address of the MongoDB host.
        
    Returns:
        None
    """
    if batch_df.isEmpty():
        return

    # Connect to MongoDB client
    client = MongoClient(host_ip, 27017)
    db = client.fit3182_a2

    # Retry and delay setup
    retries = 3
    delay = 2

    records = batch_df.collect()
    operations = []
    for data in records:

        if not data["event_id_end"]:
            event_id = data["event_id_start"]
            camera_id = data["camera_id_start"]
            timestamp = data["timestamp_start"]
            car_plate = data["car_plate_start"]
        else:
            event_id = data["event_id_end"]
            camera_id = data["camera_id_end"]
            timestamp = data["timestamp_end"]
            car_plate = data["car_plate_end"]

        operation = UpdateOne(
            {"_id": event_id},
            {
                "$set": {
                    "car_plate": car_plate,
                    "camera_id": camera_id,
                    "timestamp": timestamp,
                }
            },
            upsert=True,
        )

        operations.append(operation)

    for attempt in range(retries):
        try:
            if operations:
                db["dropped_pairs"].bulk_write(operations, ordered=False)
            break
        except PyMongoError as e:
            print(f"[Attempt {attempt}] Mongo write failed: {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                print("Write failed after retries")


def write_violations_to_mongo(
    batch_df: DataFrame, _: int, host_ip: int, violation_type: str
):
    """
    Write the violations to MongoDB in the foreachbatch sink.
    
    Args:
        batch_df (DataFrame): The DataFrame containing the violations data.
        _ (int): The batch ID, not used in this function.
        host_ip (str): The IP address of the MongoDB host.
        violation_type (str): The type of violation, "average" or "instant".
        
    Returns:
        None
    """
    if batch_df.isEmpty():
        return

    # connect to MongoDB client
    client = MongoClient(host_ip, 27017)
    db = client.fit3182_a2

    # retry and delay setup
    retries = 3
    delay = 2

    if violation_type == "average":
        method = generate_average_violation
    else:
        method = generate_instant_violation

    records = batch_df.collect()
    operations = []
    for data in records:
        if violation_type == "average":
            date = data["timestamp_end"].replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        else:
            date = data["timestamp"].replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        violation = method(data)

        operation = UpdateOne(
            {"car_plate": data["car_plate"], "date": date},
            {
                "$set": {"updateAt": datetime.utcnow()},
                "$addToSet": {"violations": violation},
            },
            upsert=True,
        )

        operations.append(operation)

    for attempt in range(retries):
        try:
            if operations:
                db["violation"].bulk_write(operations, ordered=False)
            break
        except PyMongoError as e:
            print(f"[Attempt {attempt}] Mongo write failed: {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                print("Write failed after retries")
