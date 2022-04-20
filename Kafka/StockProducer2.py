from pprint import pprint
import sqlite3
import json
import time
from kafka import KafkaProducer
import os

def json_serializer(data):
    """Returns a JSON serialized dump of the given data."""

    return json.dumps(data).encode("utf-8")


def get_cursors(connection):
    """Creates and returns a set of cursors for each category present in the stocks table."""

    cursors = []
    categories = ["Tech", "Gaming", "EVS", "Oil", "Crypto"]

    for category in categories:
        cursor = connection.cursor()
        query = "SELECT * FROM stocks WHERE category='{}'".format(category) 
        cursor.execute(query)
        cursors.append(cursor)

    return cursors


if __name__ == "__main__":
    batch_size, timeout = 10, 30

    print("-----Stock Data Producer Stream-----")

    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=json_serializer
    )

    print("Kafka Producer started.")

    try:
        connection = sqlite3.connect(os.path.dirname(__file__),f"../Database/fypdb.db")
        print("Connected to FYPDB Database.")

        categories = ["Tech", "Gaming", "EVS", "Oil", "Crypto"]

        cursors = get_cursors(connection)

        empty_counts = 0

        while True:
            for category, cursor in zip(categories, cursors):
                category = category.lower() + "-stocks"

                records = cursor.fetchmany(batch_size)

                if not records:
                    empty_counts += 1
                    #cursor.close()
                
                    if empty_counts == 5:   #break the producer if all stocks are sent
                        break
                
                for record in records:
                    producer.send(category, record) #topic: "category"-stocks
                    print(record)

            time.sleep(timeout) #wait for timeout after sending a batch of data

    
    except sqlite3.Error as error:
        print("Failed to connect to FYPDB Database.")

    
    finally:
        if connection:
            connection.close()
            print("Disconnected from FYPDB Database.")
