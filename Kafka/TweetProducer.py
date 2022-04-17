import sqlite3
import json
import time
from kafka import KafkaProducer


def json_serializer(data):
    """Returns a JSON serialized dump of the given data."""

    return json.dumps(data).encode("utf-8")


if __name__ == "__main__":
    batch_size, timeout = 10, 10

    print("-----Tweet Data Producer Stream-----")

    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=json_serializer
    )

    print("Kafka Producer started.")

    try:
        connection = sqlite3.connect("../Database/fypdb.db")
        print("Connected to FYPDB Database.")
        cursor = connection.cursor()
        query = "SELECT * FROM tweets"

        cursor.execute(query)

        while True:
            records = cursor.fetchmany(batch_size)

            if not records: #no more tweets to read
                break

            for record in records:
                producer.send("tweets", record) #topic: "tweets"
                print(record)

            time.sleep(timeout)  #wait for timeout before next send

        cursor.close()

    
    except sqlite3.Error as error:
        print("Failed to connect to FYPDB Database.")

    
    finally:
        if connection:
            connection.close()
            print("Disconnected from FYPDB Database.")
