import pprint
import sqlite3
import json
import time
from kafka import KafkaProducer
import os

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
        connection = sqlite3.connect(os.path.join(os.path.dirname(__file__),f"../Database/fypdb.db"))
        print("Connected to FYPDB Database.")
        cursor = connection.cursor()
        query = "SELECT * FROM tweets"

        cursor.execute(query)

        while True:
            records = cursor.fetchmany(batch_size)

            if not records: #no more tweets to read
                break

            for record in records:
                data = dict()
                data['category'] = record[0]
                data['date'] = record[1]
                data['tweet'] = record[2]
                
                # record = ','.join(str(x) for x in record)
                producer.send("tweets", json.dumps(data)) #topic: "tweets"
                #print(record)
                # producer.send("tweets", json.dumps(data)) #topic: "tweets"
                pprint.pprint(json.dumps(data))
                
            time.sleep(timeout)  #wait for timeout before next send

        cursor.close()

    
    except sqlite3.Error as error:
        print("Failed to connect to FYPDB Database.")

    
    finally:
        if connection:
            connection.close()
            print("Disconnected from FYPDB Database.")
