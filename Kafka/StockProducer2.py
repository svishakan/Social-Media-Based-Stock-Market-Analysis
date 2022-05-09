import pprint
import sqlite3
import json
import time
from kafka import KafkaProducer
import os

def json_serializer(data):
    """Returns a JSON serialized dump of the given data."""

    return json.dumps(data).encode("utf-8")


def get_cursors(connection, categories):
    """Creates and returns a set of cursors for each category present in the stocks table."""

    cursors = []

    for category in categories:
        cursor = connection.cursor()
        #query = "SELECT * FROM stock_data WHERE category='{}'".format(category)
        query = "SELECT * FROM stock_data WHERE ticker='TSLA'" #for only TSLA tickers
        cursor.execute(query)
        cursors.append(cursor)

    return cursors


if __name__ == "__main__":
    batch_size, timeout = 100, 1

    print("-----Stock Data Producer Stream-----")

    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=json_serializer
    )

    print("Kafka Producer started.")

    try:
        connection = sqlite3.connect(os.path.join(os.path.dirname(__file__),f"../Database/fypdb.sqlite"))
        print("Connected to FYPDB Database.")

        # categories = ["Tech", "Gaming", "EVs", "Oil", "Crypto"]
        categories = ["EVs"]

        cursors = get_cursors(connection, categories)

        empty_counts = set()

        while True:
            for category, cursor in zip(categories, cursors):
                category = category.lower() + "-stocks"

                records = cursor.fetchmany(batch_size)

                if not records:
                    empty_counts.add(category)
                    #cursor.close()
                
                
                for record in records:
                    data = dict()
                    data['category'] = record[0]
                    data['ticker'] = record[1]
                    data['stockDate'] = record[2]
                    data['open'] = record[3]
                    data['close'] = record[4]
                    
                    
                    producer.send(category, json.dumps(data)) #topic: "category"-stocks
                    pprint.pprint(json.dumps(data))
            
                if len(empty_counts) == len(categories):   #break the producer if all stocks are sent
                    break
                
            time.sleep(timeout) #wait for timeout after sending a batch of data

    
    except sqlite3.Error as error:
        print("Failed to connect to FYPDB Database.")

    
    finally:
        if connection:
            connection.close()
            print("Disconnected from FYPDB Database.")
