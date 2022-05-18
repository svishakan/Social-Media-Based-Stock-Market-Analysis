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
    batch_size, timeout = 100, 1

    print("-----Tweet Data Producer Stream-----")

    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=json_serializer
    )

    print("Kafka Producer started.")

    try:
        #categories = ["Gaming", "Oil", "EVs", "Tech"]
        table_names = ["reduced_tweet_counts"]

        #for category in categories:
        for table in table_names:
            #connection = sqlite3.connect(os.path.join(os.path.dirname(__file__),f"../Database/fypdb-{category}.sqlite"))
            connection = sqlite3.connect(os.path.join(os.path.dirname(__file__),f"../Database/fypdb-Oil.sqlite"))
            #print(f"Connected to FYPDB-{category} Database.")
            print(f"Connected to FYPDB Database.")
            cursor = connection.cursor()
            query = f"SELECT * FROM {table}"
            cursor.execute(query)


            records = cursor.fetchall()
                    
            for record in records:
                data = dict()
                data['category'] = record[0]
                #data['ticker'] = record[1]
                data['tweetDate'] = record[1]
                data['count'] = record[2]
                data['tweet'] = record[3]
                        
                        
                producer.send('tweets-category-topic', json.dumps(data)) #topic: tweets-topic
                pprint.pprint(json.dumps(data))


            time.sleep(10) #Sleep before producing the next category of Tweets  

            cursor.close()
            connection.close()     

    
    except sqlite3.Error as error:
        #print(f"Failed to connect to FYPDB-{category} Database.")
        print(f"Failed to connect to FYPDB Database.")
        exit(0)

    
    finally:
        if connection:
            connection.close()
            #print(f"Disconnected from FYPDB-{category} Database.")
            print(f"Disconnected from FYPDB Database.")
            exit(0)
