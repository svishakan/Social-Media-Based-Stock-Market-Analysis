import json
from kafka import KafkaConsumer

if __name__ == "__main__":
    consumer = KafkaConsumer(
        "stocks",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        group_id="stocks-group-1"
    )

    print("Kafka Consumer started...")

    for msg in consumer:
        print("{}".format(json.loads(msg.value)))
        #de-serialize the data using json.loads()