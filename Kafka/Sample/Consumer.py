import json
from kafka import KafkaConsumer


if __name__ == "__main__":
    consumer = KafkaConsumer(
        "sample",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        group_id="consumer-group-1"
    )

    #auto_offset_reset = earliest => consume from starting point

    print("Consumer started...")

    for msg in consumer:
        print("Registered User: {}".format(json.loads(msg.value)))
        #de-serialize the data using json.loads()

    
