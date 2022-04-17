import json
from kafka import KafkaConsumer

if __name__ == "__main__":
    categories = ["Gaming", "Tech", "EVS", "Oil", "Crypto"]
    topics = []

    for category in categories:
        topic = category.lower() + "-stocks"
        topics.append(topic)
        
    consumer = KafkaConsumer(
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        group_id="stocks-group-1",
        #enable_auto_commit=True,   
        #auto commits reads, otherwise use consumer.commit() for each message
    )

    #Need not create separate consumer for each topic,
    #if necessary it can be done
    consumer.subscribe(topics=topics)

    for msg in consumer:
        print("{}".format(json.loads(msg.value)))
        #de-serialize the data using json.loads()
