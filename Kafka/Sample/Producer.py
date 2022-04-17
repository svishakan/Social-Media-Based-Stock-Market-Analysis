import json
import time
from kafka import KafkaProducer
from Kafka.Sample.ProducerData import get_registered_user


def json_serializer(data):
    """Return a JSON serialized dump of the given data."""

    return json.dumps(data).encode("utf-8")

producer = KafkaProducer(bootstrap_servers=["localhost:9092"],
                        value_serializer=json_serializer)


if __name__ == "__main__":
    while True:
        registered_user = get_registered_user()

        producer.send("sample", registered_user) #topic name in Kafka cluster
        print(registered_user)

        time.sleep(5)
