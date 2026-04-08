from box import Box
from dotenv import load_dotenv
import os
from confluent_kafka import Consumer, KafkaError
import yaml

def main():
    # Load environment configuration
    load_dotenv()

    # Load YAML application config
    with open("config.yaml", "r") as f:
        config_dict = yaml.safe_load(f);

    config = Box(config_dict)

    # Configuration for the consumer
    consumer_conf = {
        'bootstrap.servers': os.getenv("KAFKA_BROKER_ADDRESS"),
        'group.id': config.kafka.group_id,
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe(config.kafka.topic)

    try:
        while True:
            # Poll for new messages (timeout in seconds)
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(f"Reached end of partition {msg.partition()}")
                else:
                    print(f"Error: {msg.error()}")
            else:
                # Successfully received message
                print(f"Received message: {msg.value().decode('utf-8')} from {msg.topic()}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    main()