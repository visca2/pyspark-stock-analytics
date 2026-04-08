from box import Box
from dotenv import load_dotenv
import os
from confluent_kafka import Producer
import websocket
import yaml

# Kafka delivery report callback
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def make_on_message_callback(producer, topic, key, callback):
    def on_message(ws, message):
        producer.produce(topic=topic, value=message, key=key, on_delivery=callback)
        producer.poll(0)
        print(message)
    return on_message

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("closed")

def make_on_open_callback(symbols):
    def on_open(ws):
        for symbol in symbols:
            ws.send(f'{{"type":"subscribe","symbol":"{symbol}"}}')
    return on_open

def main():
    # Load environment configuration
    load_dotenv()

    # Load YAML application config
    with open("config.yaml", "r") as f:
        config_dict = yaml.safe_load(f);

    config = Box(config_dict)

    # Set up Kafka producer
    kafka_conf = {
        'bootstrap.servers': os.getenv("KAFKA_BROKER_ADDRESS"),
        'client.id': config.finnhub.kafka.client_id
    }

    producer = Producer(kafka_conf)

    # Get Finnhub API key
    api_key = os.getenv("FINNHUB_API_KEY")
    
    # Create websocket callback functions
    kafka_topic = config.finnhub.kafka.topic
    kafka_key = config.finnhub.kafka.key

    on_message = make_on_message_callback(producer, kafka_topic, kafka_key, delivery_report)

    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={api_key}",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = make_on_open_callback(config.finnhub.symbols)
    try:
        ws.run_forever()
    finally:
        producer.flush()

if __name__ == "__main__":
    main()
