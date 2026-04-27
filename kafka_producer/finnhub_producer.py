from box import Box
from dotenv import load_dotenv
from fastavro import schemaless_writer, parse_schema
import io
import json
import os
from pathlib import Path
from confluent_kafka import Producer
import websocket
import yaml

# Kafka delivery report callback
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def to_trade_record(trade):
    return {
        "trade_conditions": trade.get("c"),
        "symbol": trade["s"],
        "price": trade["p"],
        "volume": trade["v"],
        "timestamp": trade["t"],
    }

def make_on_message_callback(producer, topic, key, parsed_schema, callback):
    def on_message(ws, message):
        value = json.loads(message)

        if value.get("type") != "trade":
            return

        for trade in value.get("data", []):
            bytes_io = io.BytesIO()
            schemaless_writer(bytes_io, parsed_schema, to_trade_record(trade))
            binary_data = bytes_io.getvalue()
            producer.produce(topic=topic, value=binary_data, key=key, on_delivery=callback)
            producer.poll(0)
    return on_message

def on_error(ws, error):
    print(error)

def on_close(ws, close_status_code, close_msg):
    print(f"closed: code={close_status_code}, message={close_msg}")

def make_on_open_callback(symbols):
    def on_open(ws):
        for symbol in symbols:
            ws.send(f'{{"type":"subscribe","symbol":"{symbol}"}}')
    return on_open

def main():
    project_root = Path(__file__).resolve().parent.parent
    producer_dir = Path(__file__).resolve().parent

    # Load environment configuration
    load_dotenv(producer_dir / ".env")

    # Load YAML application config
    with open(producer_dir / "config.yaml", "r", encoding="utf-8") as f:
        config_dict = yaml.safe_load(f)

    config = Box(config_dict)

    # Configure fastavro serializer
    with open(project_root / "avro" / "trade.avsc", "r", encoding="utf-8") as f:
        schema_dict = json.load(f)
    parsed_schema = parse_schema(schema_dict)

    # Set up Kafka producer
    kafka_conf = {
        'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        'client.id': config.finnhub.kafka.client_id
    }

    producer = Producer(kafka_conf)

    # Get Finnhub API key
    api_key = os.getenv("FINNHUB_API_KEY")
    
    # Create websocket callback functions
    kafka_topic = config.finnhub.kafka.topic
    kafka_key = config.finnhub.kafka.key

    on_message = make_on_message_callback(producer, kafka_topic, kafka_key, parsed_schema, delivery_report)

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
