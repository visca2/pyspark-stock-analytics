"""Main Kafka producer."""


import os
from pathlib import Path

from kafka_producer.finnhub_websocket import build_finnhub_websocket
from kafka_producer.utils.config_helper import (
    build_producer,
    load_config,
    parse_trade_kafka_schema,
)


def main():
    """Entry point."""
    project_root = Path.cwd()

    # Load configuration and set environment variables
    config = load_config(project_root)

    # Setup and create the Kafka producer and parse the 'trade' Avro schemas
    producer = build_producer(config.kafka.client_id, os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
    trade_parsed_schema = parse_trade_kafka_schema(project_root)

    finnhub_ws = build_finnhub_websocket(
        producer,
        config.kafka.topic,
        trade_parsed_schema,
        config.finnhub.symbols
    )

    try:
        finnhub_ws.run_forever()
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
