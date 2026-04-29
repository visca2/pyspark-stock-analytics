"""Generic config helpers."""


import json
from pathlib import Path

import yaml
from box import Box
from confluent_kafka import Producer
from dotenv import load_dotenv
from fastavro import parse_schema


def load_config(project_root: Path):
    """Load environment variables. Create and return config object."""
    producer_dir = project_root / "kafka_producer"

    # Load environment configuration
    load_dotenv(producer_dir / ".env")

    # Load YAML application config
    with open(producer_dir / "config.yaml", "r", encoding="utf-8") as f:
        config_dict = yaml.safe_load(f)

    config = Box(config_dict)
    return config


def parse_trade_kafka_schema(project_root: Path):
    """Parse and return fastavro 'trade' schema."""
    # Configure fastavro serializer from a local file
    avro_schemas_dir = project_root / "avro"

    with open(avro_schemas_dir / "trade.avsc", "r", encoding="utf-8") as f:
        schema_dict = json.load(f)
    parsed_schema = parse_schema(schema_dict)
    return parsed_schema


def build_producer(kafka_client_id, kafka_bootstrap_server):
    """Build and return Kafka producer."""
    # Set up Kafka producer
    kafka_conf = {
        'bootstrap.servers': kafka_bootstrap_server,
        'client.id': kafka_client_id
    }

    producer = Producer(kafka_conf)
    return producer
