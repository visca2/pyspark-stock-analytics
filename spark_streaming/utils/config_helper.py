"""Generic config helpers."""


import json
from pathlib import Path

import yaml
from box import Box
from dotenv import load_dotenv


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


def parse_trade_kafka_schema_json(project_root: Path):
    """Parse and return fastavro 'trade' schema."""
    # Configure fastavro serializer from a local file
    avro_schemas_dir = project_root / "avro"

    with open(avro_schemas_dir / "trade.avsc", "r", encoding="utf-8") as f:
        schema_dict = json.load(f)

    schema_json = json.dumps(schema_dict)
    return schema_json
