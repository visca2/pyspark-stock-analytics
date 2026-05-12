"""Generic config helpers."""


import json
from pathlib import Path

import yaml
from box import Box
from dotenv import load_dotenv


def load_config(project_root: Path):
    """Load environment variables. Create and return config object."""
    spark_streaming_dir = project_root / "spark_streaming"
    producer_dir = project_root / "kafka_producer"

    # Load environment configuration
    load_dotenv(producer_dir / ".env")

    # Load YAML application config
    with open(spark_streaming_dir / "config.yaml", "r", encoding="utf-8") as f:
        config_dict = yaml.safe_load(f)

    config = Box(config_dict)
    return config


def parse_avro_schema_json(project_root: Path, schema_name: str):
    """Parse and return an Avro schema by file name without extension."""
    avro_schemas_dir = project_root / "avro"

    with open(avro_schemas_dir / f"{schema_name}.avsc", "r", encoding="utf-8") as f:
        schema_dict = json.load(f)

    return json.dumps(schema_dict)


def parse_trade_kafka_schema_json(project_root: Path):
    """Parse and return the trade Avro schema."""
    return parse_avro_schema_json(project_root, "trade")
