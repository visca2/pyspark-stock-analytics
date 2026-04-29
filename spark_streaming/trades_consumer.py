import json
import os
from pathlib import Path

import yaml
from box import Box
from dotenv import load_dotenv
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col
from spark_config import build_spark_session


def main():
    project_root = Path(__file__).resolve().parent.parent
    consumer_dir = Path(__file__).resolve().parent

    # Load environment configuration
    load_dotenv(consumer_dir / ".env")

    # Load YAML application config
    with open(consumer_dir / "config.yaml", "r", encoding="utf-8") as f:
        config_dict = yaml.safe_load(f)

    config = Box(config_dict)
    kafka_topics = config.kafka.topic
    subscribe_topics = (
        ",".join(kafka_topics) if isinstance(kafka_topics, list) else kafka_topics
    )
    spark, spark_paths = build_spark_session("TradesDataAnalysis", project_root)

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
        .option("subscribe", subscribe_topics)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Load the Avro schema used by the producer.
    with open(project_root / "avro" / "trade.avsc", "r", encoding="utf-8") as f:
        schema_dict = json.load(f)
    schema_json = json.dumps(schema_dict)
    parsed = (
        df.select(from_avro(col("value"), schema_json).alias("data"))
        .where(col("data").isNotNull())
        .select("data.*")
    )

    query = (
        parsed.writeStream.format("console")
        .option("truncate", "false")
        .option("checkpointLocation", spark_paths["checkpoint_dir"].as_posix())
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
