"""Trades consumer"""

import os
from pathlib import Path

from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col
from spark_config import build_spark_session
from spark_streaming.utils.config_helper import load_config, parse_trade_kafka_schema_json


def main():
    """Entry point."""
    project_root = Path.cwd()

    # Load configuration and set environment variables
    config = load_config(project_root)

    trade_schema_json = parse_trade_kafka_schema_json(project_root)

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

    parsed = (
        df.select(from_avro(col("value"), trade_schema_json).alias("data"))
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
