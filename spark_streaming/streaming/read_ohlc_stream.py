"""Read finalized OHLC candles from Kafka."""

import os

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


OHLC_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), False),
        StructField("window_start", TimestampType(), False),
        StructField("window_end", TimestampType(), False),
        StructField("open", DoubleType(), False),
        StructField("high", DoubleType(), False),
        StructField("low", DoubleType(), False),
        StructField("close", DoubleType(), False),
        StructField("volume", DoubleType(), False),
    ]
)


def read_ohlc_stream(spark, config):
    """Read finalized OHLC candles from Kafka and parse JSON payloads."""

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
        .option("subscribe", config.kafka.ohlc_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    return (
        kafka_df.selectExpr("CAST(value AS STRING) AS value")
        .select(from_json(col("value"), OHLC_SCHEMA).alias("data"))
        .where(col("data").isNotNull())
        .select("data.*")
    )
