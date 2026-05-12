"""Write finalized OHLC candles to Kafka."""

import os

from pyspark.sql.functions import col, struct, to_json


def write_ohlc_stream(ohlc_df, config, checkpoint_location):
    """Write finalized OHLC candles as JSON messages to Kafka."""

    kafka_payload_df = ohlc_df.select(
        col("symbol").alias("key"),
        to_json(
            struct(
                "symbol",
                "window_start",
                "window_end",
                "open",
                "high",
                "low",
                "close",
                "volume",
            )
        ).alias("value"),
    )

    return (
        kafka_payload_df.writeStream.format("kafka")
        .outputMode("append")
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
        .option("topic", config.kafka.ohlc_topic)
        .option("checkpointLocation", checkpoint_location)
        .start()
    )
