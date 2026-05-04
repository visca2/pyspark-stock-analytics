"""Read and parse raw kafka stream."""

import os
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col


def read_stream(spark, schema_json, config):
    """Read and parse raw kafka stream."""

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
        .option("subscribe", config.kafka.topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        df.select(from_avro(col("value"), schema_json).alias("data"))
        .where(col("data").isNotNull())
        .select("data.*")
    )

    return parsed
