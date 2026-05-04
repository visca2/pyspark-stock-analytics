"""Clean raw kafka stream."""

from pyspark.sql.functions import col, to_timestamp


def clean_stream(raw_df):
    """Clean raw kafka stream."""

    clean_df = (
        raw_df
        .filter("price > 0 AND volume > 0")
        .withColumn("event_time", to_timestamp(col("timestamp") / 1000))
        .withWatermark("event_time", "1 minute")
    )

    return clean_df
