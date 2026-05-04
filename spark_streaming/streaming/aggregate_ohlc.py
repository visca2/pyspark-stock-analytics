"""Aggregate clean trades into OHLC candles."""

from pyspark.sql.functions import col, max, max_by, min, min_by, sum, window


def aggregate_ohlc(clean_df):
    """Aggregate trades into 1-minute OHLC candles per symbol."""

    aggregated_df = (
        clean_df.groupBy(window(col("event_time"), "1 minute"), col("symbol"))
        .agg(
            min_by(col("price"), col("event_time")).alias("open"),
            max(col("price")).alias("high"),
            min(col("price")).alias("low"),
            max_by(col("price"), col("event_time")).alias("close"),
            sum(col("volume")).alias("volume"),
        )
        .select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume"),
        )
    )

    return aggregated_df
