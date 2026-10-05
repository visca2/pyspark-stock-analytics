from datetime import datetime, timezone

import pytest
from pyspark.sql.functions import col, from_json, struct, to_json

from streaming.read_ohlc_stream import OHLC_SCHEMA

pytestmark = pytest.mark.spark


def _utc(year, month, day, hour, minute, second=0):
    """Naive datetime matching what collect() returns for a UTC instant.

    Spark's driver-side collect() converts TimestampType values to Python
    datetimes using the host's local timezone, independent of
    spark.sql.session.timeZone. Build the expected value the same way so
    assertions aren't tied to the machine's timezone.
    """
    instant = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
    return datetime.fromtimestamp(instant.timestamp())


def test_ohlc_json_round_trips_through_the_kafka_payload_schema(spark):
    source = spark.sql(
        """
        SELECT
            'AAPL' AS symbol,
            TIMESTAMP '2024-06-28 10:00:00' AS window_start,
            TIMESTAMP '2024-06-28 10:01:00' AS window_end,
            189.10D AS open,
            189.80D AS high,
            188.95D AS low,
            189.42D AS close,
            4500.0D AS volume
        """
    )

    encoded = source.select(
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
    decoded = encoded.select(from_json(col("value"), OHLC_SCHEMA).alias("data")).select(
        "data.*"
    )

    row = decoded.collect()[0]
    assert encoded.collect()[0].key == "AAPL"
    assert row.symbol == "AAPL"
    assert row.window_start == _utc(2024, 6, 28, 10, 0, 0)
    assert row.window_end == _utc(2024, 6, 28, 10, 1, 0)
    assert row.open == pytest.approx(189.10)
    assert row.high == pytest.approx(189.80)
    assert row.low == pytest.approx(188.95)
    assert row.close == pytest.approx(189.42)
    assert row.volume == pytest.approx(4500.0)
