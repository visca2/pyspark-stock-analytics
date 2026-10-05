from datetime import datetime, timezone

import pytest

from streaming.aggregate_ohlc import aggregate_ohlc
from streaming.clean_stream import clean_stream

pytestmark = pytest.mark.spark


def _millis(year, month, day, hour, minute, second):
    dt = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _utc(year, month, day, hour, minute, second=0):
    """Naive datetime matching what collect() returns for a UTC instant.

    Spark's driver-side collect() converts TimestampType values to Python
    datetimes using the host's local timezone, independent of
    spark.sql.session.timeZone. Build the expected value the same way so
    assertions aren't tied to the machine's timezone.
    """
    instant = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
    return datetime.fromtimestamp(instant.timestamp())


def _trades_df(spark, rows):
    values = ", ".join(
        f"('{symbol}', {price}D, {volume}D, {timestamp}L)"
        for symbol, price, volume, timestamp in rows
    )
    return spark.sql(
        f"""
        SELECT * FROM VALUES {values}
        AS trades(symbol, price, volume, timestamp)
        """
    )


def test_clean_stream_drops_non_positive_price_and_volume(spark):
    raw_df = _trades_df(
        spark,
        [
            ("AAPL", 100.0, 10.0, _millis(2024, 6, 28, 10, 0, 10)),
            ("AAPL", 0.0, 10.0, _millis(2024, 6, 28, 10, 0, 11)),
            ("AAPL", 100.0, 0.0, _millis(2024, 6, 28, 10, 0, 12)),
            ("AAPL", -1.0, 10.0, _millis(2024, 6, 28, 10, 0, 13)),
            ("AAPL", 100.0, -5.0, _millis(2024, 6, 28, 10, 0, 14)),
        ],
    )

    rows = clean_stream(raw_df).collect()

    assert len(rows) == 1
    assert rows[0].price == 100.0
    assert rows[0].volume == 10.0
    assert rows[0].event_time == _utc(2024, 6, 28, 10, 0, 10)


def test_aggregate_ohlc_builds_one_minute_candles_per_symbol(spark):
    raw_df = _trades_df(
        spark,
        [
            ("AAPL", 110.0, 10.0, _millis(2024, 6, 28, 10, 0, 10)),
            ("AAPL", 100.0, 20.0, _millis(2024, 6, 28, 10, 0, 30)),
            ("AAPL", 105.0, 5.0, _millis(2024, 6, 28, 10, 0, 50)),
            ("AAPL", 200.0, 8.0, _millis(2024, 6, 28, 10, 1, 5)),
            ("MSFT", 50.0, 3.0, _millis(2024, 6, 28, 10, 0, 20)),
        ],
    )

    candles = {
        (row.symbol, row.window_start, row.window_end): row
        for row in aggregate_ohlc(clean_stream(raw_df)).collect()
    }

    first_aapl = candles[
        ("AAPL", _utc(2024, 6, 28, 10, 0, 0), _utc(2024, 6, 28, 10, 1, 0))
    ]
    second_aapl = candles[
        ("AAPL", _utc(2024, 6, 28, 10, 1, 0), _utc(2024, 6, 28, 10, 2, 0))
    ]
    msft = candles[
        ("MSFT", _utc(2024, 6, 28, 10, 0, 0), _utc(2024, 6, 28, 10, 1, 0))
    ]

    assert first_aapl.open == 110.0
    assert first_aapl.high == 110.0
    assert first_aapl.low == 100.0
    assert first_aapl.close == 105.0
    assert first_aapl.volume == 35.0

    assert second_aapl.open == 200.0
    assert second_aapl.close == 200.0
    assert second_aapl.volume == 8.0

    assert msft.open == 50.0
    assert msft.volume == 3.0
    assert len(candles) == 3
