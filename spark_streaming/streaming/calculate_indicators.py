"""Calculate technical indicators for finalized OHLC candles."""

from collections.abc import Iterator

import pandas as pd
from pyspark.sql.streaming.state import GroupStateTimeout
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


DEFAULT_SMA_PERIOD = 5
DEFAULT_EMA_PERIOD = 5


def _build_indicator_updater(sma_period, ema_period):
    """Create a stateful updater for per-symbol candle indicators."""

    alpha = 2.0 / (ema_period + 1)

    def update_indicators(
        key, pdf_iter: Iterator[pd.DataFrame], state
    ) -> Iterator[pd.DataFrame]:
        symbol = key[0]
        recent_closes = []
        last_ema = None
        cumulative_price_volume = 0.0
        cumulative_volume = 0.0

        if state.exists:
            (
                saved_recent_closes,
                saved_last_ema,
                saved_cumulative_price_volume,
                saved_cumulative_volume,
            ) = state.get
            recent_closes = list(saved_recent_closes or [])
            last_ema = saved_last_ema
            cumulative_price_volume = saved_cumulative_price_volume or 0.0
            cumulative_volume = saved_cumulative_volume or 0.0

        batch_frames = [pdf for pdf in pdf_iter if not pdf.empty]
        if not batch_frames:
            return

        batch_pdf = pd.concat(batch_frames, ignore_index=True).sort_values(
            ["window_start", "window_end"]
        )

        output_rows = []
        for row in batch_pdf.itertuples(index=False):
            high_price = float(row.high)
            low_price = float(row.low)
            close_price = float(row.close)
            volume = float(row.volume)

            recent_closes.append(close_price)
            if len(recent_closes) > sma_period:
                recent_closes.pop(0)

            sma_value = None
            if len(recent_closes) == sma_period:
                sma_value = sum(recent_closes) / sma_period

            ema_value = close_price if last_ema is None else (
                alpha * close_price + ((1 - alpha) * last_ema)
            )
            last_ema = ema_value

            typical_price = (high_price + low_price + close_price) / 3.0
            cumulative_price_volume += typical_price * volume
            cumulative_volume += volume
            vwap_value = (
                cumulative_price_volume / cumulative_volume
                if cumulative_volume
                else None
            )

            output_rows.append(
                {
                    "symbol": symbol,
                    "window_start": row.window_start,
                    "window_end": row.window_end,
                    "open": float(row.open),
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "volume": volume,
                    f"sma_{sma_period}": sma_value,
                    f"ema_{ema_period}": ema_value,
                    "vwap": vwap_value,
                }
            )

        state.update(
            (
                recent_closes,
                last_ema,
                cumulative_price_volume,
                cumulative_volume,
            )
        )
        yield pd.DataFrame(output_rows)

    return update_indicators


def add_indicators(
    ohlc_df,
    sma_period=DEFAULT_SMA_PERIOD,
    ema_period=DEFAULT_EMA_PERIOD,
):
    """Add indicator columns to finalized OHLC candles."""

    output_schema = StructType(
        [
            StructField("symbol", StringType(), False),
            StructField("window_start", TimestampType(), False),
            StructField("window_end", TimestampType(), False),
            StructField("open", DoubleType(), False),
            StructField("high", DoubleType(), False),
            StructField("low", DoubleType(), False),
            StructField("close", DoubleType(), False),
            StructField("volume", DoubleType(), False),
            StructField(f"sma_{sma_period}", DoubleType(), True),
            StructField(f"ema_{ema_period}", DoubleType(), False),
            StructField("vwap", DoubleType(), True),
        ]
    )

    state_schema = StructType(
        [
            StructField("recent_closes", ArrayType(DoubleType()), True),
            StructField("last_ema", DoubleType(), True),
            StructField("cumulative_price_volume", DoubleType(), False),
            StructField("cumulative_volume", DoubleType(), False),
        ]
    )

    return ohlc_df.groupBy("symbol").applyInPandasWithState(
        _build_indicator_updater(sma_period, ema_period),
        outputStructType=output_schema,
        stateStructType=state_schema,
        outputMode="Append",
        timeoutConf=GroupStateTimeout.NoTimeout,
    )


def add_moving_averages(
    ohlc_df,
    sma_period=DEFAULT_SMA_PERIOD,
    ema_period=DEFAULT_EMA_PERIOD,
):
    """Backward-compatible alias for adding all indicator columns."""

    return add_indicators(ohlc_df, sma_period, ema_period)
