from datetime import datetime

import pandas as pd
import pytest

from streaming.calculate_indicators import _build_indicator_updater


class FakeGroupState:
    def __init__(self, value=None):
        self.exists = value is not None
        self.get = value

    def update(self, value):
        self.get = value
        self.exists = True


def _candle(close, volume=100.0, high=None, low=None, minute=0):
    price = float(close)
    return {
        "window_start": datetime(2024, 6, 28, 10, minute),
        "window_end": datetime(2024, 6, 28, 10, minute + 1),
        "open": price,
        "high": price if high is None else float(high),
        "low": price if low is None else float(low),
        "close": price,
        "volume": float(volume),
    }


def _run(state, rows, sma_period=5, ema_period=5):
    updater = _build_indicator_updater(sma_period, ema_period)
    frames = updater(("AAPL",), iter([pd.DataFrame(rows)]), state)
    return list(frames)


def test_updater_returns_nothing_for_empty_input():
    state = FakeGroupState()
    updater = _build_indicator_updater(5, 5)

    assert list(updater(("AAPL",), iter([]), state)) == []
    assert list(updater(("AAPL",), iter([pd.DataFrame()]), state)) == []
    assert state.exists is False


def test_sma_is_null_until_period_then_rolls():
    state = FakeGroupState()
    frames = _run(state, [_candle(close, minute=i) for i, close in enumerate([10, 20, 30, 40, 50, 60])])
    result = frames[0]

    assert result["sma_5"].iloc[:4].isna().all()
    assert result["sma_5"].iloc[4] == pytest.approx(30.0)
    assert result["sma_5"].iloc[5] == pytest.approx(40.0)
    assert state.get[0] == [20.0, 30.0, 40.0, 50.0, 60.0]


def test_ema_seeds_from_first_close():
    state = FakeGroupState()
    alpha = 2.0 / 6.0
    frames = _run(state, [_candle(10, minute=0), _candle(20, minute=1)])
    ema = frames[0]["ema_5"]

    assert ema.iloc[0] == pytest.approx(10.0)
    assert ema.iloc[1] == pytest.approx(alpha * 20.0 + (1 - alpha) * 10.0)


def test_vwap_uses_typical_price_and_cumulative_volume():
    state = FakeGroupState()
    frames = _run(
        state,
        [
            _candle(10, volume=100, high=12, low=8, minute=0),
            _candle(20, volume=300, high=24, low=16, minute=1),
        ],
    )
    vwap = frames[0]["vwap"]

    first_typical = (12 + 8 + 10) / 3.0
    second_typical = (24 + 16 + 20) / 3.0
    assert vwap.iloc[0] == pytest.approx(first_typical)
    assert vwap.iloc[1] == pytest.approx(
        (first_typical * 100 + second_typical * 300) / 400
    )


def test_state_resumes_across_micro_batches():
    state = FakeGroupState()
    first = _run(state, [_candle(close, minute=i) for i, close in enumerate([10, 20, 30])])
    second = _run(state, [_candle(close, minute=i + 3) for i, close in enumerate([40, 50])])

    assert first[0]["sma_5"].isna().all()
    assert pd.isna(second[0]["sma_5"].iloc[0])
    assert second[0]["sma_5"].iloc[1] == pytest.approx(30.0)
    assert second[0]["symbol"].tolist() == ["AAPL", "AAPL"]

    alpha = 2.0 / 6.0
    ema = 10.0
    for close in (20, 30, 40, 50):
        ema = alpha * close + (1 - alpha) * ema
    assert second[0]["ema_5"].iloc[1] == pytest.approx(ema)
