import pytest

from kafka_producer.utils.message_translator import finnhub_to_trade_record


def test_finnhub_to_trade_record_maps_fields():
    trade = {
        "s": "AAPL",
        "p": 189.42,
        "v": 100.0,
        "t": 1719532800000,
        "c": ["1", "12"],
    }

    assert finnhub_to_trade_record(trade) == {
        "trade_conditions": ["1", "12"],
        "symbol": "AAPL",
        "price": 189.42,
        "volume": 100.0,
        "timestamp": 1719532800000,
    }


def test_finnhub_to_trade_record_allows_missing_conditions():
    trade = {"s": "AAPL", "p": 1.0, "v": 1.0, "t": 1}

    assert finnhub_to_trade_record(trade)["trade_conditions"] is None


@pytest.mark.parametrize("missing_field", ["s", "p", "v", "t"])
def test_finnhub_to_trade_record_requires_core_fields(missing_field):
    trade = {"s": "AAPL", "p": 1.0, "v": 1.0, "t": 1}
    trade.pop(missing_field)

    with pytest.raises(KeyError):
        finnhub_to_trade_record(trade)
