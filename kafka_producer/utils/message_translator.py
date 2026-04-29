"""Mapping functions to convert financial information APIs records to Kafka 'trade' messages."""


def finnhub_to_trade_record(trade):
    """Maps a Finnhub trade object to a Kafka object compatible with the 'trade' Avro schema."""
    return {
        "trade_conditions": trade.get("c"),
        "symbol": trade["s"],
        "price": trade["p"],
        "volume": trade["v"],
        "timestamp": trade["t"],
    }
