import io
import json

from fastavro import schemaless_reader

from kafka_producer.finnhub_websocket import make_on_message_callback
from kafka_producer.utils.config_helper import parse_trade_kafka_schema


class FakeProducer:
    def __init__(self):
        self.messages = []
        self.poll_calls = []

    def produce(self, topic, value, key, on_delivery):
        self.messages.append(
            {
                "topic": topic,
                "value": value,
                "key": key,
                "on_delivery": on_delivery,
            }
        )

    def poll(self, timeout):
        self.poll_calls.append(timeout)


def _callback(project_root, producer, on_delivery=None):
    schema = parse_trade_kafka_schema(project_root)
    return (
        make_on_message_callback(
            producer,
            "trades",
            None,
            schema,
            on_delivery,
        ),
        schema,
    )


def test_on_message_ignores_non_trade_payloads(project_root):
    producer = FakeProducer()
    on_message, _schema = _callback(project_root, producer)

    on_message(None, json.dumps({"type": "ping"}))
    on_message(None, json.dumps({"type": "subscribe", "data": [{"s": "AAPL"}]}))

    assert producer.messages == []
    assert producer.poll_calls == []


def test_on_message_publishes_one_avro_record_per_trade(project_root):
    producer = FakeProducer()
    on_delivery = object()
    on_message, schema = _callback(project_root, producer, on_delivery=on_delivery)
    payload = {
        "type": "trade",
        "data": [
            {"s": "AAPL", "p": 189.42, "v": 100.0, "t": 1719532800000, "c": ["1"]},
            {"s": "MSFT", "p": 420.0, "v": 50.0, "t": 1719532801000},
        ],
    }

    on_message(None, json.dumps(payload))

    assert len(producer.messages) == 2
    assert producer.poll_calls == [0, 0]

    decoded = []
    for message in producer.messages:
        assert message["topic"] == "trades"
        assert message["key"] is None
        assert message["on_delivery"] is on_delivery
        decoded.append(schemaless_reader(io.BytesIO(message["value"]), schema))

    assert decoded == [
        {
            "trade_conditions": ["1"],
            "symbol": "AAPL",
            "price": 189.42,
            "volume": 100.0,
            "timestamp": 1719532800000,
        },
        {
            "trade_conditions": None,
            "symbol": "MSFT",
            "price": 420.0,
            "volume": 50.0,
            "timestamp": 1719532801000,
        },
    ]


def test_on_message_skips_trade_messages_with_empty_data(project_root):
    producer = FakeProducer()
    on_message, _schema = _callback(project_root, producer)

    on_message(None, json.dumps({"type": "trade"}))
    on_message(None, json.dumps({"type": "trade", "data": []}))

    assert producer.messages == []
