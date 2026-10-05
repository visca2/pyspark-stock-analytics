import json

from kafka_producer.finnhub_websocket import make_on_open_callback


class FakeWebSocket:
    def __init__(self):
        self.sent = []

    def send(self, message):
        self.sent.append(message)


def test_on_open_subscribes_to_each_symbol():
    websocket = FakeWebSocket()
    on_open = make_on_open_callback(["AAPL", "MSFT"])

    on_open(websocket)

    assert [json.loads(message) for message in websocket.sent] == [
        {"type": "subscribe", "symbol": "AAPL"},
        {"type": "subscribe", "symbol": "MSFT"},
    ]
