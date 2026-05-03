"""Create a Finnhub websocket that converts its messages to
    'trade' objects to send to the relevant Kafka topic."""


import io
import json
import os

from fastavro import schemaless_writer
import websocket

from kafka_producer.utils.kafka_callbacks import delivery_report
from kafka_producer.utils.message_translator import finnhub_to_trade_record
from kafka_producer.utils.websocket_callbacks import on_close, on_error


def make_on_message_callback(producer, topic, key, parsed_schema, callback):
    """Closure that creates an on_message websocket callback that maps
        Finnhub message to a 'trade' record and publishes it to Kafka."""

    def on_message(ws, message):
        value = json.loads(message)

        if value.get("type") != "trade":
            return

        for trade in value.get("data", []):
            bytes_io = io.BytesIO()
            schemaless_writer(bytes_io, parsed_schema, finnhub_to_trade_record(trade))
            binary_data = bytes_io.getvalue()
            producer.produce(topic=topic, value=binary_data, key=key, on_delivery=callback)
            producer.poll(0)
    return on_message


def make_on_open_callback(symbols):
    """Closure that creates an on_message websocket function to
        subscribe to the configured list of Finnhub symbols."""
    def on_open(ws):
        for symbol in symbols:
            ws.send(f'{{"type":"subscribe","symbol":"{symbol}"}}')
    return on_open


def build_finnhub_websocket(kafka_producer, kafka_topic, parsed_schema, finnhub_symbols):
    """Build and return a websocket for the Finnhub service."""
    on_message = make_on_message_callback(
        kafka_producer,
        kafka_topic,
        None,
        parsed_schema,
        delivery_report
    )

    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(
        f"wss://ws.finnhub.io?token={os.getenv("FINNHUB_API_KEY")}",
        on_message = on_message,
        on_error = on_error,
        on_close = on_close
    )

    ws.on_open = make_on_open_callback(finnhub_symbols)
    return ws
