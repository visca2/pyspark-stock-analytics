"""Seed canned Avro trade events onto the `trades` Kafka topic for a manual smoke test.

Bypasses the real Finnhub WebSocket entirely: reuses the same Avro schema and
producer setup as kafka_producer.main, and publishes fabricated trades directly.
Run with the kafka_producer virtualenv, from the repo root:

    kafka_producer\\.venv\\Scripts\\python.exe scripts\\seed_trades.py

See docs/smoke-test.md for the full runbook.
"""

import argparse
import io
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastavro import schemaless_writer

from kafka_producer.utils.config_helper import (
    build_producer,
    load_config,
    parse_trade_kafka_schema,
)
from kafka_producer.utils.kafka_callbacks import delivery_report


def _aligned_minute(dt):
    return dt.replace(second=0, microsecond=0)


def build_trades(symbol, base_minute, num_candles):
    """Two trades per one-minute candle, plus one flush trade well past the
    last window so the watermark advances and Spark emits it.

    Structured Streaming only emits a windowed-aggregation row (in append
    mode) once the watermark passes window_end. With a 1-minute watermark
    (see clean_stream.py), the final candle stays buffered until a later
    event arrives — the flush trade exists purely to unstick it.
    """
    trades = []
    for i in range(num_candles):
        minute = base_minute + timedelta(minutes=i)
        open_price = 100.0 + i
        close_price = open_price + 0.5
        trades.append((symbol, open_price, 10.0, minute + timedelta(seconds=5)))
        trades.append((symbol, close_price, 20.0, minute + timedelta(seconds=45)))

    flush_time = base_minute + timedelta(minutes=num_candles + 5)
    trades.append((symbol, trades[-1][1], 1.0, flush_time))
    return trades


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--symbol",
        default="SMOKETEST",
        help="use a distinct value per run to avoid resuming prior indicator state",
    )
    parser.add_argument(
        "--candles", type=int, default=6, help="number of 1-minute candles to generate"
    )
    args = parser.parse_args()

    project_root = Path.cwd()
    config = load_config(project_root)
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap_servers:
        raise RuntimeError(
            "KAFKA_BOOTSTRAP_SERVERS not set — copy kafka_producer/.env.example "
            "to kafka_producer/.env first."
        )

    producer = build_producer(config.kafka.client_id, bootstrap_servers)
    schema = parse_trade_kafka_schema(project_root)

    base_minute = _aligned_minute(datetime.now(timezone.utc))
    trades = build_trades(args.symbol, base_minute, args.candles)

    for symbol, price, volume, event_time in trades:
        record = {
            "trade_conditions": None,
            "symbol": symbol,
            "price": price,
            "volume": volume,
            "timestamp": int(event_time.timestamp() * 1000),
        }
        buf = io.BytesIO()
        schemaless_writer(buf, schema, record)
        producer.produce(
            topic=config.kafka.topic,
            value=buf.getvalue(),
            key=None,
            on_delivery=delivery_report,
        )
        producer.poll(0)
        print(f"produced {symbol} @ {event_time.isoformat()} price={price} volume={volume}")

    producer.flush()
    print(f"\nDone. Seeded {args.candles} candles + 1 flush trade for symbol={args.symbol}.")
    print(f"First candle window_start should be {base_minute.isoformat()}.")


if __name__ == "__main__":
    main()
