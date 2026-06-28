# PySpark Stock Analytics

Real-time stock price analysis pipeline using Finnhub WebSockets, Apache Kafka, and PySpark Structured Streaming.

## Overview

The pipeline has three stages running concurrently:

```
Finnhub WebSocket
      │
      ▼
Kafka Producer  ──►  [Kafka: trades]  ──►  Spark OHLC Job  ──►  [Kafka: ohlc]  ──►  Spark Indicators Job
```

1. **Kafka Producer** — connects to the Finnhub WebSocket API and publishes raw trade events (serialized as Avro) to the `trades` Kafka topic.
2. **Spark OHLC Job** — reads the `trades` topic, cleans the data, and aggregates trades into 1-minute OHLC candles per symbol, publishing results as JSON to the `ohlc` topic.
3. **Spark Indicators Job** — reads the `ohlc` topic and calculates stateful technical indicators per symbol, printing results to the console.

## Technical Indicators

The indicators job computes three indicators per candle using stateful per-symbol aggregation:

| Indicator | Description |
|-----------|-------------|
| SMA (period 5) | Simple Moving Average over the last 5 closes |
| EMA (period 5) | Exponential Moving Average with α = 2/(n+1) |
| VWAP | Cumulative Volume-Weighted Average Price |

## Project Structure

```
pyspark-stock-analytics/
├── avro/
│   └── trade.avsc              # Avro schema for raw trade events
├── kafka_producer/
│   ├── main.py                 # Kafka producer entry point
│   ├── finnhub_websocket.py    # Finnhub WebSocket client
│   ├── config.yaml             # Producer config (topic, symbols)
│   ├── .env.example            # Required environment variables
│   └── utils/
│       ├── config_helper.py    # Config loading, producer setup
│       ├── kafka_callbacks.py  # Delivery report callback
│       ├── message_translator.py  # Finnhub → Trade record mapping
│       └── websocket_callbacks.py
├── spark_streaming/
│   ├── main.py                 # OHLC producer entry point
│   ├── indicators_main.py      # Indicators consumer entry point
│   ├── spark_config.py         # SparkSession builder (incl. Windows support)
│   └── streaming/
│       ├── read_stream.py      # Read Avro trades from Kafka
│       ├── clean_stream.py     # Validate and clean raw trades
│       ├── aggregate_ohlc.py   # 1-minute OHLC windowed aggregation
│       ├── write_ohlc_stream.py # Publish OHLC candles to Kafka
│       ├── read_ohlc_stream.py  # Read OHLC candles from Kafka
│       └── calculate_indicators.py  # SMA, EMA, VWAP via applyInPandasWithState
├── docker-compose.yaml         # Kafka, Schema Registry, Kafka UI
└── init-topics.sh              # Creates 'trades' and 'ohlc' topics
```

## Prerequisites

- Python 3.11+
- Java 11+ (required by PySpark)
- Docker and Docker Compose
- A [Finnhub](https://finnhub.io/) API key (free tier is sufficient)
- **Windows only**: `winutils.exe` — set `HADOOP_HOME` to a folder containing `bin\winutils.exe`

## Setup

**1. Start the Kafka infrastructure**

```bash
docker compose up -d
```

This starts Kafka (KRaft mode), Confluent Schema Registry, Kafbat Kafka UI, and creates the `trades` and `ohlc` topics automatically.

**2. Configure the Kafka producer**

```bash
cp kafka_producer/.env.example kafka_producer/.env
```

Edit `kafka_producer/.env`:

```env
FINNHUB_API_KEY=your_finnhub_api_key
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SCHEMA_REGISTRY_URL=http://localhost:8081
```

Edit `kafka_producer/config.yaml` to set the symbols you want to track.

**3. Install Python dependencies**

```bash
pip install -r requirements.txt
```

## Running

Each component runs as a separate process. Open three terminals.

**Terminal 1 — Kafka Producer** (ingests live trades from Finnhub):

```bash
python -m kafka_producer.main
```

**Terminal 2 — Spark OHLC Job** (aggregates trades into candles):

```bash
cd spark_streaming
python main.py
```

**Terminal 3 — Spark Indicators Job** (calculates SMA, EMA, VWAP):

```bash
cd spark_streaming
python indicators_main.py
```

## Monitoring

Kafbat Kafka UI is available at [http://localhost:8080](http://localhost:8080) — use it to inspect topic messages and consumer lag.

## Data Flow Details

### Trade event (Avro, `trades` topic)

```json
{
  "trade_conditions": ["string"] | null,
  "symbol": "AAPL",
  "price": 189.42,
  "volume": 100.0,
  "timestamp": 1719532800000
}
```

### OHLC candle (JSON, `ohlc` topic)

```json
{
  "symbol": "AAPL",
  "window_start": "2024-06-28T10:00:00",
  "window_end": "2024-06-28T10:01:00",
  "open": 189.10,
  "high": 189.80,
  "low": 188.95,
  "close": 189.42,
  "volume": 4500.0
}
```

### Indicators output (console)

Adds `sma_5`, `ema_5`, and `vwap` columns to each OHLC candle row, keyed per symbol.
