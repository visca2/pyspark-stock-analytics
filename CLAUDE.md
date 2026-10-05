# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

Real-time stock price pipeline: Finnhub WebSocket → Kafka (`trades`, Avro) → Spark Structured Streaming OHLC aggregation → Kafka (`ohlc`, JSON) → Spark Structured Streaming stateful indicators (SMA/EMA/VWAP) → console.

```
Finnhub WebSocket → Kafka Producer → [Kafka: trades] → Spark OHLC Job → [Kafka: ohlc] → Spark Indicators Job
```

There are two independent Python components, each with its own virtualenv and no shared root-level dependency file:
- `kafka_producer/` — has its own `.venv`. Entry point: `python -m kafka_producer.main` (run from repo root).
- `spark_streaming/` — has its own `.venv`. Entry points `main.py` (OHLC job) and `indicators_main.py` (indicators job) are run with `cwd` set to `spark_streaming/` itself (e.g. `cd spark_streaming && python main.py`), not from the repo root.

This difference in working directory matters: `spark_streaming` modules (`spark_config`, `streaming.*`, `utils.config_helper`) are imported as top-level names (e.g. `from streaming.aggregate_ohlc import aggregate_ohlc`), not as `spark_streaming.streaming...`. `kafka_producer` modules, by contrast, are imported with the `kafka_producer.` package prefix.

## Running components

```bash
docker compose up -d                  # Kafka (KRaft), Schema Registry, Kafbat Kafka UI; auto-creates `trades`/`ohlc` topics via init-topics.sh
python -m kafka_producer.main         # Terminal 1 — from repo root, uses kafka_producer/.venv
cd spark_streaming && python main.py              # Terminal 2 — OHLC aggregation job
cd spark_streaming && python indicators_main.py   # Terminal 3 — SMA/EMA/VWAP job
```

Kafka UI: http://localhost:8080. Schema Registry: http://localhost:8081.

Each component loads env vars from `kafka_producer/.env` (copy from `kafka_producer/.env.example`) regardless of which component is running — `spark_streaming/utils/config_helper.py` also points `load_dotenv` at `kafka_producer/.env`. Required vars: `FINNHUB_API_KEY`, `KAFKA_BOOTSTRAP_SERVERS`, `SCHEMA_REGISTRY_URL`.

**Windows Spark note**: `spark_streaming/spark_config.py` requires `HADOOP_HOME` to be set to an absolute path containing `bin\winutils.exe`, or `build_spark_session` raises `RuntimeError`. Spark runtime state (checkpoints, warehouse, local dirs) is written under `.spark-runtime/<app_name_lowercased_with_underscores>/`.

## Tests

Run from repo root (`pytest.ini` sets `pythonpath = . spark_streaming`, which is why `spark_streaming` modules import without the package prefix in tests too):

```bash
pip install -r requirements-dev.txt
pytest                                 # run full suite
pytest tests/spark_streaming/test_indicators_updater.py   # single file
pytest tests/spark_streaming/test_indicators_updater.py::test_updater_returns_nothing_for_empty_input  # single test
pytest -m spark                        # only tests requiring a local SparkSession
```

Tests that need a real `SparkSession` use the session-scoped `spark` fixture in `tests/conftest.py`, marked `spark`; that fixture calls `pytest.skip` if Spark can't start in the environment (e.g. missing Java), so a clean skip is expected on machines without a JVM configured for PySpark.

Note: there is no root-level `requirements.txt`; `kafka_producer/` and `spark_streaming/` each manage dependencies in their own `.venv` (see `.vscode/launch.json` for the venv paths used by each debug config).

## Architecture notes

- **Avro schema is the source of truth for trade records**: `avro/trade.avsc` is read by both `kafka_producer` (to serialize) and `spark_streaming` (to deserialize via `from_avro`). Changing the schema means updating producer serialization and the Spark read path together.
- **OHLC aggregation (`spark_streaming/streaming/aggregate_ohlc.py`)** uses `min_by`/`max_by` on `event_time` to derive `open`/`close` (not row order), and a 1-minute watermark (`clean_stream.py`) to bound late data for the windowed aggregation.
- **Indicators job is fully stateful** (`spark_streaming/streaming/calculate_indicators.py`): `applyInPandasWithState` keeps a per-symbol state tuple `(recent_closes, last_ema, cumulative_price_volume, cumulative_volume)` across micro-batches with `GroupStateTimeout.NoTimeout` — state never expires, so a symbol's VWAP is cumulative since the job's checkpoint was created, not a rolling window. Deleting `.spark-runtime/checkpoints/indicatorsconsumer/` resets this state.
- **Config pattern**: both components use a near-identical `load_config`/Box(yaml) pattern in their respective `utils/config_helper.py` — `config.yaml` holds non-secret settings (topics, symbols, client IDs), `.env` holds secrets/environment-specific values (API keys, bootstrap servers). When adding config, follow this split rather than hardcoding or merging the two.
- **Two-stage Kafka handoff**: the OHLC job's Kafka *sink* schema (JSON via `to_json`/`struct` in `write_ohlc_stream.py`) and the indicators job's Kafka *source* schema (manual `StructType` in `read_ohlc_stream.py`) are declared independently and must be kept in sync by hand — there's no shared schema file for the `ohlc` topic the way there is for `trades`.
