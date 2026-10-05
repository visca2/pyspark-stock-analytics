# Manual smoke test

Validates the real Kafka wiring — Avro serialization over the wire, topic
config, checkpointing, and the OHLC job talking to the indicators job —
which the unit and Spark-batch tests under `tests/` can't reach (they call
the transform functions in-process, no Kafka involved). Run this after
touching the Avro schema, producer/consumer Kafka config, or the two
streaming jobs, or before a release. It's not part of `pytest`.

It bypasses the real Finnhub WebSocket: `scripts/seed_trades.py` publishes
fabricated trades directly onto the `trades` topic using the same Avro
schema and producer setup as `kafka_producer.main`.

> **Windows**: the indicators job uses a Python UDF
> (`applyInPandasWithState`), which spawns a separate Python worker process
> that the driver JVM talks to over a local
> socket. On Windows this handshake can fail outright — confirmed across
> every Python version (3.12, 3.14), every installed JDK (17, 21), with the
> correct interpreter/packages verified via live process monitoring, with
> antivirus exclusions added, with a clean Winsock provider catalog, and
> with explicit `--add-opens` JVM flags for Arrow's JPMS access — the worker
> just dies instantly with no Python-side output and no OS-level crash
> record. This is a PySpark-on-Windows limitation, not something wrong with
> this code or your machine. Use the Docker path below instead; it's
> confirmed working.

## Prerequisites

- `kafka_producer/.env` exists with at least `KAFKA_BOOTSTRAP_SERVERS` set
  (copy from `kafka_producer/.env.example`). `FINNHUB_API_KEY` is not needed
  since the WebSocket is bypassed.
- `kafka_producer/.venv` and `spark_streaming/.venv` are set up (see
  `.vscode/launch.json` for the expected paths).

## Steps

**1. Start infrastructure** (from repo root):

```bash
docker compose up -d
```

Creates `trades` and `ohlc` topics via `init-topics.sh`.

**2. Seed canned trades**:

```bash
kafka_producer\.venv\Scripts\python.exe scripts\seed_trades.py --symbol SMOKETEST
```

This produces 6 one-minute candles' worth of trades (2 trades per minute,
differing price/volume so open/high/low/close/volume are distinguishable)
plus one "flush" trade several minutes later, for symbol `SMOKETEST`. Use a
`--symbol` you haven't used before (or clear checkpoints — see Cleanup
below) so you're not resuming indicator state from a prior run.

**3. Run the OHLC job** (separate terminal, from repo root):

Natively:

```bash
cd spark_streaming
python main.py
```

Or via Docker (recommended on Windows — see the note above):

```bash
docker compose --profile spark up -d ohlc-job
docker logs -f ohlc-job
```

Let it run until candles stop appearing in the logs (~10–20s after step 2
finishes), then leave it running or stop it (Ctrl+C natively, or
`docker compose --profile spark stop ohlc-job`) — the `ohlc` topic already
has what it wrote.

**4. Check the `ohlc` topic** — via Kafka UI at http://localhost:8080, or a
console consumer. For `SMOKETEST` you should see 6 candles with
sequential `window_start` values one minute apart, `volume = 30.0` each
(10 + 20 from the two seeded trades), and `open`/`close` matching the
seeded prices.

> If you only see 5 candles (or fewer), the watermark hasn't advanced far
> enough to flush the last window yet — the OHLC job needs to still be
> running when the flush trade's event time passes through. Re-run step 3
> if you stopped it before the flush trade was processed.

**5. Run the indicators job** (another terminal):

Natively:

```bash
cd spark_streaming
python indicators_main.py
```

Or via Docker (recommended on Windows — see the note above):

```bash
docker compose --profile spark up -d indicators-job
docker logs -f indicators-job
```

Watch the console output for `SMOKETEST` rows:

- `sma_5` is `null` (NaN) for the first 4 candles, then populated from the
  5th candle onward.
- `ema_5` is populated starting from the first candle (seeds from the
  first close).
- `vwap` is cumulative — it should trend toward the average typical price
  across all candles seen so far, not reset per candle.

These are the same invariants `tests/spark_streaming/test_indicators_updater.py`
checks in isolation — this step proves they hold through the real Kafka
read/write path too.

## Cleanup / re-running

Both jobs checkpoint under `.spark-runtime/checkpoints/<app>/`
(`ohlcproducer` and `indicatorsconsumer`) whether run natively or via
Docker — the Docker services bind-mount the same host directory, so a
native run and a Docker run share checkpoint state. If you switch between
the two mid-troubleshooting (e.g. a native run crashed mid-batch, leaving
partially-written state) the next run — native or Docker — can hit a
state-store error unrelated to whatever you were actually testing; clearing
the checkpoint (below) resolves it. The indicators job's per-symbol
state (recent closes, last EMA, cumulative VWAP) is scoped by the `symbol`
column, so a fresh `--symbol` naturally gets fresh state. Re-using a symbol
resumes its old state, which isn't a bug but will throw off the walkthrough
in step 5 (e.g. `sma_5` populated immediately).

To force a fully clean run instead:

```bash
rm -rf .spark-runtime/checkpoints/ohlcproducer .spark-runtime/checkpoints/indicatorsconsumer
```

**This is destructive to any in-progress pipeline state**, including offsets
for symbols you actually care about — only do this on a dev setup you're
using solely for this smoke test.
