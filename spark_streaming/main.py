"""Trades consumer"""

from pathlib import Path

from spark_config import build_spark_session
from streaming.aggregate_ohlc import aggregate_ohlc
from streaming.clean_stream import clean_stream
from streaming.read_stream import read_stream
from streaming.write_ohlc_stream import write_ohlc_stream
from utils.config_helper import load_config, parse_trade_kafka_schema_json


def main():
    """Entry point."""
    project_root = Path.cwd()

    # Load configuration and set environment variables
    config = load_config(project_root)
    trade_schema_json = parse_trade_kafka_schema_json(project_root)

    spark, spark_paths = build_spark_session("OhlcProducer", project_root)

    raw_df = read_stream(spark, trade_schema_json, config)
    clean_df = clean_stream(raw_df)
    aggregate_ohlc_df = aggregate_ohlc(clean_df)

    query = write_ohlc_stream(
        aggregate_ohlc_df,
        config,
        spark_paths["checkpoint_dir"].as_posix(),
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
