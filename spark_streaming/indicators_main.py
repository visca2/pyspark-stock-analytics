"""OHLC indicators consumer."""

from pathlib import Path

from spark_config import build_spark_session
from streaming.calculate_indicators import add_moving_averages
from streaming.read_ohlc_stream import read_ohlc_stream
from utils.config_helper import load_config


def main():
    """Entry point."""
    project_root = Path.cwd()
    config = load_config(project_root)

    spark, spark_paths = build_spark_session("IndicatorsConsumer", project_root)

    ohlc_df = read_ohlc_stream(spark, config)
    indicators_df = add_moving_averages(ohlc_df)

    query = (
        indicators_df.writeStream.format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("checkpointLocation", spark_paths["checkpoint_dir"].as_posix())
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
