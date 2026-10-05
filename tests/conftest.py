import os
import sys
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def project_root():
    return Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark")

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    from pyspark.sql import SparkSession

    spark_tmp = tmp_path_factory.mktemp("spark")
    try:
        session = (
            SparkSession.builder.master("local[1]")
            .appName("pyspark-stock-analytics-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.execution.arrow.pyspark.enabled", "false")
            .config("spark.local.dir", str(spark_tmp / "local"))
            .config("spark.sql.warehouse.dir", (spark_tmp / "warehouse").as_posix())
            .getOrCreate()
        )
        session.sparkContext.setLogLevel("ERROR")
        session.conf.set("spark.sql.session.timeZone", "UTC")
        session.sql("SELECT 1").collect()
    except Exception as exc:
        pytest.skip(f"Spark is unavailable: {exc}")

    yield session
    session.stop()
