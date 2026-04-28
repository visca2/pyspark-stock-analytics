import json
import os
from pathlib import Path

import yaml
from box import Box
from dotenv import load_dotenv
from pyspark import __version__ as pyspark_version
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col


def _default_spark_packages():
    spark_version = ".".join(pyspark_version.split(".")[:3])
    spark_major = int(pyspark_version.split(".")[0])
    scala_suffix = "2.13" if spark_major >= 4 else "2.12"

    return [
        os.getenv(
            "SPARK_KAFKA_PACKAGE",
            f"org.apache.spark:spark-sql-kafka-0-10_{scala_suffix}:{spark_version}",
        ),
        os.getenv(
            "SPARK_AVRO_PACKAGE",
            f"org.apache.spark:spark-avro_{scala_suffix}:{spark_version}",
        ),
    ]


def _normalize_windows_path(path_value):
    drive, tail = os.path.splitdrive(path_value)
    if drive and tail and not tail.startswith(("\\", "/")):
        return f"{drive}\\{tail}"
    return path_value


def _configure_windows_hadoop(builder):
    if os.name != "nt":
        return builder

    hadoop_home = os.getenv("HADOOP_HOME")
    if not hadoop_home:
        raise RuntimeError(
            "Windows Spark requires HADOOP_HOME to point to a Hadoop folder "
            "that contains bin\\winutils.exe."
        )

    hadoop_home = _normalize_windows_path(hadoop_home)
    if not os.path.isabs(hadoop_home):
        raise RuntimeError(
            "HADOOP_HOME must be an absolute Windows path such as "
            "'C:\\hadoop'."
        )

    hadoop_home = str(Path(hadoop_home).resolve())
    hadoop_home_posix = Path(hadoop_home).as_posix()
    winutils_path = Path(hadoop_home) / "bin" / "winutils.exe"
    if not winutils_path.exists():
        raise RuntimeError(
            f"HADOOP_HOME is set to '{hadoop_home}', but "
            f"'{winutils_path}' was not found."
        )

    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["hadoop.home.dir"] = hadoop_home
    os.environ["PATH"] = f"{winutils_path.parent}{os.pathsep}{os.environ.get('PATH', '')}"

    return (
        builder
        .config("spark.hadoop.hadoop.home.dir", hadoop_home_posix)
        .config("spark.hadoop.home.dir", hadoop_home_posix)
    )


def main():
    """
    Entry point for the script.

    Returns:
        None
    """
    project_root = Path(__file__).resolve().parent.parent
    consumer_dir = Path(__file__).resolve().parent
    spark_runtime_dir = project_root / ".spark-runtime"
    checkpoint_dir = spark_runtime_dir / "checkpoints" / "trades_consumer"
    warehouse_dir = spark_runtime_dir / "warehouse"
    local_dir = spark_runtime_dir / "local"

    # Load environment configuration
    load_dotenv(consumer_dir / ".env")

    # Load YAML application config
    with open(consumer_dir / "config.yaml", "r", encoding="utf-8") as f:
        config_dict = yaml.safe_load(f)

    config = Box(config_dict)
    kafka_topics = config.kafka.topic
    subscribe_topics = ",".join(kafka_topics) if isinstance(kafka_topics, list) else kafka_topics
    spark_packages = ",".join(_default_spark_packages())

    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    local_dir.mkdir(parents=True, exist_ok=True)

    spark_builder = (
        SparkSession.builder
        .appName("TradesDataAnalysis") \
        .config("spark.local.dir", str(local_dir))
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", warehouse_dir.as_posix())
        .config("spark.jars.packages", spark_packages)
    )
    spark_builder = _configure_windows_hadoop(spark_builder)
    spark = spark_builder.getOrCreate()

    df = (
        spark.readStream
        .format("kafka") \
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS")) \
        .option("subscribe", subscribe_topics) \
        .option("startingOffsets", "earliest") \
        .load()
    )
    
    # Load the Avro schema used by the producer.
    with open(project_root / "avro" / "trade.avsc", "r", encoding="utf-8") as f:
        schema_dict = json.load(f)
    schema_json = json.dumps(schema_dict)

    parsed = df.select(
        from_avro(col("value"), schema_json).alias("data")
    ).select("data.*")

    query = (
        parsed.writeStream
        .format("console") \
        .option("truncate", "false")
        .option("checkpointLocation", checkpoint_dir.as_posix())
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
