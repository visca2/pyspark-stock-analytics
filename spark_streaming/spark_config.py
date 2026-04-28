import os
from pathlib import Path

from pyspark import __version__ as pyspark_version
from pyspark.sql import SparkSession


def default_spark_packages():
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


def normalize_windows_path(path_value):
    drive, tail = os.path.splitdrive(path_value)
    if drive and tail and not tail.startswith(("\\", "/")):
        return f"{drive}\\{tail}"
    return path_value


def configure_windows_hadoop(builder):
    if os.name != "nt":
        return builder

    hadoop_home = os.getenv("HADOOP_HOME")
    if not hadoop_home:
        raise RuntimeError(
            "Windows Spark requires HADOOP_HOME to point to a Hadoop folder "
            "that contains bin\\winutils.exe."
        )

    hadoop_home = normalize_windows_path(hadoop_home)
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
    os.environ["PATH"] = (
        f"{winutils_path.parent}{os.pathsep}{os.environ.get('PATH', '')}"
    )

    return (
        builder
        .config("spark.hadoop.hadoop.home.dir", hadoop_home_posix)
        .config("spark.hadoop.home.dir", hadoop_home_posix)
    )


def runtime_paths(project_root, app_name):
    spark_runtime_dir = project_root / ".spark-runtime"
    app_key = app_name.lower().replace(" ", "_")

    return {
        "checkpoint_dir": spark_runtime_dir / "checkpoints" / app_key,
        "warehouse_dir": spark_runtime_dir / "warehouse",
        "local_dir": spark_runtime_dir / "local",
    }


def build_spark_session(app_name, project_root):
    paths = runtime_paths(project_root, app_name)

    paths["checkpoint_dir"].mkdir(parents=True, exist_ok=True)
    paths["warehouse_dir"].mkdir(parents=True, exist_ok=True)
    paths["local_dir"].mkdir(parents=True, exist_ok=True)

    spark_packages = ",".join(default_spark_packages())
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.local.dir", str(paths["local_dir"]))
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", paths["warehouse_dir"].as_posix())
        .config("spark.jars.packages", spark_packages)
    )
    builder = configure_windows_hadoop(builder)

    return builder.getOrCreate(), paths
