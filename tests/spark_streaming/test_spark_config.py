from pathlib import Path

import pytest

import spark_config


class FakeBuilder:
    def __init__(self):
        self.configs = {}

    def config(self, key, value):
        self.configs[key] = value
        return self


def test_normalize_windows_path_inserts_separator_after_drive():
    assert spark_config.normalize_windows_path("C:hadoop") == "C:\\hadoop"
    assert spark_config.normalize_windows_path("C:\\hadoop") == "C:\\hadoop"


def test_runtime_paths_are_scoped_per_app(tmp_path):
    paths = spark_config.runtime_paths(tmp_path, "Ohlc Producer")

    assert paths["checkpoint_dir"] == tmp_path / ".spark-runtime" / "checkpoints" / "ohlc_producer"
    assert paths["warehouse_dir"] == tmp_path / ".spark-runtime" / "warehouse" / "ohlc_producer"
    assert paths["local_dir"] == tmp_path / ".spark-runtime" / "local" / "ohlc_producer"


def test_configure_windows_hadoop_is_noop_on_posix(monkeypatch):
    monkeypatch.setattr(spark_config.os, "name", "posix")
    builder = FakeBuilder()

    assert spark_config.configure_windows_hadoop(builder) is builder
    assert builder.configs == {}


def test_configure_windows_hadoop_requires_hadoop_home(monkeypatch):
    monkeypatch.setattr(spark_config.os, "name", "nt")
    monkeypatch.delenv("HADOOP_HOME", raising=False)

    with pytest.raises(RuntimeError, match="HADOOP_HOME"):
        spark_config.configure_windows_hadoop(FakeBuilder())


def test_configure_windows_hadoop_rejects_relative_home(monkeypatch):
    monkeypatch.setattr(spark_config.os, "name", "nt")
    monkeypatch.setenv("HADOOP_HOME", "hadoop")

    with pytest.raises(RuntimeError, match="absolute"):
        spark_config.configure_windows_hadoop(FakeBuilder())


def test_configure_windows_hadoop_requires_winutils(monkeypatch, tmp_path):
    monkeypatch.setattr(spark_config.os, "name", "nt")
    monkeypatch.setenv("HADOOP_HOME", str(tmp_path))

    with pytest.raises(RuntimeError, match="winutils.exe"):
        spark_config.configure_windows_hadoop(FakeBuilder())


def test_configure_windows_hadoop_sets_paths_when_winutils_exists(monkeypatch, tmp_path):
    monkeypatch.setattr(spark_config.os, "name", "nt")
    winutils = tmp_path / "bin" / "winutils.exe"
    winutils.parent.mkdir(parents=True)
    winutils.write_bytes(b"")
    monkeypatch.setenv("HADOOP_HOME", str(tmp_path))

    builder = spark_config.configure_windows_hadoop(FakeBuilder())
    hadoop_home = str(Path(tmp_path).resolve())

    assert spark_config.os.environ["HADOOP_HOME"] == hadoop_home
    assert builder.configs["spark.hadoop.hadoop.home.dir"] == Path(hadoop_home).as_posix()
    assert builder.configs["spark.hadoop.home.dir"] == Path(hadoop_home).as_posix()
