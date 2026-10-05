FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

WORKDIR /app

COPY spark_streaming/requirements.txt spark_streaming/requirements.txt
RUN pip install --no-cache-dir -r spark_streaming/requirements.txt

COPY avro/ avro/
COPY spark_streaming/ spark_streaming/

# Pre-fetch the Kafka/Avro packages into the Ivy cache at build time so
# containers don't need network access (or the resolve delay) on every start.
RUN python -c "\
from pyspark.sql import SparkSession; \
from spark_streaming.spark_config import default_spark_packages; \
spark = SparkSession.builder.master('local[1]').config('spark.jars.packages', ','.join(default_spark_packages())).getOrCreate(); \
spark.stop()"

ENTRYPOINT ["python"]
