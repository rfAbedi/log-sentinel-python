import os
import time
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from log_sentinel_python.evaluator.repository import _persist_alerts_batch
from log_sentinel_python.evaluator.rules import EVENT_SCHEMA, build_alerts_dataframe, load_rules
from log_sentinel_python.utils.loggers import get_logger


logger = get_logger(__name__)

DEFAULT_KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "log-events")
DEFAULT_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_RULES_PATH = Path("config/rules.yaml")


def build_spark_session(master: str = "local[*]") -> SparkSession:
    logger.info("Creating Spark session in %s mode", master)
    builder = SparkSession.builder.appName("log-sentinel-spark-job").master(master)
    builder = builder.config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0",
    )
    return builder.getOrCreate()


def run_streaming(
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    topic: str = DEFAULT_KAFKA_TOPIC,
    rules_path: Path = DEFAULT_RULES_PATH,
    duration_seconds: int = 30,
) -> None:
    logger.info("Starting streaming: bootstrap=%s topic=%s rules=%s", bootstrap_servers, topic, rules_path)
    rules = load_rules(rules_path)
    spark = build_spark_session()

    try:
        logger.debug("Configuring Kafka source")
        raw_stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
        )

        logger.debug("Parsing Kafka messages with event schema")
        parsed = raw_stream.select(
            F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
            F.col("value").cast("string").alias("raw_value"),
        ).select("e.*", "raw_value")

        logger.info("Building alerts stream")
        alerts_df = build_alerts_dataframe(parsed, rules)

        logger.info("Starting streaming query")
        query = (
            alerts_df.writeStream.foreachBatch(_persist_alerts_batch)
            .outputMode("append")
            .start()
        )

        logger.info("Streaming will run for %d seconds", duration_seconds)
        time.sleep(duration_seconds)
        query.stop()
        logger.info("Streaming query stopped")
    finally:
        spark.stop()