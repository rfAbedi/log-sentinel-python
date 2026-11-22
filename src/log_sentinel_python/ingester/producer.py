import os
import time
from pathlib import Path
from typing import Any

from kafka import DefaultSerializer, JsonSerializer, KafkaProducer
from kafka.errors import KafkaError

from log_sentinel_python.ingester.parser import read_logs
from log_sentinel_python.utils.loggers import get_logger


logger = get_logger(__name__)

TOPIC = os.environ.get("KAFKA_TOPIC")
DEFAULT_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
PUBLISH_INTERVAL_SECONDS = float(os.environ.get("KAFKA_PRODUCE_INTERVAL", "1"))


def _bootstrap_servers() -> list[str]:
    raw = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    return [server.strip() for server in raw.split(",") if server.strip()]


def build_producer() -> KafkaProducer:
    logger.info("Creating Kafka producer for topic: %s", TOPIC)
    return KafkaProducer(
        bootstrap_servers=_bootstrap_servers(),
        key_serializer=DefaultSerializer(),
        value_serializer=JsonSerializer(),
        enable_idempotence=True,
    )


def publish_event(producer: KafkaProducer, event: dict[str, Any]) -> None:
    key = event["service"]
    value = event

    try:
        record_metadata = producer.send(TOPIC, key=key, value=value).get(timeout=30)
    except KafkaError as error:
        logger.error("Failed to publish event for service %s: %s", key, error)
        raise

    logger.info(
        "Delivered event for service=%s to partition=%d offset=%d",
        key,
        record_metadata.partition,
        record_metadata.offset,
    )


def produce(events: list[dict[str, Any]], interval: float = PUBLISH_INTERVAL_SECONDS) -> int:
    producer = build_producer()
    published = 0

    try:
        for event in events:
            publish_event(producer, event)
            published += 1
            if interval > 0:
                time.sleep(interval)

        logger.info("published %d events to topic %s", published, TOPIC)
    finally:
        logger.debug("Closing kafka producer")
        producer.close(timeout=10)
        logger.debug("kafka producer got closed")

    return published

def main() -> None:
    events = read_logs(Path("data/sample_logs_100.jsonl"))
    published = produce(events)
    logger.info("Producer completed successfully with %d events", published)


if __name__ == "__main__":
    main()
