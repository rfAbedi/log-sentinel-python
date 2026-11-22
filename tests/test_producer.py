import json

import pytest
from kafka import KafkaConsumer
from kafka.serializer import Deserializer

from log_sentinel_python.ingester import producer


pytestmark = pytest.mark.integration


class JsonDeserializer(Deserializer):
    def deserialize(self, topic, headers, data):
        return json.loads(data.decode("utf-8"))


def make_event(service: str, level: str = "INFO", latency_ms: int = 42) -> dict:
    return {
        "timestamp": "2025-07-01T12:00:00Z",
        "service": service,
        "level": level,
        "status_code": 200,
        "latency_ms": latency_ms,
    }


def consume_topic(topic: str, bootstrap_servers: str) -> list:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
        value_deserializer=JsonDeserializer(),
    )
    try:
        return list(consumer)
    finally:
        consumer.close()


def test_produce_publishes_each_event_with_service_key(
    kafka_topic, kafka_bootstrap_servers
):
    events = [
        make_event("payments"),
        make_event("auth", level="ERROR", latency_ms=5000),
    ]

    published = producer.produce(events, interval=0)

    assert published == len(events)

    records = consume_topic(kafka_topic, kafka_bootstrap_servers)
    assert len(records) == len(events)
    assert [record.key.decode("utf-8") for record in records] == ["payments", "auth"]
    assert [record.value for record in records] == events


def test_produce_groups_events_for_the_same_service_under_one_key(
    kafka_topic, kafka_bootstrap_servers
):
    events = [
        make_event("payments", latency_ms=100),
        make_event("payments", latency_ms=200),
        make_event("orders", level="CRITICAL", latency_ms=300),
    ]

    producer.produce(events, interval=0)

    records = consume_topic(kafka_topic, kafka_bootstrap_servers)
    keys = [record.key.decode("utf-8") for record in records]

    assert keys == ["payments", "payments", "orders"]
    assert len({keys[0], keys[1]}) == 1