import json
import psycopg2
import pytest

from log_sentinel_python.core import producer
from log_sentinel_python.evaluator.streaming import build_spark_session, run_streaming


def make_event(service="payments", level="CRITICAL", status_code=401, latency_ms=4200):
    return {
        "timestamp": "2025-07-01T12:00:00Z",
        "service": service,
        "level": level,
        "status_code": status_code,
        "latency_ms": latency_ms,
    }


def reset_alerts(params):
    conn = psycopg2.connect(**params)
    with conn.cursor() as c:
        c.execute("DROP TABLE IF EXISTS alerts")
    conn.commit()
    conn.close()


@pytest.mark.integration
def test_build_spark_session():
    spark = build_spark_session()
    try:
        assert spark is not None
        assert spark.version.startswith("4.")
    finally:
        spark.stop()


@pytest.mark.integration
def test_run_streaming_persists_alerts(kafka_topic, kafka_bootstrap_servers, postgres_env, rules_path):
    reset_alerts(postgres_env)
    events = [make_event("payments"), make_event("catalog", level="INFO", status_code=200, latency_ms=250)]
    producer.produce(events, interval=0)

    run_streaming(
        bootstrap_servers=kafka_bootstrap_servers,
        topic=kafka_topic,
        rules_path=rules_path,
        duration_seconds=5,
    )

    conn = psycopg2.connect(**postgres_env)
    try:
        with conn.cursor() as c:
            c.execute("SELECT rule_name, service FROM alerts")
            rows = c.fetchall()
        assert len(rows) >= 3
        assert {r[1] for r in rows} == {"payments"}
    finally:
        conn.close()