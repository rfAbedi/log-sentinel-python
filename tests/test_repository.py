import json
import psycopg2
import pytest

from log_sentinel_python.evaluator.repository import (
    _ensure_alerts_table,
    _persist_alerts_batch,
    _pg_connection,
)


@pytest.mark.integration
def test_pg_connection_and_ensure_table(postgres_env):
    connection = _pg_connection()
    try:
        _ensure_alerts_table(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT to_regclass('public.alerts')"
            )
            result = cursor.fetchone()[0]
        assert result == "alerts"
    finally:
        connection.close()


@pytest.mark.integration
def test_persist_alerts_batch_writes_rows(postgres_env, spark_session):
    reset = psycopg2.connect(**postgres_env)
    with reset.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS alerts")
    reset.commit()
    reset.close()

    event = {
        "timestamp": "2025-07-01T12:00:00Z",
        "service": "payments",
        "level": "CRITICAL",
        "status_code": 401,
        "latency_ms": 4200,
    }
    raw = json.dumps(event)
    rows = [
        ("critical_level", "payments", "level", "CRITICAL", "CRITICAL", raw),
        ("auth_failure", "payments", "status_code", "401", "401", raw),
    ]
    df = spark_session.createDataFrame(rows, ["rule_name", "service", "field", "actual_value", "expected_value", "event_json"])

    _persist_alerts_batch(df, batch_id=1)

    connection = psycopg2.connect(**postgres_env)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT rule_name, service FROM alerts ORDER BY id")
            persisted = cursor.fetchall()
        assert len(persisted) == 2
        assert {row[0] for row in persisted} == {"critical_level", "auth_failure"}
    finally:
        connection.close()