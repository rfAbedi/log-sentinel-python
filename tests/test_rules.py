from pathlib import Path

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F

from log_sentinel_python.evaluator.rules import (
    EVENT_SCHEMA,
    build_alerts_dataframe,
    build_rule_condition,
    load_rules,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RULES_PATH = PROJECT_ROOT / "config" / "rules.yaml"


def test_load_rules_returns_three_rules():
    rules = load_rules(RULES_PATH)
    assert [rule["name"] for rule in rules] == [
        "critical_level",
        "high_latency",
        "auth_failure",
    ]


def test_build_rule_condition_equals(spark_session):
    rule = {"name": "r", "field": "level", "operator": "equals", "value": "CRITICAL"}
    condition = build_rule_condition(rule)
    df = spark_session.createDataFrame([("CRITICAL",), ("INFO",)], ["level"])
    result = df.filter(condition).collect()
    assert len(result) == 1
    assert result[0]["level"] == "CRITICAL"


def test_build_rule_condition_greater_than(spark_session):
    rule = {"name": "r", "field": "latency_ms", "operator": "greater_than", "value": 3000}
    condition = build_rule_condition(rule)
    df = spark_session.createDataFrame([(4200,), (250,)], ["latency_ms"])
    result = df.filter(condition).collect()
    assert len(result) == 1
    assert result[0]["latency_ms"] == 4200


def test_build_alerts_dataframe_produces_matching_alerts(spark_session):
    rules = load_rules(RULES_PATH)
    event = {
        "timestamp": "2025-07-01T12:00:00Z",
        "service": "auth",
        "level": "CRITICAL",
        "message": "m",
        "endpoint": "/login",
        "status_code": 401,
        "latency_ms": 4200,
        "client_ip": "1.1.1.1",
    }
    raw_value = __import__("json").dumps(event)
    df = spark_session.createDataFrame([(raw_value,)], ["raw_value"])
    parsed = df.select(
        F.from_json(F.col("raw_value"), EVENT_SCHEMA).alias("e"),
        F.col("raw_value"),
    ).select("e.*", "raw_value")

    alerts = build_alerts_dataframe(parsed, rules)
    rows = alerts.collect()
    assert len(rows) == 3
    assert {row.rule_name for row in rows} == {"critical_level", "high_latency", "auth_failure"}
    assert all(row.service == "auth" for row in rows)
    assert all(row.event_json == raw_value for row in rows)