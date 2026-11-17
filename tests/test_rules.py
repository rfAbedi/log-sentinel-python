from pathlib import Path

from log_sentinel_python.core.rules import evaluate_rules, load_rules


RULES_PATH = Path("config/rules.yaml")
RULES = [
    {"name": "critical_level", "field": "level", "operator": "equals", "value": "CRITICAL"},
    {"name": "high_latency", "field": "latency_ms", "operator": "greater_than", "value": 3000},
    {"name": "auth_failure", "field": "status_code", "operator": "equals", "value": 401},
]


def test_load_rules_returns_three_configured_rules() -> None:
    rules = load_rules(RULES_PATH)

    assert [rule["name"] for rule in rules] == [
        "critical_level",
        "high_latency",
        "auth_failure",
    ]


def test_evaluate_rules_returns_all_matching_alerts() -> None:
    event = {
        "timestamp": "2025-07-01T12:00:00Z",
        "service": "auth",
        "level": "CRITICAL",
        "status_code": 401,
        "latency_ms": 4200,
    }

    alerts = evaluate_rules(event, RULES)

    assert [alert["rule_name"] for alert in alerts] == [
        "critical_level",
        "high_latency",
        "auth_failure",
    ]


def test_evaluate_rules_returns_no_alerts_for_non_matching_event() -> None:
    event = {
        "timestamp": "2025-07-01T12:00:00Z",
        "service": "catalog",
        "level": "INFO",
        "status_code": 200,
        "latency_ms": 250,
    }

    assert evaluate_rules(event, RULES) == []
