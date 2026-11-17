from pathlib import Path
from typing import Any

import yaml

from log_sentinel_python.utils.loggers import get_logger


logger = get_logger(__name__)
REQUIRED_RULE_FIELDS = {"name", "field", "operator", "value"}


def load_rules(path: Path) -> list[dict[str, Any]]:
    logger.info("Starting rule configuration load: %s", path)

    with path.open(encoding="utf-8") as rules_file:
        loaded_config = yaml.safe_load(rules_file)
        
    if not isinstance(loaded_config, dict) or not isinstance(loaded_config.get("rules"), list):
        logger.error("Rule configuration must contain a rules list")
        raise ValueError("Rule configuration must contain a rules list")

    rules = loaded_config["rules"]
    for rule_number, rule in enumerate(rules, start=1):
        if not isinstance(rule, dict):
            logger.error("Rule %d is not a mapping", rule_number)
            raise ValueError(f"Rule {rule_number} is not a mapping")

        missing_fields = REQUIRED_RULE_FIELDS - rule.keys()
        if missing_fields:
            logger.error("Rule %d has missing fields: %s", rule_number, sorted(missing_fields))
            raise ValueError(f"Rule {rule_number} has missing fields: {sorted(missing_fields)}")

        logger.debug("Loaded rule %d: %s", rule_number, rule["name"])

    logger.info("Completed rule configuration load: %d rules", len(rules))
    return rules


def rule_matches(event: dict[str, Any], rule: dict[str, Any]) -> bool:
    field = rule["field"]
    actual_value = event.get(field)
    expected_value = rule["value"]
    operator = rule["operator"]

    logger.debug("Evaluating rule %s against field %s", rule["name"], field)

    if operator == "equals":
        matched = actual_value == expected_value
    elif operator == "greater_than":
        matched = actual_value is not None and actual_value > expected_value
    else:
        logger.error("Rule %s has unsupported operator: %s", rule["name"], operator)
        raise ValueError(f"Unsupported operator: {operator}")

    logger.debug("Rule %s matched: %s", rule["name"], matched)
    return matched


def evaluate_rules(event: dict[str, Any], rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    logger.info("Starting rule evaluation for service: %s", event.get("service", "unknown"))
    alerts: list[dict[str, Any]] = []

    for rule in rules:
        if not rule_matches(event, rule):
            continue

        alert = {
            "rule_name": rule["name"],
            "field": rule["field"],
            "actual_value": event.get(rule["field"]),
            "expected_value": rule["value"],
            "event": event,
        }
        alerts.append(alert)
        logger.warning("Rule %s triggered for service: %s", rule["name"], event.get("service", "unknown"))

    logger.info("Completed rule evaluation: %d alerts triggered", len(alerts))
    return alerts
