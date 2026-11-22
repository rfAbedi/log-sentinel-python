import os
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from log_sentinel_python.utils.loggers import get_logger


logger = get_logger(__name__)


EVENT_SCHEMA = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField("service", StringType(), True),
        StructField("level", StringType(), True),
        StructField("message", StringType(), True),
        StructField("endpoint", StringType(), True),
        StructField("status_code", IntegerType(), True),
        StructField("latency_ms", IntegerType(), True),
        StructField("client_ip", StringType(), True),
    ]
)


def load_rules(path: Path) -> list[dict[str, Any]]:
    logger.info("Loading rules from %s", path)
    with path.open(encoding="utf-8") as rules_file:
        loaded = yaml.safe_load(rules_file)

    if not isinstance(loaded, dict) or not isinstance(loaded.get("rules"), list):
        raise ValueError("Rule configuration must contain a rules list")

    rules = loaded["rules"]
    logger.info("Loaded %d rules", len(rules))
    logger.debug("Rules: %s", [rule.get("name") for rule in rules])
    return rules


def build_rule_condition(rule: dict[str, Any]) -> Any:
    logger.debug(
        "Building condition for rule %s: %s %s %s",
        rule["name"],
        rule["field"],
        rule["operator"],
        rule["value"],
    )
    field = rule["field"]
    operator = rule["operator"]
    value = rule["value"]
    column = F.col(field)

    if operator == "equals":
        return column == value
    if operator == "greater_than":
        return column > value
    raise ValueError(f"Unsupported operator: {operator}")


def build_alerts_dataframe(parsed: Any, rules: list[dict[str, Any]]) -> Any:
    logger.info("Building alerts dataframe from %d rules", len(rules))
    alert_frames = []
    for rule in rules:
        condition = build_rule_condition(rule)
        alert_frame = (
            parsed.filter(condition)
            .withColumn("rule_name", F.lit(rule["name"]))
            .withColumn("field", F.lit(rule["field"]))
            .withColumn("actual_value", F.col(rule["field"]).cast("string"))
            .withColumn("expected_value", F.lit(str(rule["value"])))
            .select(
                F.col("rule_name"),
                F.col("service"),
                F.col("field"),
                F.col("actual_value"),
                F.col("expected_value"),
                F.col("raw_value").alias("event_json"),
            )
        )
        alert_frames.append(alert_frame)

    if not alert_frames:
        return parsed.limit(0).select(
            F.lit(None).cast("string").alias("rule_name"),
            F.col("service"),
            F.lit(None).cast("string").alias("field"),
            F.lit(None).cast("string").alias("actual_value"),
            F.lit(None).cast("string").alias("expected_value"),
            F.col("raw_value").alias("event_json"),
        )

    result = alert_frames[0]
    for frame in alert_frames[1:]:
        result = result.unionByName(frame)
    return result