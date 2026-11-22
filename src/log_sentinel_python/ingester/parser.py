import json
from collections import Counter
from pathlib import Path
from typing import Any

from log_sentinel_python.utils.loggers import get_logger


logger = get_logger(__name__)
REQUIRED_FIELDS = {"timestamp", "service", "level", "status_code", "latency_ms"}


def parse_log_line(line: str) -> dict[str, Any] | None:
    logger.debug("Starting JSON log-line parsing")

    try:
        event = json.loads(line)
    except json.JSONDecodeError as error:
        logger.warning("Rejected malformed JSON log line: %s", error.msg)
        return None

    if not isinstance(event, dict):
        logger.warning("Rejected JSON value because it is not an object")
        return None

    missing_fields = REQUIRED_FIELDS - event.keys()
    if missing_fields:
        logger.warning("Rejected log event with missing fields: %s", sorted(missing_fields))
        return None

    logger.debug("Successfully parsed a valid log event")
    return event


def read_logs(path: Path) -> list[dict[str, Any]]:
    logger.info("Starting log-file read: %s", path)
    events: list[dict[str, Any]] = []

    with path.open(encoding="utf-8") as log_file:
        logger.debug("Opened log file successfully: %s", path)

        for line_number, line in enumerate(log_file, start=1):
            if not line.strip():
                logger.debug("Skipping blank log line %d", line_number)
                continue

            event = parse_log_line(line)
            if event is None:
                logger.warning("Skipping invalid log line %d", line_number)
                continue

            events.append(event)
            logger.debug("Accepted log line %d; valid event count is %d", line_number, len(events))

    logger.info("Completed log-file read: %d valid events", len(events))
    return events


def level_counts(events: list[dict[str, Any]]) -> Counter[str]:
    counts = Counter(event["level"] for event in events)
    logger.debug("Calculated level counts: %s", dict(counts))
    return counts


def print_summary(events: list[dict[str, Any]]) -> None:
    logger.info("Printing parser summary for %d valid events", len(events))
    print(f"Valid log lines: {len(events)}")
    print("Log levels:")
    for level, count in sorted(level_counts(events).items()):
        print(f"  {level:>8}: {count}")