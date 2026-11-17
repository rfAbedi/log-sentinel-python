from pathlib import Path
import sys

from log_sentinel_python.core.parser import read_logs, print_summary
from log_sentinel_python.core.rules import load_rules, evaluate_rules

from log_sentinel_python.utils.loggers import get_logger


logger = get_logger(__name__)

def main() -> None:
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data/sample_logs_100.jsonl")
    logger.info("Parser service starting with input file: %s", path)
    events = read_logs(path)
    print_summary(events)
    logger.info("Parser service completed successfully")
    
    logger.info("Loading rules from configuration file")
    rules = load_rules(Path("config/rules.yaml"))
    logger.info("Evaluating rules against log events")
    for event_number, event in enumerate(events, start=1):
        logger.debug("Evaluating event %d: %s", event_number, event)
        matched_rules = evaluate_rules(event, rules)
        if matched_rules:
            logger.info("Event %d matched %d rules", event_number, len(matched_rules))
            for rule in matched_rules:
                logger.info("  Matched rule: %s", rule["name"])
        else:
            logger.debug("Event %d did not match any rules", event_number)
    logger.info("Rule evaluation completed successfully")


if __name__ == "__main__":
    main()
