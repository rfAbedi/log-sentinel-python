from log_sentinel_python.core.parser import parse_log_line, read_logs


def test_parse_log_line_returns_valid_json_object() -> None:
    line = (
        '{"timestamp":"2025-07-01T12:00:00Z","service":"payments",'
        '"level":"INFO","status_code":200,"latency_ms":42}'
    )

    event = parse_log_line(line)

    assert event is not None
    assert event["service"] == "payments"
    assert event["latency_ms"] == 42


def test_read_logs_skips_malformed_json_line(tmp_path) -> None:
    log_file = tmp_path / "logs.jsonl"
    log_file.write_text(
        """
{\"timestamp\":\"2025-07-01T12:00:00Z\",\"service\":\"payments\",\"level\":\"INFO\",\"status_code\":200,\"latency_ms\":42}
not valid json
""".lstrip(),
        encoding="utf-8",
    )

    events = read_logs(log_file)

    assert len(events) == 1
    assert events[0]["service"] == "payments"


def test_parse_log_line_rejects_missing_required_fields() -> None:
    line = '{"timestamp":"2025-07-01T12:00:00Z","level":"INFO"}'

    assert parse_log_line(line) is None
