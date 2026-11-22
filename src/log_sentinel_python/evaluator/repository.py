import json
import os
from typing import Any

from log_sentinel_python.utils.loggers import get_logger


logger = get_logger(__name__)


def _pg_connection() -> Any:
    logger.debug("Connecting to PostgreSQL")
    import psycopg2

    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def _ensure_alerts_table(connection: Any) -> None:
    logger.debug("Ensuring alerts table exists")
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id SERIAL PRIMARY KEY,
                rule_name TEXT NOT NULL,
                service TEXT,
                field TEXT,
                actual_value TEXT,
                expected_value TEXT,
                event JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
    connection.commit()


def _persist_alerts_batch(batch_df: Any, batch_id: int) -> None:
    if batch_df.isEmpty():
        logger.debug("Batch %d is empty, skipping", batch_id)
        return

    import psycopg2.extras

    rows = batch_df.collect()
    logger.debug("Batch %d collected %d alert row(s)", batch_id, len(rows))
    alerts = [
        {
            "rule_name": row.rule_name,
            "service": row.service,
            "field": row.field,
            "actual_value": row.actual_value,
            "expected_value": row.expected_value,
            "event": json.loads(row.event_json),
        }
        for row in rows
    ]

    connection = _pg_connection()
    try:
        _ensure_alerts_table(connection)
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO alerts (rule_name, service, field, actual_value, expected_value, event)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        alert["rule_name"],
                        alert["service"],
                        alert["field"],
                        alert["actual_value"],
                        alert["expected_value"],
                        psycopg2.extras.Json(alert["event"]),
                    )
                    for alert in alerts
                ],
            )
        connection.commit()
        logger.info("Batch %d: persisted %d alert(s) to PostgreSQL", batch_id, len(alerts))
    finally:
        connection.close()