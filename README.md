# Log Sentinel (Python)

Kafka log producer + Spark Structured Streaming that applies alert rules and persists triggered alerts to PostgreSQL.

## Setup

```bash
cp .env.example .env
# edit .env as needed
make up          # start Kafka + Postgres + Spark (docker compose)
pip install -e ".[dev,spark]"
```

## Run

```bash
make produce     # publish sample logs to Kafka
make stream      # Spark streaming: rules -> alerts in Postgres
make test        # run test suite
```

## Layout

- `src/log_sentinel_python/ingester/` — parser, Kafka producer
- `src/log_sentinel_python/evaluator/` — rules, Postgres repository, Spark streaming
- `config/rules.yaml` — alert rules
- `data/sample_logs_100.jsonl` — sample input
- `scripts/run_demo.sh` — end-to-end demo

## Environment

See `.env.example` for required variables (`KAFKA_*`, `POSTGRES_*`, `RULES_PATH`). No defaults are baked into the code.