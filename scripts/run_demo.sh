#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_DIR}"

if [ -x ".venv/bin/python" ]; then
  PYTHON=".venv/bin/python"
else
  PYTHON="python3"
fi

echo "==> Running unit tests"
"${PYTHON}" -m pytest

echo "==> Producing log events to Kafka"
"${PYTHON}" -m log_sentinel_python.ingester.producer

echo "==> Running Spark streaming (alerts to Postgres)"
"${PYTHON}" -m log_sentinel_python.evaluator.streaming --stream-seconds 30

echo "==> Demo complete"