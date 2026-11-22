PYTHON ?= .venv/bin/python

.PHONY: up down test produce stream logs clean

up:
	docker compose up -d

down:
	docker compose down

test:
	$(PYTHON) -m pytest

produce:
	$(PYTHON) -m log_sentinel_python.ingester.producer

stream:
	$(PYTHON) -m log_sentinel_python.evaluator.streaming --stream-seconds 30

logs:
	docker compose logs -f

clean:
	docker compose down -v
	find . -type d -name __pycache__ -prune -exec rm -rf {} +