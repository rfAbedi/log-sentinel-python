import uuid
from pathlib import Path

import pytest
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

try:
    from testcontainers.community.kafka import KafkaContainer
    from testcontainers.community.postgres import PostgresContainer

    DOCKER_AVAILABLE = True
except Exception:
    KafkaContainer = None
    PostgresContainer = None
    DOCKER_AVAILABLE = False


@pytest.fixture(scope="session")
def spark_session():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("log-sentinel-tests")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def kafka_container():
    if not DOCKER_AVAILABLE:
        pytest.skip("testcontainers is not installed")

    container = KafkaContainer()
    try:
        container.start()
    except Exception as error:
        pytest.skip(f"Docker is required for integration tests: {error}")
    yield container
    container.stop()


@pytest.fixture(scope="session")
def kafka_bootstrap_servers(kafka_container):
    return kafka_container.get_bootstrap_server()


@pytest.fixture(scope="session")
def postgres_container():
    if not DOCKER_AVAILABLE:
        pytest.skip("testcontainers is not installed")

    container = PostgresContainer(
        "postgres:17-alpine",
        username="log_sentinel",
        password="log_sentinel",
        dbname="log_sentinel",
    )
    try:
        container.start()
    except Exception as error:
        pytest.skip(f"Docker is required for integration tests: {error}")
    yield container
    container.stop()


@pytest.fixture
def kafka_topic(kafka_bootstrap_servers, monkeypatch):
    from log_sentinel_python.ingester import producer

    topic = f"log-events-{uuid.uuid4().hex}"
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", kafka_bootstrap_servers)
    monkeypatch.setenv("KAFKA_TOPIC", topic)
    monkeypatch.setenv("KAFKA_CONSUMER_GROUP", f"log-sentinel-{uuid.uuid4().hex}")
    monkeypatch.setattr(producer, "TOPIC", topic)

    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap_servers)
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()

    return topic


@pytest.fixture
def postgres_env(postgres_container, monkeypatch):
    params = {
        "host": postgres_container.get_container_host_ip(),
        "port": postgres_container.get_exposed_port(5432),
        "dbname": postgres_container.dbname,
        "user": postgres_container.username,
        "password": postgres_container.password,
    }

    monkeypatch.setenv("POSTGRES_HOST", str(params["host"]))
    monkeypatch.setenv("POSTGRES_PORT", str(params["port"]))
    monkeypatch.setenv("POSTGRES_DB", params["dbname"])
    monkeypatch.setenv("POSTGRES_USER", params["user"])
    monkeypatch.setenv("POSTGRES_PASSWORD", params["password"])

    return params


@pytest.fixture
def rules_path():
    return Path(__file__).resolve().parents[1] / "config" / "rules.yaml"