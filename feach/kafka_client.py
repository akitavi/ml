import os
import json
import time
import threading
from typing import Callable, Optional

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

from logger import get_logger

logger = get_logger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "ohlcv-group")

MAX_RETRIES = int(os.getenv("KAFKA_MAX_RETRIES", "15"))
RETRY_INTERVAL = float(os.getenv("KAFKA_RETRY_INTERVAL", "3"))

_producer_lock = threading.Lock()
_producer: Optional[KafkaProducer] = None


def _get_producer() -> KafkaProducer:
    global _producer
    if _producer is not None:
        return _producer

    with _producer_lock:
        if _producer is not None:
            return _producer

        last_err: Optional[Exception] = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                _producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    request_timeout_ms=5000,
                    retries=5,
                    retry_backoff_ms=1000,
                    api_version_auto_timeout_ms=5000,
                )
                logger.info(f"Kafka producer connected to {KAFKA_BOOTSTRAP_SERVERS} (attempt {attempt})")
                return _producer
            except Exception as e:
                last_err = e
                logger.warning(f"Kafka producer connect failed (attempt {attempt}/{MAX_RETRIES}): {e}")
                time.sleep(RETRY_INTERVAL)

        raise RuntimeError(f"Kafka producer is not available: {last_err}")


def send_to_kafka(topic: str, message: dict, key: Optional[bytes] = None) -> None:
    producer = _get_producer()
    try:
        fut = producer.send(topic, value=message, key=key)
        fut.get(timeout=10)
        producer.flush(timeout=5)
        logger.info(f"Sent message to topic '{topic}'")
    except KafkaError as e:
        logger.exception(f"Failed to send message to Kafka topic '{topic}': {e}")
        raise


def start_consumer(
    topic: str,
    on_message: Callable[[dict], None],
    auto_offset_reset: str = "latest",
    run_in_thread: bool = True,
) -> None:
    """Start Kafka consumer."""

    def _consume():
        last_err: Optional[Exception] = None
        consumer: Optional[KafkaConsumer] = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    auto_offset_reset=auto_offset_reset,
                    enable_auto_commit=True,
                    group_id=KAFKA_GROUP_ID,
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                )
                logger.info(f"Consumer subscribed to topic: {topic}")
                break
            except Exception as e:
                last_err = e
                logger.warning(f"KafkaConsumer init failed (attempt {attempt}/{MAX_RETRIES}): {e}")
                time.sleep(RETRY_INTERVAL)

        if consumer is None:
            raise RuntimeError(f"KafkaConsumer initialization failed: {last_err}")

        for msg in consumer:
            try:
                logger.info(f"Received message from '{topic}'")
                on_message(msg.value)
            except Exception as e:
                logger.exception(f"Error while processing message from '{topic}': {e}")

    if run_in_thread:
        t = threading.Thread(target=_consume, daemon=True)
        t.start()
    else:
        _consume()


def wait_for_kafka(max_attempts: int = MAX_RETRIES, delay: float = RETRY_INTERVAL) -> None:
    from kafka import KafkaAdminClient

    last_err: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            client.close()
            logger.info(f"Kafka is available (attempt {attempt})")
            return
        except Exception as e:
            last_err = e
            logger.warning(f"Kafka not ready (attempt {attempt}/{max_attempts}): {e}")
            time.sleep(delay)

    raise RuntimeError(f"Kafka is not available after {max_attempts} attempts: {last_err}")
