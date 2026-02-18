import json
import time
import threading
from typing import Callable, Any, Dict, Optional

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from kafka.admin import KafkaAdminClient

import config
from logger import get_logger

logger = get_logger(__name__)

# Глобальное состояние продьюсера (процесс-локально)
producer: Optional[KafkaProducer] = None
kafka_enabled: bool = False


def _producer_kwargs() -> Dict[str, Any]:
    base = dict(getattr(config, "KAFKA_PRODUCER_CONFIG", {}) or {})
    base.setdefault("bootstrap_servers", config.KAFKA_BOOTSTRAP_SERVERS)
    base.setdefault("value_serializer", lambda v: json.dumps(v).encode("utf-8"))
    base.setdefault("request_timeout_ms", 5000)
    base.setdefault("retries", 5)
    base.setdefault("retry_backoff_ms", 1000)
    base.setdefault("api_version_auto_timeout_ms", 5000)
    return base


def init_kafka_producer() -> bool:
    """
    Инициализирует KafkaProducer с ретраями.
    Возвращает True если подключились.
    """
    global producer, kafka_enabled

    logger.info("Kafka init started bootstrap=%s", config.KAFKA_BOOTSTRAP_SERVERS)

    max_retries = int(getattr(config, "KAFKA_MAX_RETRIES", 15))
    retry_interval = float(getattr(config, "KAFKA_RETRY_INTERVAL", 3))

    last_err: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(**_producer_kwargs())
            kafka_enabled = True
            logger.info("Kafka connected bootstrap=%s attempt=%d", config.KAFKA_BOOTSTRAP_SERVERS, attempt)
            return True
        except KafkaError as e:
            last_err = e
            kafka_enabled = False
            producer = None
            logger.warning(
                "Kafka attempt %d/%d failed: %s",
                attempt, max_retries, e,
                exc_info=True,
            )
            time.sleep(retry_interval)
        except Exception as e:
            last_err = e
            kafka_enabled = False
            producer = None
            logger.exception("Unexpected error during Kafka init (attempt %d/%d)", attempt, max_retries)
            time.sleep(retry_interval)

    logger.error("Kafka connection failed after retries: %s", last_err)
    return False


def send_to_kafka(topic: str, message: dict) -> bool:
    """
    Отправляет сообщение в Kafka.
    Возвращает True/False по факту успеха.
    """
    global producer, kafka_enabled

    if not kafka_enabled or producer is None:
        logger.warning("Kafka producer not ready, trying to reinitialize...")
        init_kafka_producer()

    if not kafka_enabled or producer is None:
        logger.error("Kafka producer unavailable, message not sent.")
        return False

    try:
        # send() асинхронный; flush не делаем чтобы не тормозить API
        producer.send(topic, message)
        logger.debug("Sent message to topic=%s payload=%s", topic, message)
        return True
    except KafkaError as e:
        logger.error("KafkaError while sending to topic=%s: %s", topic, e, exc_info=True)
        return False
    except Exception as e:
        logger.exception("Unexpected error while sending to Kafka topic=%s: %s", topic, e)
        return False


def start_consumer(
    topic: str,
    on_message: Callable[[dict], None],
    auto_offset_reset: str = "latest",
    run_in_thread: bool = True,
):
    """
    :param topic: Kafka topic name
    :param on_message: callback(message_dict: dict)
    :param auto_offset_reset: "earliest" | "latest"
    :param run_in_thread: if True, runs in background thread
    """

    def _consume():
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset=auto_offset_reset,
                enable_auto_commit=True,
                group_id=config.KAFKA_GROUP_ID,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            logger.info(
                "Consumer subscribed topic=%s bootstrap=%s group_id=%s",
                topic, config.KAFKA_BOOTSTRAP_SERVERS, config.KAFKA_GROUP_ID,
            )
        except Exception:
            logger.exception("KafkaConsumer initialization failed")
            return

        for msg in consumer:
            try:
                on_message(msg.value)
            except Exception:
                logger.exception("Error processing message from topic=%s", topic)

    if run_in_thread:
        thread = threading.Thread(target=_consume, daemon=True, name=f"kafka-consumer-{topic}")
        thread.start()
        return thread

    _consume()
    return None


def wait_for_kafka(max_attempts: Optional[int] = None, delay: float = 1.0) -> None:
    attempts = int(max_attempts or getattr(config, "KAFKA_MAX_RETRIES", 15))

    for attempt in range(1, attempts + 1):
        try:
            client = KafkaAdminClient(bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS)
            client.close()
            logger.info("Kafka is available attempt=%d bootstrap=%s", attempt, config.KAFKA_BOOTSTRAP_SERVERS)
            return
        except Exception as e:
            logger.warning(
                "Kafka not ready (attempt %d/%d) bootstrap=%s err=%s",
                attempt, attempts, config.KAFKA_BOOTSTRAP_SERVERS, e,
            )
            time.sleep(delay)

    raise RuntimeError("Kafka is not available after several attempts")