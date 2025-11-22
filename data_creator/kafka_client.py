import os
import json
import time
import threading
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError


from logger import get_logger
logger = get_logger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "ohlcv-group")
MAX_RETRIES = 10

producer = None
kafka_enabled = False


def init_kafka_producer():
    global producer, kafka_enabled
    logger.info("Kafka init started")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=5000,
                retries=0,
                retry_backoff_ms=1000,
            )
            kafka_enabled = True
            logger.info(f"Kafka connected to {KAFKA_BOOTSTRAP_SERVERS} (attempt {attempt})")
            break
        except KafkaError as e:
            logger.warning(f"Kafka attempt {attempt}/{MAX_RETRIES} failed: {e}")
            time.sleep(1)
    else:
        logger.error("Kafka connection failed after retries")


def send_to_kafka(topic: str, message: dict):
    global producer, kafka_enabled

    if not kafka_enabled or producer is None:
        logger.warning("Kafka producer not ready, trying to reinitialize...")
        init_kafka_producer()

    if kafka_enabled and producer:
        try:
            producer.send(topic, message)
            logger.info(f"Sent message to topic '{topic}': {message}")
        except KafkaError as e:
            logger.error(f"KafkaError while sending to topic '{topic}': {e}")
        except Exception as e:
            logger.exception(f"Unexpected error while sending to Kafka topic '{topic}': {e}")
    else:
        logger.error("Kafka producer unavailable, message not sent.")


# --- Consumer section ---
def start_consumer(topic: str, on_message, auto_offset_reset="latest", run_in_thread=True):
    """
    :param topic: Kafka topic name
    :param on_message: function(message_dict: dict) — callback
    :param auto_offset_reset: "earliest" | "latest"
    :param run_in_thread: if True, runs in background thread
    """

    def _consume():
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
        except Exception as e:
            logger.exception("KafkaConsumer initialization failed")
            return

        for message in consumer:
            try:
                logger.info(f"Received message from '{topic}'")
                on_message(message.value)
            except Exception as msg_err:
                logger.error(f"Error processing message: {msg_err}")

    if run_in_thread:
        thread = threading.Thread(target=_consume, daemon=True)
        thread.start()
    else:
        _consume()

def wait_for_kafka(max_attempts=MAX_RETRIES, delay=1):
    from kafka import KafkaAdminClient
    for attempt in range(1, max_attempts + 1):
        try:
            client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            client.close()
            logger.info(f"Kafka is available on attempt {attempt}")
            return
        except Exception as e:
            logger.error(f"Kafka not ready (attempt {attempt}/{max_attempts}): {e}")
            time.sleep(delay)
    raise RuntimeError("Kafka is not available after several attempts")