"""
consumers/dlq_consumer.py — Real-time DLQ monitoring consumer

ROLE IN THE ARCHITECTURE:
This consumer reads from the Kafka dead-letter-events topic and provides
real-time structured logging of validation failures for monitoring and alerting.

IMPORTANT — SINGLE WRITE OWNERSHIP:
PySpark's write_dlq_to_postgres() in spark/streaming_job.py is the sole
authoritative writer to the PostgreSQL dead_letter_events table. This consumer
deliberately does NOT write to PostgreSQL — doing so would cause duplicate
inserts since every DLQ event is already written by Spark before being
published to Kafka. This consumer's role is observability only.
"""

import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
logger = logging.getLogger("dlq_consumer")

def run_dlq_consumer():
    """
    Main consumer loop. Reads from dead-letter-events topic continuously.

    CONSUMER CONFIG:
    - group.id: Consumer group name. Multiple consumers with the same group
      share the work (each partition goes to one consumer in the group).
      Since we only have one DLQ consumer, this just tracks our offset.
    - auto.offset.reset=earliest: On first start (no committed offset),
      read from the beginning of the topic. This means we never miss
      DLQ events that happened before this consumer started.
    - enable.auto.commit=False: We commit offsets MANUALLY after writing
      to Postgres. WHY? If auto-commit fires before the Postgres write
      succeeds, a crash between commit and write = lost DLQ event.
      Manual commit = at-least-once delivery to Postgres.
    """
    consumer_config = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "group.id": "dlq-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe(["dead-letter-events"])

    pg_conn = get_postgres_connection()

    # Graceful shutdown on Ctrl+C or SIGTERM
    running = True
    def handle_shutdown(signum, frame):
        nonlocal running
        logger.info("Shutdown signal received, finishing current batch...")
        running = False

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    logger.info(json.dumps({
        "event": "dlq_consumer_started",
        "topic": "dead-letter-events",
        "group_id": "dlq-consumer-group",
    }))

    processed = 0
    errors = 0

    try:
        while running:
            # poll(timeout=1.0): Wait up to 1 second for a message.
            # Returns None if no message arrived in that window.
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue  # No message, loop again

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Reached end of partition — not an error, just caught up
                    logger.debug(f"Reached end of partition: {msg.partition()}")
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    errors += 1
                continue

            # Decode the message
            raw_payload = msg.value().decode("utf-8") if msg.value() else ""

            # Try to extract failure reason from the payload
            # The Spark job writes structured JSON with a "failure_reason" field
            try:
                payload_dict = json.loads(raw_payload)
                failure_reason = payload_dict.get("failure_reason", "UNKNOWN")
                source_topic = payload_dict.get("source_topic", msg.topic())
            except json.JSONDecodeError:
                failure_reason = "INVALID_JSON"
                source_topic = msg.topic()

            logger.info(json.dumps({
                    "event": "dlq_event_received",
                    "source_topic": payload.get("source_topic", "unknown"),
                    "failure_reason": payload.get("failure_reason", "unknown"),
                    "batch_id": payload.get("batch_id"),
                    "note": "PostgreSQL write handled by Spark — this consumer is monitoring only"
                }))

    finally:
        consumer.close()
        pg_conn.close()
        logger.info(json.dumps({
            "event": "dlq_consumer_stopped",
            "total_processed": processed,
            "total_errors": errors,
        }))


if __name__ == "__main__":
    run_dlq_consumer()
