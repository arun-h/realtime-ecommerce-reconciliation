"""
dlq_consumer.py — Dead Letter Queue consumer

PURPOSE:
Consumes from the dead-letter-events Kafka topic and writes
failures to the PostgreSQL dead_letter_events table.

WHY TWO PLACES (Kafka topic + PostgreSQL table)?
  Kafka DLQ topic:   For replay and re-processing (fix the bug, replay the event)
  PostgreSQL table:  For querying, trending, alerting ("DLQ rate rising?")

They serve different purposes. In production you'd have both.
For this project: Spark writes to the Kafka DLQ topic.
This consumer reads from it and gives you queryable history.
"""

import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone

import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
logger = logging.getLogger("dlq_consumer")


def get_postgres_connection():
    """
    Create a PostgreSQL connection using environment variables.

    WHY NOT HARDCODE THE CONNECTION STRING?
    Same reason as before — secrets don't belong in code.
    In production, this would use a connection pool (psycopg2 pool
    or SQLAlchemy) rather than a single connection, but for our
    DLQ consumer (low volume) a single connection is fine.
    """
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB"),
    )


def write_to_dead_letter_table(
    conn,
    source_topic: str,
    raw_payload: str,
    failure_reason: str,
) -> None:
    """
    Write one DLQ event to PostgreSQL.

    NOTE: We use TEXT for raw_payload here, not JSONB.
    WHY? Because the event might not be valid JSON —
    that could be WHY it ended up in the DLQ.
    Storing as TEXT ensures we never lose the payload
    even if it's malformed.

    ON CONFLICT DO NOTHING:
    The dead_letter_events table uses BIGSERIAL (auto-increment) PK,
    so duplicates can't happen via key conflict. But if this consumer
    restarts and reprocesses Kafka messages (at-least-once), we could
    write the same DLQ event twice. The failure_reason + failed_at
    combination won't be identical enough to deduplicate automatically.
    For DLQ, this is acceptable — we'd rather have duplicates in the
    forensic log than miss events.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dead_letter_events
                (source_topic, raw_payload, failure_reason, failed_at)
            VALUES (%s, %s, %s, %s)
            """,
            (
                source_topic,
                raw_payload,
                failure_reason[:200],  # Truncate to column length
                datetime.now(timezone.utc),
            )
        )
    conn.commit()


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

            # Write to Postgres
            try:
                write_to_dead_letter_table(
                    conn=pg_conn,
                    source_topic=source_topic,
                    raw_payload=raw_payload,
                    failure_reason=failure_reason,
                )

                # Commit offset AFTER successful Postgres write
                # This is the manual commit for at-least-once delivery
                consumer.commit(asynchronous=False)

                processed += 1

                if processed % 10 == 0:
                    logger.info(json.dumps({
                        "event": "dlq_checkpoint",
                        "processed": processed,
                        "errors": errors,
                        "last_failure_reason": failure_reason,
                        "last_source_topic": source_topic,
                    }))

            except Exception as e:
                logger.error(json.dumps({
                    "event": "dlq_write_failed",
                    "error": str(e),
                    "raw_payload_preview": raw_payload[:100],
                }))
                errors += 1
                # Reconnect to Postgres if connection was lost
                try:
                    pg_conn = get_postgres_connection()
                except Exception as conn_err:
                    logger.error(f"Failed to reconnect to Postgres: {conn_err}")

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
