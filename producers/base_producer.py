"""
base_producer.py — Shared Kafka producer logic

WHY A BASE CLASS?
All three producers (order, inventory, payment) share the same
connection setup, error handling, and delivery confirmation logic.
Instead of copy-pasting that 3 times (which means 3 places to fix bugs),
we put it here once and inherit it.

This is the DRY principle: Don't Repeat Yourself.
"""

import json
import logging
import os
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Producer, KafkaException
from dotenv import load_dotenv

load_dotenv()

# ── STRUCTURED LOGGING SETUP ───────────────────────────────────
# We use JSON format so logs can be parsed programmatically.
# A human reads "message sent", a monitoring system reads JSON.
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
logger = logging.getLogger(__name__)


class BaseProducer(ABC):
    """
    Abstract base class for all Kafka producers.

    WHAT IS AN ABSTRACT CLASS?
    It's a class you can't instantiate directly — it's a template.
    Subclasses (OrderProducer, etc.) MUST implement the abstract methods.
    The shared logic (connect, send, close) lives here.
    """

    def __init__(self, topic: str):
        self.topic = topic
        self.producer = self._create_producer()
        self._messages_sent = 0
        self._messages_failed = 0

    def _create_producer(self) -> Producer:
        """
        Create and configure the Kafka producer.

        KEY CONFIG DECISIONS:
        - bootstrap.servers: Where Kafka lives. localhost:9092 for our Docker setup.
        - acks=all: Producer waits for ALL in-sync replicas to acknowledge.
          WHY? Prevents data loss if the broker crashes mid-write.
          TRADEOFF: Slightly slower than acks=1, but correct.
        - retries=3: Retry failed sends 3 times before giving up.
        - retry.backoff.ms=500: Wait 500ms between retries (don't hammer a sick broker).
        - linger.ms=5: Wait up to 5ms to batch messages together.
          WHY? Batching is more efficient than sending one message at a time.
          TRADEOFF: Adds up to 5ms latency. Acceptable for our use case.
        - enable.idempotence=True: Prevents duplicate messages from retries.
          WHY? Without this, a retry after a network blip could send the same
          message twice. With idempotence, Kafka deduplicates by sequence number.
        """
        config = {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 500,
            "linger.ms": 5,
            "enable.idempotence": True,
            # Compress messages to save network bandwidth and storage
            "compression.type": "snappy",
        }
        logger.info(f"Creating producer for topic={self.topic}")
        return Producer(config)

    def _delivery_callback(self, err, msg):
        """
        Called by Kafka for EVERY message after it's acknowledged (or failed).

        WHY A CALLBACK?
        Kafka producers are ASYNC by default — produce() returns immediately
        without waiting for broker confirmation. The callback fires later
        when the broker responds.

        This is where you'd plug in metrics or alerting in production.
        For our project: we log failures and track counts.
        """
        if err is not None:
            self._messages_failed += 1
            logger.error(
                json.dumps({
                    "event": "delivery_failed",
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "error": str(err),
                    "total_failed": self._messages_failed,
                })
            )
        else:
            self._messages_sent += 1
            if self._messages_sent % 100 == 0:  # Log every 100 messages, not every one
                logger.info(
                    json.dumps({
                        "event": "delivery_milestone",
                        "topic": msg.topic(),
                        "messages_sent": self._messages_sent,
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                    })
                )

    def send(self, key: str, payload: dict[str, Any]) -> None:
        """
        Serialize and send one message to Kafka.

        ABOUT THE KEY:
        Kafka uses the key to decide which PARTITION a message goes to.
        Same key → always same partition → ORDER PRESERVED for that key.
        We use order_id / payment_id as keys so all events for one order
        land in the same partition and are processed in order.

        ABOUT produce() vs. send():
        confluent-kafka uses produce(). It's non-blocking — returns before
        the broker has acknowledged. poll(0) tells the producer to check
        for delivery callbacks without blocking.
        """
        try:
            self.producer.produce(
                topic=self.topic,
                key=key.encode("utf-8"),
                value=json.dumps(payload).encode("utf-8"),
                on_delivery=self._delivery_callback,
                headers={
                    # Message-level metadata — visible in Kafka UI
                    "source": "ecommerce-platform",
                    "schema_version": "1.0",
                    "produced_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            # Process delivery callbacks for messages already in the queue
            self.producer.poll(0)

        except KafkaException as e:
            logger.error(f"Failed to produce message: {e}")
            raise
        except BufferError:
            # Producer's internal queue is full — flush and retry
            logger.warning("Producer queue full, flushing...")
            self.producer.flush(timeout=10)
            self.send(key, payload)  # Retry once after flush

    def flush(self) -> None:
        """
        Wait for ALL pending messages to be delivered.
        Call this before shutting down — otherwise buffered messages are lost.
        """
        logger.info(f"Flushing producer. Sent={self._messages_sent}, Failed={self._messages_failed}")
        self.producer.flush(timeout=30)

    def get_stats(self) -> dict:
        return {
            "topic": self.topic,
            "messages_sent": self._messages_sent,
            "messages_failed": self._messages_failed,
            "success_rate": (
                self._messages_sent / max(self._messages_sent + self._messages_failed, 1)
            )
        }

    @abstractmethod
    def generate_event(self, **kwargs) -> dict[str, Any]:
        """
        Each producer defines its own event shape.
        Subclasses MUST implement this method.
        """
        pass
