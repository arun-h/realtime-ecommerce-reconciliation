"""
order_producer.py — Produces realistic order events with fault injection

FAULT INJECTION EXPLAINED:
Real systems have messy data. If your pipeline only handles clean data,
it's not production-ready. We intentionally produce:
  - Malformed events (missing required fields) → tests DLQ routing
  - Schema version mismatches → tests forward compatibility
  - Delayed events → tests watermark behavior in Spark

This is the "controlled chaos" that separates demo from real engineering proof.
"""

import random
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any

from producers.base_producer import BaseProducer, logger


# ── REALISTIC FAKE DATA ────────────────────────────────────────
# Using fixed lists gives us realistic variety without external dependencies.
# In a real system, this would pull from your actual customer/product database.

PRODUCT_CATALOG = [
    {"sku": "LAPTOP-PRO-15", "price": 1299.99, "category": "electronics"},
    {"sku": "WIRELESS-MOUSE", "price": 49.99,  "category": "accessories"},
    {"sku": "USB-C-HUB",     "price": 79.99,  "category": "accessories"},
    {"sku": "MONITOR-27-4K", "price": 549.99,  "category": "electronics"},
    {"sku": "MECH-KEYBOARD",  "price": 159.99,  "category": "accessories"},
    {"sku": "WEBCAM-HD-1080", "price": 89.99,   "category": "accessories"},
    {"sku": "HEADPHONES-NC",  "price": 299.99,  "category": "audio"},
    {"sku": "DESK-LAMP-LED",  "price": 39.99,   "category": "office"},
]

CURRENCIES = ["USD", "EUR", "GBP", "CAD"]

ORDER_STATUSES = ["CREATED", "CONFIRMED", "PROCESSING"]

EVENT_TYPES = ["ORDER_CREATED", "ORDER_CONFIRMED", "ORDER_UPDATED"]


class OrderProducer(BaseProducer):

    def __init__(self, fault_injection_rate: float = 0.05):
        """
        Args:
            fault_injection_rate: Probability of producing a malformed event.
                                  0.05 = 5% of events are intentionally bad.
                                  This matches realistic production failure rates.
        """
        super().__init__(topic="order-events")
        self.fault_injection_rate = fault_injection_rate

    def generate_event(
        self,
        order_id: str | None = None,
        customer_id: str | None = None,
        inject_fault: bool | None = None,
    ) -> dict[str, Any]:
        """
        Generate one realistic order event.

        ABOUT EVENT TIMESTAMPS:
        We generate event_timestamp in the PAST (0-300 seconds ago).
        WHY? Real events don't arrive at exactly the moment they happen.
        Network latency, processing delays, and retries mean events arrive
        after the fact. This tests our watermark logic in Spark —
        can it handle events that arrive slightly out of order?
        """
        order_id = order_id or f"ORD-{uuid.uuid4().hex[:10].upper()}"
        customer_id = customer_id or f"CUST-{random.randint(1000, 9999)}"

        # Simulate event happening 0-300 seconds in the past
        event_time = datetime.now(timezone.utc) - timedelta(
            seconds=random.uniform(0, 300)
        )

        # Choose a random product for this order
        product = random.choice(PRODUCT_CATALOG)
        quantity = random.randint(1, 5)
        total_amount = round(product["price"] * quantity, 2)

        # Decide whether to inject a fault
        should_fault = inject_fault if inject_fault is not None else (
            random.random() < self.fault_injection_rate
        )

        if should_fault:
            return self._generate_malformed_event(order_id)

        return {
            "order_id": order_id,
            "event_type": random.choice(EVENT_TYPES),
            "event_timestamp": event_time.isoformat(),
            "schema_version": "1.0",
            "customer_id": customer_id,
            "total_amount": total_amount,
            "currency": random.choice(CURRENCIES),
            "status": random.choice(ORDER_STATUSES),
            "items": [
                {
                    "sku": product["sku"],
                    "quantity": quantity,
                    "unit_price": product["price"],
                    "category": product["category"],
                }
            ],
            "shipping_address": {
                "country": random.choice(["US", "UK", "CA", "DE"]),
                "postal_code": f"{random.randint(10000, 99999)}",
            },
            "metadata": {
                "source": "web",
                "user_agent": "Mozilla/5.0",
                "session_id": uuid.uuid4().hex,
            }
        }

    def _generate_malformed_event(self, order_id: str) -> dict[str, Any]:
        """
        Intentionally broken events to test DLQ routing.
        We randomly pick from different failure modes to test multiple
        validation paths in our Spark job.
        """
        fault_type = random.choice([
            "missing_order_id",
            "invalid_amount",
            "missing_timestamp",
            "wrong_schema_version",
            "null_event_type",
        ])

        logger.debug(f"Injecting fault: {fault_type} for order {order_id}")

        base = {
            "order_id": order_id,
            "event_type": "ORDER_CREATED",
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "schema_version": "1.0",
            "total_amount": 99.99,
            "_injected_fault": fault_type,  # For debugging — labels the fault
        }

        if fault_type == "missing_order_id":
            del base["order_id"]
        elif fault_type == "invalid_amount":
            base["total_amount"] = "not-a-number"
        elif fault_type == "missing_timestamp":
            del base["event_timestamp"]
        elif fault_type == "wrong_schema_version":
            base["schema_version"] = "99.0"  # Far-future version our parser doesn't know
        elif fault_type == "null_event_type":
            base["event_type"] = None

        return base

    def run_continuous(self, events_per_second: float = 2.0, max_events: int = 1000):
        """
        Produce events continuously at a controlled rate.

        Args:
            events_per_second: Target production rate. 2.0 = 2 events/sec.
            max_events: Stop after this many events (prevents infinite loops in dev).
        """
        sleep_time = 1.0 / events_per_second
        produced = 0

        logger.info(
            json_log("producer_started", {
                "topic": self.topic,
                "rate": events_per_second,
                "fault_rate": self.fault_injection_rate,
                "max_events": max_events,
            })
        )

        try:
            while produced < max_events:
                event = self.generate_event()

                # Use order_id as Kafka key (routes same order to same partition)
                key = event.get("order_id", "unknown")

                self.send(key=key, payload=event)
                produced += 1

                time.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("Producer interrupted by user")
        finally:
            self.flush()
            logger.info(json_log("producer_finished", self.get_stats()))


def json_log(event: str, data: dict) -> str:
    """Helper to format structured log messages."""
    import json
    return json.dumps({"event": event, **data})


if __name__ == "__main__":
    import json
    producer = OrderProducer(fault_injection_rate=0.05)
    producer.run_continuous(events_per_second=2.0, max_events=500)
