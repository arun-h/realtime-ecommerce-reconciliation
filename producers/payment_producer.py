"""
payment_producer.py — Produces payment webhook events with duplicate injection

WHY PAYMENTS HAVE THE HIGHEST DUPLICATE RATE:
Payment gateways (Stripe, PayPal) use webhook retry logic.
If your endpoint doesn't respond within 30 seconds, they retry.
If your server restarts mid-request, they retry.
In production, duplicate payment webhooks happen on 2-8% of transactions.

This is NOT a bug in Stripe — it's a deliberate design choice.
Idempotency is YOUR responsibility as the receiver.

Our fault injection rate for payments is 0.08 (8%) — higher than
orders (5%) and inventory (3%) — to reflect this real-world pattern.
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any

from producers.base_producer import BaseProducer, logger


PAYMENT_GATEWAYS = ["STRIPE", "PAYPAL", "ADYEN", "SQUARE"]

PAYMENT_EVENT_TYPES = [
    "PAYMENT_INITIATED",     # Customer clicked "Pay"
    "PAYMENT_CONFIRMED",     # Gateway confirmed funds captured
    "PAYMENT_FAILED",        # Card declined / insufficient funds
    "PAYMENT_REFUNDED",      # Merchant initiated refund
]

FAILURE_REASONS = [
    "INSUFFICIENT_FUNDS",
    "CARD_EXPIRED",
    "FRAUD_SUSPECTED",
    "NETWORK_ERROR",
    "LIMIT_EXCEEDED",
]


class PaymentProducer(BaseProducer):

    def __init__(self, fault_injection_rate: float = 0.08):
        super().__init__(topic="payment-events")
        self.fault_injection_rate = fault_injection_rate

        # Track payment_ids we've recently produced so we can
        # deliberately re-send them (simulating gateway retries)
        self._recent_payment_ids: list[str] = []
        self._duplicate_rate = 0.05  # 5% of sends are deliberate duplicates

    def generate_event(
        self,
        order_id: str | None = None,
        inject_fault: bool | None = None,
        force_duplicate: bool = False,
    ) -> dict[str, Any]:
        """
        DUPLICATE SIMULATION:
        When force_duplicate=True, we reuse a recent payment_id.
        The event content is identical to the original.
        Our PostgreSQL upsert must handle this without creating
        duplicate records — that's the idempotency test.
        """
        order_id = order_id or f"ORD-{uuid.uuid4().hex[:10].upper()}"

        # DUPLICATE LOGIC:
        # If we have recent payment IDs AND randomly decide to duplicate
        if force_duplicate and self._recent_payment_ids:
            payment_id = random.choice(self._recent_payment_ids)
            logger.debug(f"Injecting duplicate payment: {payment_id}")
        else:
            payment_id = f"PAY-{uuid.uuid4().hex[:12].upper()}"
            # Track for potential future duplication
            self._recent_payment_ids.append(payment_id)
            if len(self._recent_payment_ids) > 50:  # Keep only last 50
                self._recent_payment_ids.pop(0)

        # Payment events arrive with MORE latency than orders
        # because they go through external gateway → your webhook endpoint
        event_time = datetime.now(timezone.utc) - timedelta(
            seconds=random.uniform(10, 180)  # 10s to 3min lag (gateway processing)
        )

        event_type = random.choice(PAYMENT_EVENT_TYPES)

        should_fault = inject_fault if inject_fault is not None else (
            random.random() < self.fault_injection_rate
        )

        if should_fault:
            return self._generate_malformed_event(payment_id, order_id)

        event = {
            "payment_id": payment_id,
            "event_type": event_type,
            "event_timestamp": event_time.isoformat(),
            "schema_version": "1.0",
            "order_id": order_id,
            "amount": round(random.uniform(10.0, 2000.0), 2),
            "currency": random.choice(["USD", "EUR", "GBP"]),
            "gateway": random.choice(PAYMENT_GATEWAYS),
            "status": "SUCCESS" if event_type == "PAYMENT_CONFIRMED" else "PENDING",

            # Gateway-specific fields that orders/inventory don't have
            "gateway_transaction_id": f"txn_{uuid.uuid4().hex[:16]}",
            "gateway_fee": round(random.uniform(0.30, 5.00), 2),

            # Failure context — only populated for failed payments
            "failure_reason": (
                random.choice(FAILURE_REASONS) if event_type == "PAYMENT_FAILED" else None
            ),

            "metadata": {
                "gateway_webhook_id": uuid.uuid4().hex,  # Unique per webhook delivery
                "retry_count": 0,                         # 0 = first delivery attempt
                "ip_address": f"192.168.{random.randint(1,255)}.{random.randint(1,255)}",
            }
        }

        return event

    def _generate_malformed_event(self, payment_id: str, order_id: str) -> dict[str, Any]:
        fault_type = random.choice([
            "missing_payment_id",
            "amount_as_string",
            "missing_order_reference",
            "invalid_gateway",
            "future_timestamp",        # Timestamp in the future (clock skew)
        ])

        logger.debug(f"Injecting payment fault: {fault_type}")

        base = {
            "payment_id": payment_id,
            "event_type": "PAYMENT_CONFIRMED",
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "schema_version": "1.0",
            "order_id": order_id,
            "amount": 99.99,
            "gateway": "STRIPE",
            "_injected_fault": fault_type,
        }

        if fault_type == "missing_payment_id":
            del base["payment_id"]
        elif fault_type == "amount_as_string":
            base["amount"] = "$99.99"   # String with currency symbol
        elif fault_type == "missing_order_reference":
            del base["order_id"]
        elif fault_type == "invalid_gateway":
            base["gateway"] = ""        # Empty string
        elif fault_type == "future_timestamp":
            # Event claiming to be from 10 minutes in the future
            # Tests Spark's handling of clock skew
            future = datetime.now(timezone.utc) + timedelta(minutes=10)
            base["event_timestamp"] = future.isoformat()

        return base

    def run_continuous(self, events_per_second: float = 1.0, max_events: int = 1000):
        """
        Lower rate than orders — not every order action triggers a payment event.
        Includes automatic duplicate injection at self._duplicate_rate.
        """
        sleep_time = 1.0 / events_per_second
        produced = 0
        duplicates_sent = 0

        logger.info(json.dumps({
            "event": "payment_producer_started",
            "topic": self.topic,
            "rate": events_per_second,
            "fault_rate": self.fault_injection_rate,
            "duplicate_rate": self._duplicate_rate,
        }))

        try:
            while produced < max_events:
                # Decide: send a duplicate or a fresh event?
                is_duplicate = (
                    random.random() < self._duplicate_rate
                    and len(self._recent_payment_ids) > 0
                )

                event = self.generate_event(force_duplicate=is_duplicate)
                key = event.get("payment_id", "unknown")
                self.send(key=key, payload=event)

                if is_duplicate:
                    duplicates_sent += 1

                produced += 1
                time.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("Payment producer interrupted")
        finally:
            self.flush()
            logger.info(json.dumps({
                "event": "payment_producer_finished",
                "duplicates_sent": duplicates_sent,
                **self.get_stats()
            }))


if __name__ == "__main__":
    producer = PaymentProducer(fault_injection_rate=0.08)
    producer.run_continuous(events_per_second=1.0, max_events=500)
