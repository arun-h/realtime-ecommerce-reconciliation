"""
inventory_producer.py — Produces inventory events with intentional schema differences

INTENTIONAL SCHEMA DIFFERENCES FROM ORDER EVENTS:
Real e-commerce systems have inventory managed by a SEPARATE team
with a SEPARATE service. They don't coordinate schemas with the orders team.
This creates real conflicts our Spark reconciler must handle:

  Orders team uses:    "event_timestamp"
  Inventory team uses: "occurred_at"       ← different field name, same concept

  Orders team uses:    "total_amount" (float)
  Inventory team uses: "quantity_delta" (integer, positive or negative)

  Orders team uses:    "customer_id"
  Inventory team uses: no customer concept ← inventory doesn't know about customers

This is the schema reconciliation problem our pipeline is built to solve.
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any

from producers.base_producer import BaseProducer, logger


WAREHOUSE_IDS = ["WH-EAST-01", "WH-WEST-02", "WH-CENTRAL-03"]

INVENTORY_EVENT_TYPES = [
    "INVENTORY_RESERVED",    # Stock held for a pending order
    "INVENTORY_RELEASED",    # Hold released (order cancelled)
    "INVENTORY_ADJUSTED",    # Manual stock correction
    "INVENTORY_RESTOCKED",   # New stock arrived
]

# Deliberately reusing SKUs from order producer so reconciliation can JOIN them
SKUS = [
    "LAPTOP-PRO-15", "WIRELESS-MOUSE", "USB-C-HUB",
    "MONITOR-27-4K", "MECH-KEYBOARD", "WEBCAM-HD-1080",
    "HEADPHONES-NC", "DESK-LAMP-LED",
]


class InventoryProducer(BaseProducer):

    def __init__(self, fault_injection_rate: float = 0.03):
        """
        Lower fault rate than orders (0.03 vs 0.05) because
        inventory systems tend to be more reliable than order webhooks.
        This makes our DLQ stats realistic.
        """
        super().__init__(topic="inventory-events")
        self.fault_injection_rate = fault_injection_rate

    def generate_event(
        self,
        order_id: str | None = None,
        sku: str | None = None,
        inject_fault: bool | None = None,
    ) -> dict[str, Any]:
        """
        NOTICE: This schema uses "occurred_at" not "event_timestamp".
        Our Spark schema reconciler must normalize this to a common field name.
        This is a real problem — Spark will not automatically know these mean
        the same thing. The reconciler must explicitly map them.
        """
        inventory_id = f"INV-{uuid.uuid4().hex[:10].upper()}"
        sku = sku or random.choice(SKUS)

        # Simulate events happening 0-120 seconds in the past
        # SHORTER lag than orders (inventory systems update faster)
        occurred_time = datetime.now(timezone.utc) - timedelta(
            seconds=random.uniform(0, 120)
        )

        event_type = random.choice(INVENTORY_EVENT_TYPES)

        # Quantity delta logic:
        # RESERVED/RELEASED = negative/positive small numbers (1-5 units)
        # RESTOCKED = large positive numbers (10-100 units)
        if event_type == "INVENTORY_RESTOCKED":
            quantity_delta = random.randint(10, 100)
        elif event_type in ("INVENTORY_RESERVED",):
            quantity_delta = -random.randint(1, 5)
        elif event_type in ("INVENTORY_RELEASED",):
            quantity_delta = random.randint(1, 5)
        else:  # ADJUSTED
            quantity_delta = random.randint(-10, 10)

        should_fault = inject_fault if inject_fault is not None else (
            random.random() < self.fault_injection_rate
        )

        if should_fault:
            return self._generate_malformed_event(inventory_id)

        return {
            "inventory_id": inventory_id,
            "event_type": event_type,

            # ← SCHEMA DIFFERENCE: "occurred_at" not "event_timestamp"
            # This is intentional. The Spark reconciler normalizes this.
            "occurred_at": occurred_time.isoformat(),

            "schema_version": "1.0",
            "sku": sku,
            "quantity_delta": quantity_delta,       # ← int, not float like amount
            "warehouse_id": random.choice(WAREHOUSE_IDS),
            "current_stock_level": random.randint(0, 500),  # Inventory has this; orders don't

            # order_id links this to an order — but it's OPTIONAL.
            # RESTOCKED events have no associated order.
            "order_id": order_id if event_type == "INVENTORY_RESERVED" else None,

            "metadata": {
                "operator_id": f"SYS-{random.randint(1, 5)}",  # Automated system ID
                "reason": random.choice(["SALE", "MANUAL", "RETURN", "CORRECTION"]),
            }
        }

    def _generate_malformed_event(self, inventory_id: str) -> dict[str, Any]:
        fault_type = random.choice([
            "missing_sku",
            "invalid_quantity",  # String instead of int — type mismatch
            "missing_occurred_at",
            "unknown_warehouse",
        ])

        logger.debug(f"Injecting inventory fault: {fault_type}")

        base = {
            "inventory_id": inventory_id,
            "event_type": "INVENTORY_RESERVED",
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": "1.0",
            "sku": "LAPTOP-PRO-15",
            "quantity_delta": -1,
            "_injected_fault": fault_type,
        }

        if fault_type == "missing_sku":
            del base["sku"]
        elif fault_type == "invalid_quantity":
            base["quantity_delta"] = "many"   # String where int expected
        elif fault_type == "missing_occurred_at":
            del base["occurred_at"]
        elif fault_type == "unknown_warehouse":
            base["warehouse_id"] = "WH-DOESNT-EXIST"

        return base

    def run_continuous(self, events_per_second: float = 1.5, max_events: int = 1000):
        """
        Slightly lower rate than orders (1.5 vs 2.0/sec) because
        not every order triggers an inventory event immediately.
        """
        sleep_time = 1.0 / events_per_second
        produced = 0

        logger.info(json.dumps({
            "event": "inventory_producer_started",
            "topic": self.topic,
            "rate": events_per_second,
            "fault_rate": self.fault_injection_rate,
        }))

        try:
            while produced < max_events:
                event = self.generate_event()
                key = event.get("inventory_id", "unknown")
                self.send(key=key, payload=event)
                produced += 1
                time.sleep(sleep_time)
        except KeyboardInterrupt:
            logger.info("Inventory producer interrupted")
        finally:
            self.flush()
            logger.info(json.dumps({"event": "inventory_producer_finished", **self.get_stats()}))


if __name__ == "__main__":
    producer = InventoryProducer(fault_injection_rate=0.03)
    producer.run_continuous(events_per_second=1.5, max_events=500)
