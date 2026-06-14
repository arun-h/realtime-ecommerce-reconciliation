"""
spark/validators.py — Business rule validation for all three event types

TWO-LAYER VALIDATION STRATEGY:
Layer 1 — Spark StructType (in schemas.py):
    Runs in JVM. Fast. Catches structural problems:
    missing required fields, wrong types, unparseable JSON.
    Events that fail here are null rows — Spark drops them automatically.

Layer 2 — This file (Python UDFs):
    Runs in Python worker processes. Catches business rule violations:
    negative amounts, unknown event types, future timestamps,
    impossible quantity values, unrecognised gateways.

PERFORMANCE TRADEOFF DOCUMENTED:
Python UDFs cross the JVM → Python serialisation boundary once per row.
At this event volume (~5 events/sec) the overhead is acceptable.
At production scale (>1M events/day), this layer would move upstream
to the producer via a schema registry, or be rewritten as a Spark
SQL expression (stays in JVM, no serialisation cost).

This tradeoff is explicit and documented. It is not an oversight.
"""

import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from pyspark.sql.functions import udf, col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

logger = logging.getLogger(__name__)

# ── VALID VALUE SETS ───────────────────────────────────────────
# Defined once here, not scattered across validator functions.
# If a producer adds a new event type, add it here — one place.

VALID_ORDER_EVENT_TYPES = {
    "ORDER_CREATED", "ORDER_CONFIRMED", "ORDER_UPDATED",
    "ORDER_CANCELLED", "ORDER_SHIPPED", "ORDER_DELIVERED"
}

VALID_INVENTORY_EVENT_TYPES = {
    "INVENTORY_RESERVED", "INVENTORY_RELEASED",
    "INVENTORY_ADJUSTED", "INVENTORY_RESTOCKED"
}

VALID_PAYMENT_EVENT_TYPES = {
    "PAYMENT_INITIATED", "PAYMENT_CONFIRMED",
    "PAYMENT_FAILED", "PAYMENT_CANCELLED",
    "PAYMENT_DISPUTED", "PAYMENT_REFUNDED"
}

VALID_GATEWAYS = {"STRIPE", "PAYPAL", "ADYEN", "SQUARE"}

VALID_CURRENCIES = {"USD", "EUR", "GBP", "CAD", "AUD", "SGD", "INR"}

# Maximum clock skew we tolerate — events this far in the future
# are rejected as likely clock skew or test data errors
MAX_FUTURE_SKEW_SECONDS = 300  # 5 minutes


# ── VALIDATION RESULT SCHEMA ───────────────────────────────────
# Each validator returns this shape:
#   is_valid: bool   — True = process normally, False = route to DLQ
#   reason:   str    — if invalid, why (empty string if valid)
#
# WHY RETURN REASON INSTEAD OF RAISING EXCEPTION?
# Raising an exception inside a UDF crashes the Spark task.
# Returning a structured result lets the streaming job decide
# what to do — route to DLQ, log, alert — without crashing.

VALIDATION_RESULT_SCHEMA = StructType([
    StructField("is_valid", BooleanType(), nullable=False),
    StructField("reason",   StringType(),  nullable=False),
])


def _parse_timestamp(ts_string: Optional[str]) -> Optional[datetime]:
    """
    Parse ISO 8601 timestamp string to datetime object.
    Returns None if parsing fails — caller handles the None case.
    """
    if not ts_string:
        return None
    try:
        # Handle both with and without timezone info
        if ts_string.endswith('Z'):
            ts_string = ts_string[:-1] + '+00:00'
        return datetime.fromisoformat(ts_string)
    except (ValueError, TypeError):
        return None


def _is_future_timestamp(ts_string: Optional[str]) -> bool:
    """
    Returns True if timestamp is more than MAX_FUTURE_SKEW_SECONDS
    in the future. Used to catch clock skew events.
    """
    dt = _parse_timestamp(ts_string)
    if dt is None:
        return False  # Can't parse = not a future timestamp problem
    now = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt > now + timedelta(seconds=MAX_FUTURE_SKEW_SECONDS)


# ── ORDER EVENT VALIDATOR ──────────────────────────────────────
def _validate_order_event(
    order_id:        Optional[str],
    event_type:      Optional[str],
    event_timestamp: Optional[str],
    total_amount:    Optional[float],
    schema_version:  Optional[str],
) -> dict:
    """
    Validates business rules for order events.
    Structural validation (field presence/type) already handled by StructType.
    """
    # Required field checks — these should be caught by StructType
    # but we double-check because nullable=False in Spark is advisory
    # when reading from JSON (Spark may still produce nulls)
    if not order_id or not order_id.strip():
        return {"is_valid": False, "reason": "MISSING_ORDER_ID"}

    if not event_type:
        return {"is_valid": False, "reason": "MISSING_EVENT_TYPE"}

    if not event_timestamp:
        return {"is_valid": False, "reason": "MISSING_TIMESTAMP"}

    # Business rule: event type must be from known set
    if event_type not in VALID_ORDER_EVENT_TYPES:
        return {"is_valid": False, "reason": f"UNKNOWN_EVENT_TYPE:{event_type}"}

    # Business rule: amount must be positive if present
    if total_amount is not None and total_amount <= 0:
        return {"is_valid": False, "reason": f"INVALID_AMOUNT:{total_amount}"}

    # Business rule: timestamp must be parseable
    if _parse_timestamp(event_timestamp) is None:
        return {"is_valid": False, "reason": f"UNPARSEABLE_TIMESTAMP:{event_timestamp}"}

    # Business rule: reject future timestamps beyond clock skew tolerance
    if _is_future_timestamp(event_timestamp):
        return {"is_valid": False, "reason": f"FUTURE_TIMESTAMP:{event_timestamp}"}

    # Business rule: schema version must be known
    if schema_version and schema_version not in ("1.0", "1.1", "2.0"):
        return {"is_valid": False, "reason": f"UNKNOWN_SCHEMA_VERSION:{schema_version}"}

    return {"is_valid": True, "reason": ""}


# ── INVENTORY EVENT VALIDATOR ──────────────────────────────────
def _validate_inventory_event(
    inventory_id:  Optional[str],
    event_type:    Optional[str],
    occurred_at:   Optional[str],   # ← note: occurred_at, not event_timestamp
    sku:           Optional[str],
    quantity_delta: Optional[int],
) -> dict:
    if not inventory_id or not inventory_id.strip():
        return {"is_valid": False, "reason": "MISSING_INVENTORY_ID"}

    if not event_type:
        return {"is_valid": False, "reason": "MISSING_EVENT_TYPE"}

    if not occurred_at:
        return {"is_valid": False, "reason": "MISSING_OCCURRED_AT"}

    if not sku or not sku.strip():
        return {"is_valid": False, "reason": "MISSING_SKU"}

    if event_type not in VALID_INVENTORY_EVENT_TYPES:
        return {"is_valid": False, "reason": f"UNKNOWN_EVENT_TYPE:{event_type}"}

    # Business rule: quantity_delta must be present and non-zero
    if quantity_delta is None:
        return {"is_valid": False, "reason": "MISSING_QUANTITY_DELTA"}

    if quantity_delta == 0:
        return {"is_valid": False, "reason": "ZERO_QUANTITY_DELTA"}

    # Business rule: quantity bounds — no single event should move >10,000 units
    if abs(quantity_delta) > 10000:
        return {"is_valid": False, "reason": f"QUANTITY_DELTA_OUT_OF_BOUNDS:{quantity_delta}"}

    if _parse_timestamp(occurred_at) is None:
        return {"is_valid": False, "reason": f"UNPARSEABLE_TIMESTAMP:{occurred_at}"}

    if _is_future_timestamp(occurred_at):
        return {"is_valid": False, "reason": f"FUTURE_TIMESTAMP:{occurred_at}"}

    return {"is_valid": True, "reason": ""}


# ── PAYMENT EVENT VALIDATOR ────────────────────────────────────
def _validate_payment_event(
    payment_id:      Optional[str],
    event_type:      Optional[str],
    event_timestamp: Optional[str],
    order_id:        Optional[str],
    amount:          Optional[float],
    gateway:         Optional[str],
) -> dict:
    if not payment_id or not payment_id.strip():
        return {"is_valid": False, "reason": "MISSING_PAYMENT_ID"}

    if not event_type:
        return {"is_valid": False, "reason": "MISSING_EVENT_TYPE"}

    if not event_timestamp:
        return {"is_valid": False, "reason": "MISSING_TIMESTAMP"}

    if event_type not in VALID_PAYMENT_EVENT_TYPES:
        return {"is_valid": False, "reason": f"UNKNOWN_EVENT_TYPE:{event_type}"}

    # Business rule: amount must be positive for confirmed payments
    if event_type == "PAYMENT_CONFIRMED":
        if amount is None or amount <= 0:
            return {"is_valid": False, "reason": f"INVALID_AMOUNT_FOR_CONFIRMED:{amount}"}

    # Business rule: amount sanity check — no single payment > $50,000
    if amount is not None and amount > 50000:
        return {"is_valid": False, "reason": f"AMOUNT_EXCEEDS_LIMIT:{amount}"}

    # Business rule: negative amount is always wrong
    if amount is not None and amount < 0:
        return {"is_valid": False, "reason": f"NEGATIVE_AMOUNT:{amount}"}

    if gateway and gateway not in VALID_GATEWAYS:
        return {"is_valid": False, "reason": f"UNKNOWN_GATEWAY:{gateway}"}

    if _parse_timestamp(event_timestamp) is None:
        return {"is_valid": False, "reason": f"UNPARSEABLE_TIMESTAMP:{event_timestamp}"}

    if _is_future_timestamp(event_timestamp):
        return {"is_valid": False, "reason": f"FUTURE_TIMESTAMP:{event_timestamp}"}

    return {"is_valid": True, "reason": ""}


# ── REGISTER SPARK UDFs ────────────────────────────────────────
# These wrap the Python functions above as Spark UDFs.
# Called in streaming_job.py on each micro-batch.
#
# returnType=VALIDATION_RESULT_SCHEMA means Spark expects a dict
# with keys "is_valid" (bool) and "reason" (str).

validate_order_udf = udf(
    lambda order_id, event_type, event_timestamp, total_amount, schema_version:
        _validate_order_event(order_id, event_type, event_timestamp,
                              total_amount, schema_version),
    returnType=VALIDATION_RESULT_SCHEMA
)

validate_inventory_udf = udf(
    lambda inventory_id, event_type, occurred_at, sku, quantity_delta:
        _validate_inventory_event(inventory_id, event_type, occurred_at,
                                  sku, quantity_delta),
    returnType=VALIDATION_RESULT_SCHEMA
)

validate_payment_udf = udf(
    lambda payment_id, event_type, event_timestamp, order_id, amount, gateway:
        _validate_payment_event(payment_id, event_type, event_timestamp,
                                order_id, amount, gateway),
    returnType=VALIDATION_RESULT_SCHEMA
)