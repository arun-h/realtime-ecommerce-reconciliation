"""

The validator functions in spark/validators.py contain the business rules
that determine whether an event is valid or routes to the dead-letter queue.
These are pure Python functions with no Spark dependency — they can be
tested without a running Spark session, Docker, or Kafka.

These tests verify:
- Valid events pass through correctly
- Each failure mode produces the correct failure reason
- Edge cases (null fields, boundary values) are handled explicitly

COVERAGE:
- Order event validation: 5 test cases
- Inventory event validation: 3 test cases
- Payment event validation: 4 test cases
"""

import pytest
from spark.validators import (
    _validate_order_event,
    _validate_inventory_event,
    _validate_payment_event,
)


# ── ORDER EVENT TESTS ──────────────────────────────────────────

def test_valid_order_passes():
    result = _validate_order_event(
        "ORD-123", "ORDER_CREATED",
        "2026-06-01T10:00:00.000000+00:00", 99.99, "1.0"
    )
    assert result["is_valid"] is True
    assert result["reason"] == ""


def test_null_order_id_fails():
    result = _validate_order_event(
        None, "ORDER_CREATED",
        "2026-06-01T10:00:00.000000+00:00", 99.99, "1.0"
    )
    assert result["is_valid"] is False
    assert result["reason"] == "MISSING_ORDER_ID"


def test_unknown_event_type_fails():
    result = _validate_order_event(
        "ORD-123", "UNKNOWN_TYPE",
        "2026-06-01T10:00:00.000000+00:00", 99.99, "1.0"
    )
    assert result["is_valid"] is False
    assert "UNKNOWN_EVENT_TYPE" in result["reason"]


def test_negative_amount_fails():
    result = _validate_order_event(
        "ORD-123", "ORDER_CREATED",
        "2026-06-01T10:00:00.000000+00:00", -50.0, "1.0"
    )
    assert result["is_valid"] is False
    assert "INVALID_AMOUNT" in result["reason"]


def test_unknown_schema_version_fails():
    result = _validate_order_event(
        "ORD-123", "ORDER_CREATED",
        "2026-06-01T10:00:00.000000+00:00", 99.99, "99.0"
    )
    assert result["is_valid"] is False
    assert "UNKNOWN_SCHEMA_VERSION" in result["reason"]


# ── INVENTORY EVENT TESTS ──────────────────────────────────────

def test_valid_inventory_passes():
    result = _validate_inventory_event(
        "INV-123", "INVENTORY_RESERVED",
        "2026-06-01T10:00:00.000000+00:00",
        "LAPTOP-PRO-15", -2
    )
    assert result["is_valid"] is True
    assert result["reason"] == ""


def test_zero_quantity_delta_fails():
    result = _validate_inventory_event(
        "INV-123", "INVENTORY_RESERVED",
        "2026-06-01T10:00:00.000000+00:00",
        "LAPTOP-PRO-15", 0
    )
    assert result["is_valid"] is False
    assert result["reason"] == "ZERO_QUANTITY_DELTA"


def test_missing_sku_fails():
    result = _validate_inventory_event(
        "INV-123", "INVENTORY_RESERVED",
        "2026-06-01T10:00:00.000000+00:00",
        None, -2
    )
    assert result["is_valid"] is False
    assert result["reason"] == "MISSING_SKU"


# ── PAYMENT EVENT TESTS ────────────────────────────────────────

def test_valid_payment_passes():
    result = _validate_payment_event(
        "PAY-123", "PAYMENT_CONFIRMED",
        "2026-06-01T10:00:00.000000+00:00",
        "ORD-123", 99.99, "STRIPE"
    )
    assert result["is_valid"] is True
    assert result["reason"] == ""


def test_unknown_gateway_fails():
    result = _validate_payment_event(
        "PAY-123", "PAYMENT_CONFIRMED",
        "2026-06-01T10:00:00.000000+00:00",
        "ORD-123", 99.99, "WESTERN_UNION"
    )
    assert result["is_valid"] is False
    assert "UNKNOWN_GATEWAY" in result["reason"]


def test_negative_payment_amount_for_confirmed_fails():
    result = _validate_payment_event(
        "PAY-123", "PAYMENT_CONFIRMED",
        "2026-06-01T10:00:00.000000+00:00",
        "ORD-123", -10.0, "STRIPE"
    )
    assert result["is_valid"] is False
    assert result["is_valid"] is False
    assert "INVALID_AMOUNT_FOR_CONFIRMED" in result["reason"]


def test_negative_payment_amount_for_non_confirmed_fails():
    # For non-CONFIRMED events, the negative amount check runs separately
    result = _validate_payment_event(
        "PAY-123", "PAYMENT_INITIATED",
        "2026-06-01T10:00:00.000000+00:00",
        "ORD-123", -10.0, "STRIPE"
    )
    assert result["is_valid"] is False
    assert "NEGATIVE_AMOUNT" in result["reason"]


def test_confirmed_payment_with_zero_amount_fails():
    result = _validate_payment_event(
        "PAY-123", "PAYMENT_CONFIRMED",
        "2026-06-01T10:00:00.000000+00:00",
        "ORD-123", 0.0, "STRIPE"
    )
    assert result["is_valid"] is False
    assert "INVALID_AMOUNT_FOR_CONFIRMED" in result["reason"]