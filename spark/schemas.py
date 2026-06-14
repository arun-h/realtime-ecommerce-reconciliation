"""
spark/schemas.py — Spark StructType definitions for all three Kafka topics

WHY STRUCTTYPE INSTEAD OF INFERRING SCHEMA?
Spark can infer schema from JSON automatically. We don't use that because:
1. Inference requires reading a sample of data — adds startup latency
2. Inferred types are wrong for our data (Spark infers all numbers as
   LongType, we need DoubleType for amounts)
3. Inference fails silently on empty topics — job crashes at runtime
4. Explicit schema = contract. If a producer changes its payload shape,
   Spark rejects the event at the schema layer and routes it to DLQ.
   With inferred schema, bad events corrupt our dataset silently.

FIELD NAMING INTENTIONAL DIFFERENCES:
- Orders use:    event_timestamp (string, ISO 8601)
- Inventory use: occurred_at    (string, ISO 8601)  ← different name, same concept
- Payments use:  event_timestamp (string, ISO 8601)

The reconciler normalises occurred_at → event_timestamp during processing.
These schemas reflect the ACTUAL payload shapes from the producers.
Do not "fix" the field names here — the divergence is intentional.
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType,
    BooleanType, ArrayType, MapType
)

# ── ORDER EVENTS SCHEMA ────────────────────────────────────────
# Matches order_producer.py generate_event() output exactly
ORDER_SCHEMA = StructType([
    StructField("order_id",        StringType(),  nullable=False),
    StructField("event_type",      StringType(),  nullable=False),
    StructField("event_timestamp", StringType(),  nullable=False),
    StructField("schema_version",  StringType(),  nullable=True),
    StructField("customer_id",     StringType(),  nullable=True),
    StructField("total_amount",    DoubleType(),  nullable=True),
    StructField("currency",        StringType(),  nullable=True),
    StructField("status",          StringType(),  nullable=True),

    # items is an array of structs — each item has sku, quantity, price
    StructField("items", ArrayType(
        StructType([
            StructField("sku",        StringType(), nullable=True),
            StructField("quantity",   IntegerType(), nullable=True),
            StructField("unit_price", DoubleType(),  nullable=True),
            StructField("category",   StringType(),  nullable=True),
        ])
    ), nullable=True),

    # Nested structs — Spark handles these natively
    StructField("shipping_address", StructType([
        StructField("country",     StringType(), nullable=True),
        StructField("postal_code", StringType(), nullable=True),
    ]), nullable=True),

    StructField("metadata", StructType([
        StructField("source",     StringType(), nullable=True),
        StructField("user_agent", StringType(), nullable=True),
        StructField("session_id", StringType(), nullable=True),
    ]), nullable=True),
])


# ── INVENTORY EVENTS SCHEMA ────────────────────────────────────
# Matches inventory_producer.py generate_event() output exactly
# NOTE: uses "occurred_at" not "event_timestamp" — intentional divergence
INVENTORY_SCHEMA = StructType([
    StructField("inventory_id",   StringType(),  nullable=False),
    StructField("event_type",     StringType(),  nullable=False),
    StructField("occurred_at",    StringType(),  nullable=False),  # ← different field name
    StructField("schema_version", StringType(),  nullable=True),
    StructField("sku",            StringType(),  nullable=True),
    StructField("quantity_delta", IntegerType(), nullable=True),
    StructField("warehouse_id",   StringType(),  nullable=True),
    StructField("current_stock_level", IntegerType(), nullable=True),
    StructField("order_id",       StringType(),  nullable=True),  # nullable — restocks have no order

    StructField("metadata", StructType([
        StructField("operator_id", StringType(), nullable=True),
        StructField("reason",      StringType(), nullable=True),
    ]), nullable=True),
])


# ── PAYMENT EVENTS SCHEMA ──────────────────────────────────────
# Matches stripe_receiver.py normalise_stripe_payload() output
# These are NORMALISED Stripe events — already cents→dollars, unix→ISO 8601
# This schema represents what lands in payment-events topic after normalisation
PAYMENT_SCHEMA = StructType([
    StructField("payment_id",            StringType(), nullable=False),
    StructField("event_type",            StringType(), nullable=False),
    StructField("event_timestamp",       StringType(), nullable=False),
    StructField("schema_version",        StringType(), nullable=True),
    StructField("order_id",              StringType(), nullable=True),
    StructField("amount",                DoubleType(), nullable=True),
    StructField("currency",              StringType(), nullable=True),
    StructField("gateway",               StringType(), nullable=True),
    StructField("status",                StringType(), nullable=True),
    StructField("gateway_event_id",      StringType(), nullable=True),
    StructField("gateway_transaction_id",StringType(), nullable=True),
    StructField("failure_reason",        StringType(), nullable=True),
    StructField("raw_stripe_event",      StringType(), nullable=True),
])


# ── KAFKA MESSAGE WRAPPER SCHEMA ───────────────────────────────
# When Spark reads from Kafka, each message has this structure.
# The actual event payload is in the "value" field as raw bytes.
# We parse value as JSON using from_json() with the schemas above.
#
# We never use this schema directly — it is here for documentation.
# Spark's Kafka connector produces this shape automatically:
#
# root
#  |-- key: binary
#  |-- value: binary        ← this is what we parse with from_json()
#  |-- topic: string
#  |-- partition: integer
#  |-- offset: long
#  |-- timestamp: timestamp  ← Kafka broker timestamp (processing-time)
#  |-- timestampType: integer
#
# We use event_timestamp from the parsed payload (event-time),
# NOT the Kafka broker timestamp (processing-time).
# See watermark strategy in streaming_job.py for reasoning.


# ── SCHEMA REGISTRY ────────────────────────────────────────────
# Single lookup dict used by validators and streaming job
# to get the right schema for each topic without if/else chains
SCHEMA_REGISTRY = {
    "order-events":     ORDER_SCHEMA,
    "inventory-events": INVENTORY_SCHEMA,
    "payment-events":   PAYMENT_SCHEMA,
}