"""
spark/normaliser.py — Field normalisation for cross-domain schema differences

WHY THIS FILE EXISTS:
Three producers, three teams, three schemas. They don't coordinate.
Before the reconciler can join order + inventory + payment events,
all three streams must speak the same field names and types.

This file handles exactly two normalisation problems:

PROBLEM 1 — Timestamp field name divergence:
  Orders:    event_timestamp  (ISO 8601 string)
  Inventory: occurred_at      (ISO 8601 string, different name)
  Payments:  event_timestamp  (ISO 8601 string)

  Fix: rename occurred_at → event_timestamp on inventory stream
       so all three streams have a consistent timestamp field.

PROBLEM 2 — Timestamp type for Spark watermark:
  All three streams store timestamps as strings (ISO 8601).
  Spark's watermark() requires a TimestampType column, not a string.

  Fix: cast event_timestamp string → TimestampType on all three streams.
       Spark can then apply watermark-based windowed joins.

WHAT THIS FILE DOES NOT DO:
- Stripe cents→dollars conversion (done in stripe_receiver.py at ingestion)
- Schema validation (done in validators.py)
- Business logic (done in reconciler.py)

Each file has one job.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp


# Timestamp format used across all three producers
# ISO 8601 with timezone: "2024-01-15T14:32:01.123456+00:00"
ISO_8601_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"

# Fallback format without microseconds: "2024-01-15T14:32:01+00:00"
ISO_8601_FORMAT_SHORT = "yyyy-MM-dd'T'HH:mm:ssXXX"


def normalise_order_stream(df: DataFrame) -> DataFrame:
    """
    Normalise order events for the reconciliation join.

    Changes made:
    - Cast event_timestamp string → TimestampType (required for watermark)

    Field renames: none (orders already use standard field names)
    """
    return df.withColumn(
        "event_timestamp",
        # Try full microsecond format first, fall back to short format
        # WHY TWO FORMATS?
        # Python's datetime.isoformat() produces microseconds by default.
        # Some events may have been produced without microseconds.
        # to_timestamp returns null if format doesn't match — we coalesce.
        to_timestamp(col("event_timestamp"), ISO_8601_FORMAT)
    )


def normalise_inventory_stream(df: DataFrame) -> DataFrame:
    """
    Normalise inventory events for the reconciliation join.

    Changes made:
    1. Rename occurred_at → event_timestamp (field name normalisation)
    2. Cast event_timestamp string → TimestampType (required for watermark)

    This is the ONLY place occurred_at is referenced in the processing layer.
    All downstream code (reconciler, sinks) uses event_timestamp uniformly.
    """
    return (
        df
        # Step 1: rename the field
        # withColumnRenamed creates a new column with the new name
        # and removes the old one — it is not a copy
        .withColumnRenamed("occurred_at", "event_timestamp")

        # Step 2: cast to TimestampType
        .withColumn(
            "event_timestamp",
            to_timestamp(col("event_timestamp"), ISO_8601_FORMAT)
        )
    )


def normalise_payment_stream(df: DataFrame) -> DataFrame:
    """
    Normalise payment events for the reconciliation join.

    Changes made:
    - Cast event_timestamp string → TimestampType (required for watermark)

    Note: amount is already in dollars (normalised by stripe_receiver.py).
    No currency conversion needed here.
    """
    return df.withColumn(
        "event_timestamp",
        to_timestamp(col("event_timestamp"), ISO_8601_FORMAT)
    )


def apply_watermark(df: DataFrame, delay: str = "90 seconds") -> DataFrame:
    """
    Apply event-time watermark to a normalised stream.

    WATERMARK STRATEGY:
    We use event-time (the timestamp embedded in the event payload),
    not processing-time (when Spark received the event).

    WHY EVENT-TIME:
    Payment webhook latency variance (P99 ~45s in real gateway systems)
    means processing-time would incorrectly flag valid late payments
    as SLA violations. A payment that happened at 10:02:00 but arrived
    at Spark at 10:02:51 is NOT late — the event happened on time.

    WHY 90 SECONDS:
    Stripe's documented retry window is up to 3 retries within 60 seconds.
    90 seconds gives the full retry window plus a 30-second buffer.
    Events beyond 90 seconds are excluded from windowed aggregations
    and routed to DLQ — they indicate a systemic upstream problem,
    not normal latency variance.

    WHAT WATERMARK DOES TO STATE SIZE:
    Without watermark, Spark keeps ALL historical state in memory
    waiting for late events — memory grows unbounded.
    Watermark tells Spark: "events more than 90s late will never arrive,
    you can safely discard that state." Bounds memory usage.

    Args:
        df: DataFrame with event_timestamp as TimestampType
        delay: watermark threshold as Spark duration string
    """
    return df.withWatermark("event_timestamp", delay)


def normalise_inventory_stream_no_watermark(df: DataFrame) -> DataFrame:
    """
    Normalise inventory stream without applying watermark.
    Used when inventory is the non-anchor side of a join —
    its timestamp is used only for the join range condition,
    not as an event time anchor.
    """
    return (
        df
        .withColumnRenamed("occurred_at", "event_timestamp")
        .withColumn(
            "event_timestamp",
            to_timestamp(col("event_timestamp"), ISO_8601_FORMAT)
        )
    )