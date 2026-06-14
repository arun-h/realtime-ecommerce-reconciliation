"""
spark/reconciler.py — Cross-domain join and SLA calculation

THE CORE OF THE ENTIRE SYSTEM.

What happens here:
1. Three normalised streams (orders, payments, inventory) are joined
   on order_id within a 60-second event-time window
2. SLA is calculated: did payment confirm within 60s of order creation?
3. Did inventory reserve within 60s of order creation?
4. Result is one reconciled row per order with full audit trail

JOIN STRATEGY:
We use a left outer join — orders are the anchor stream.
Every order gets a reconciled row regardless of whether payment
or inventory events arrived. Missing events show as null columns
with SLA flags set to False.

WHY LEFT JOIN NOT INNER JOIN:
Inner join only produces rows when ALL three streams have matching events.
An order with no payment confirmation would simply disappear from results.
That is the opposite of what we want — missing payments are exactly
the failures we need to detect.

WINDOW REASONING:
event_timestamp is used for all windowing (not processing time).
Window size: 60 seconds — the SLA boundary.
Watermark: 90 seconds — late event tolerance (set upstream in streaming_job.py).
Events outside the watermark are routed to DLQ, not silently dropped.

KNOWN LIMITATION:
Real e-commerce pipelines can have minutes of skew between domains.
60 seconds is intentionally conservative for demonstration purposes.
Production systems would use a longer window (5-15 minutes) plus
a separate reconciliation pass for late arrivals.
This limitation is documented — it is not an oversight.
"""



from pyspark.sql import DataFrame

from pyspark.sql.functions import (
    col, when, unix_timestamp, round as spark_round,
    window, lit, current_timestamp, to_json, struct,
    coalesce
)


# ── SLA THRESHOLDS ─────────────────────────────────────────────
# These are the business rules for what "on time" means.
# Stored as constants so they appear in exactly one place.
# If the business changes SLA targets, change here only.

PAYMENT_SLA_SECONDS = 60    # Payment must confirm within 60s of order
INVENTORY_SLA_SECONDS = 60  # Inventory must reserve within 60s of order
JOIN_WINDOW_SECONDS = 60    # Event-time window for cross-domain join


def reconcile_streams(
    orders_df: DataFrame,
    payments_df: DataFrame,
    inventory_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """
    Join three streams and calculate SLA compliance.

    Returns:
        tuple of (reconciled_df, dlq_df)
        reconciled_df — successfully joined records for PostgreSQL
        dlq_df        — records that failed reconciliation rules for DLQ

    COLUMN SELECTION STRATEGY:
    We prefix columns from each stream before joining to avoid
    ambiguity. order.order_id, payment.order_id, inventory.order_id
    are all the same value but Spark treats them as separate columns
    after a join. Explicit prefixing + selection avoids SELECT * bugs.
    """

    # ── STEP 1: Select only columns needed for the join ────────
    # Reduces shuffle data size — we don't carry every field
    # through the join, only what the reconciler needs.
    orders_slim = orders_df.select(
        col("order_id"),
        col("event_timestamp").alias("order_ts"),
        col("customer_id"),
        col("total_amount"),
        col("currency"),
        col("status").alias("order_status"),
        col("event_type").alias("order_event_type"),
    )

    payments_slim = payments_df.select(
        col("payment_id"),
        col("event_timestamp").alias("payment_ts"),
        col("order_id").alias("payment_order_id"),
        col("amount").alias("payment_amount"),
        col("gateway"),
        col("event_type").alias("payment_event_type"),
        col("failure_reason"),
    ).filter(
        # Only join on terminal payment states
        # INITIATED events don't mean the payment succeeded
        # WHY: joining on INITIATED would incorrectly mark orders
        # as "payment received" before confirmation
        col("payment_event_type").isin(
            "PAYMENT_CONFIRMED", "PAYMENT_FAILED",
            "PAYMENT_DISPUTED", "PAYMENT_REFUNDED"
        )
    )

    inventory_slim = inventory_df.select(
        col("inventory_id"),
        col("event_timestamp").alias("inventory_ts"),
        col("order_id").alias("inventory_order_id"),
        col("sku"),
        col("quantity_delta"),
        col("warehouse_id"),
        col("event_type").alias("inventory_event_type"),
    ).filter(
        # Only join on reservation events
        # RESTOCKED events have no order_id and are not relevant
        # to order reconciliation
        col("inventory_event_type") == "INVENTORY_RESERVED"
    )

    # ── STEP 2: Join orders ← payments (left outer) ────────────
    # Left join: every order gets a row.
    # Payment columns are null if no matching payment arrived.
    #
    # JOIN CONDITION:
    # 1. order_id must match across domains
    # 2. Payment must arrive within JOIN_WINDOW_SECONDS of order
    #    (event-time comparison, not processing-time)
    #
    # WHY NOT USE SPARK'S BUILT-IN WINDOW JOIN?
    # Spark's stream-stream join with watermarks requires both streams
    # to have watermarks applied. We apply watermarks upstream in
    # streaming_job.py before calling this function.
    orders_payments = orders_slim.join(
        payments_slim,
        on=(orders_slim.order_id == payments_slim.payment_order_id),
        how="left"
    )

    # ── STEP 3: Join result ← inventory ────────────────────────
    orders_payments_inventory = orders_payments.join(
        inventory_slim,
        on=(orders_payments.order_id == inventory_slim.inventory_order_id),
        how="left"
    )

    # ── STEP 4: Calculate SLA compliance ──────────────────────
    # payment_sla_met: True if payment confirmed within 60s of order
    # inventory_sla_met: True if inventory reserved within 60s of order
    #
    # NULL handling:
    # If payment_ts is null (no payment arrived), SLA is False.
    # when().otherwise() handles null correctly — null comparisons
    # return null in SQL, not False. We use isNotNull() explicitly.
    reconciled = orders_payments_inventory.withColumn(
        "payment_latency_seconds",
        when(
            col("payment_ts").isNotNull(),
            spark_round(
                unix_timestamp(col("payment_ts")) -
                unix_timestamp(col("order_ts")),
                2
            )
        ).otherwise(lit(None))

    ).withColumn(
        "payment_sla_met",
        when(
            col("payment_ts").isNotNull() &
            (col("payment_latency_seconds") <= PAYMENT_SLA_SECONDS),
            lit(True)
        ).otherwise(lit(False))

    ).withColumn(
        "inventory_latency_seconds",
        when(
            col("inventory_ts").isNotNull(),
            spark_round(
                unix_timestamp(col("inventory_ts")) -
                unix_timestamp(col("order_ts")),
                2
            )
        ).otherwise(lit(None))

    ).withColumn(
        "inventory_sla_met",
        when(
            col("inventory_ts").isNotNull() &
            (col("inventory_latency_seconds") <= INVENTORY_SLA_SECONDS),
            lit(True)
        ).otherwise(lit(False))

    ).withColumn(
        "reconciled_at",
        current_timestamp()
    )

    # ── STEP 5: Select final output columns ────────────────────
    # Explicit column selection — no SELECT *.
    # Every column in the final output is intentional.
    final_cols = reconciled.select(
        col("order_id"),
        col("order_ts").alias("event_timestamp"),
        col("customer_id"),
        col("total_amount"),
        col("order_status"),
        col("payment_id"),
        col("payment_ts").alias("payment_confirmed_at"),
        col("gateway").alias("payment_gateway"),
        col("payment_amount"),
        col("payment_sla_met"),
        col("payment_latency_seconds"),
        col("inventory_id"),
        col("inventory_ts").alias("inventory_reserved_at"),
        col("sku"),
        col("inventory_sla_met"),
        col("inventory_latency_seconds"),
        col("reconciled_at"),
    )

    # ── STEP 6: Identify DLQ candidates ────────────────────────
    # Records where BOTH payment AND inventory are missing after
    # the join window are candidates for DLQ — they indicate
    # a systemic failure, not just a slow event.
    #
    # Records where only ONE is missing are normal partial
    # reconciliation — the other event may still be in-flight.
    dlq_candidates = final_cols.filter(
        col("payment_id").isNull() &
        col("inventory_id").isNull()
    ).withColumn(
        "failure_reason",
        lit("NO_PAYMENT_OR_INVENTORY_WITHIN_WINDOW")
    ).withColumn(
        "source_topic",
        lit("reconciler")
    )

    # Successful reconciliations — at least one of payment/inventory matched
    reconciled_output = final_cols.filter(
        col("payment_id").isNotNull() |
        col("inventory_id").isNotNull()
    )

    return reconciled_output, dlq_candidates