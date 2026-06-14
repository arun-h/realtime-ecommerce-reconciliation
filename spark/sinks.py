"""
spark/sinks.py — PostgreSQL upsert and MinIO Parquet writer

TWO SINKS, TWO PURPOSES:

SINK 1 — PostgreSQL (operational state store)
    Reconciled events written via idempotent upsert.
    ON CONFLICT DO UPDATE means the same event arriving N times
    produces exactly one row. This is the idempotency guarantee.
    Used for: dashboards, alerting, SLA queries, DLQ inspection.

SINK 2 — MinIO / S3 (raw event archive)
    All three domain streams archived as Parquet files.
    Partitioned by year/month/day/hour.
    Append-only. Never mutated after write.
    Used for: replay, backfill, Airflow batch jobs, Athena queries.

WHY BOTH?
PostgreSQL answers operational questions fast (index lookups, joins).
MinIO answers historical questions cheap (full scans, backfills).
They are complementary, not redundant.

BATCH WRITE PATTERN:
Both sinks use foreachBatch() — Spark calls our function once per
micro-batch with a static DataFrame. This gives us:
1. Full DataFrame API inside the batch function
2. Ability to use non-streaming sinks (psycopg2, boto3)
3. Batch-level metrics (row counts, timing)
4. Explicit error handling per batch
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, year, month, dayofmonth, hour,
    to_json, struct, lit
)

load_dotenv()
logger = logging.getLogger(__name__)


# ── POSTGRESQL CONNECTION ──────────────────────────────────────
def get_postgres_connection():
    """
    Create a fresh PostgreSQL connection per batch.

    WHY NOT A CONNECTION POOL?
    Spark runs foreachBatch on the driver. A single connection per
    batch is sufficient at this event volume. Connection pools add
    complexity for no benefit here.

    In production with high-volume batches: use psycopg2 pool
    or SQLAlchemy with pool_size tuned to batch concurrency.
    """
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB"),
        connect_timeout=10,
    )


# ── SINK 1: POSTGRESQL UPSERT ──────────────────────────────────
def write_reconciled_to_postgres(batch_df: DataFrame, batch_id: int) -> int:
    """
    Write reconciled events to PostgreSQL via idempotent upsert.

    IDEMPOTENCY MECHANISM:
    INSERT INTO reconciled_order_events (...)
    VALUES (...)
    ON CONFLICT (order_id, event_timestamp)
    DO UPDATE SET ...

    The composite primary key (order_id, event_timestamp) is the
    deduplication key. Same event arriving twice = one row updated,
    not two rows inserted.

    WHY executemany() NOT execute() IN A LOOP?
    executemany() sends all rows in one network round-trip.
    A loop calls execute() once per row — N round-trips for N rows.
    At 100 rows/batch: executemany() is ~100x faster.

    Returns:
        Number of rows upserted
    """
    rows = batch_df.collect()

    if not rows:
        logger.info(json.dumps({
            "event": "postgres_write_skipped",
            "batch_id": batch_id,
            "reason": "empty_batch",
        }))
        return 0

    # Convert Spark Row objects to plain tuples for psycopg2
    records = []
    for row in rows:
        records.append((
            row.order_id,
            row.event_timestamp,
            row.customer_id,
            row.total_amount,
            row.order_status,
            row.payment_id,
            row.payment_confirmed_at,
            row.payment_gateway,
            row.payment_amount,
            bool(row.payment_sla_met) if row.payment_sla_met is not None else False,
            float(row.payment_latency_seconds) if row.payment_latency_seconds else None,
            row.inventory_id,
            row.inventory_reserved_at,
            row.sku,
            bool(row.inventory_sla_met) if row.inventory_sla_met is not None else False,
            float(row.inventory_latency_seconds) if row.inventory_latency_seconds else None,
            row.reconciled_at,
        ))

    upsert_sql = """
        INSERT INTO reconciled_order_events (
            order_id,
            event_timestamp,
            customer_id,
            total_amount,
            order_status,
            payment_id,
            payment_confirmed_at,
            payment_gateway,
            payment_amount,
            payment_sla_met,
            payment_latency_seconds,
            inventory_id,
            inventory_reserved_at,
            sku,
            inventory_sla_met,
            inventory_latency_seconds,
            reconciled_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (order_id, event_timestamp)
        DO UPDATE SET
            payment_id               = EXCLUDED.payment_id,
            payment_confirmed_at     = EXCLUDED.payment_confirmed_at,
            payment_gateway          = EXCLUDED.payment_gateway,
            payment_amount           = EXCLUDED.payment_amount,
            payment_sla_met          = EXCLUDED.payment_sla_met,
            payment_latency_seconds  = EXCLUDED.payment_latency_seconds,
            inventory_id             = EXCLUDED.inventory_id,
            inventory_reserved_at    = EXCLUDED.inventory_reserved_at,
            sku                      = EXCLUDED.sku,
            inventory_sla_met        = EXCLUDED.inventory_sla_met,
            inventory_latency_seconds = EXCLUDED.inventory_latency_seconds,
            reconciled_at            = EXCLUDED.reconciled_at
    """

    conn = None
    try:
        conn = get_postgres_connection()
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, upsert_sql, records)
        conn.commit()

        logger.info(json.dumps({
            "event": "postgres_write_success",
            "batch_id": batch_id,
            "rows_upserted": len(records),
        }))
        return len(records)

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(json.dumps({
            "event": "postgres_write_failed",
            "batch_id": batch_id,
            "error": str(e),
            "rows_attempted": len(records),
        }))
        raise
    finally:
        if conn:
            conn.close()


def write_dlq_to_postgres(batch_df: DataFrame, batch_id: int) -> int:
    """
    Write DLQ events to PostgreSQL dead_letter_events table.

    DLQ events use TEXT for raw_payload, not JSONB.
    WHY: the payload may not be valid JSON — that could be
    why it ended up in the DLQ. TEXT never rejects input.
    """
    rows = batch_df.collect()

    if not rows:
        return 0

    records = []
    for row in rows:
        records.append((
            row.source_topic if hasattr(row, 'source_topic') else 'reconciler',
            json.dumps(row.asDict(), default=str),
            row.failure_reason if hasattr(row, 'failure_reason') else 'UNKNOWN',
            datetime.now(timezone.utc),
        ))

    insert_sql = """
        INSERT INTO dead_letter_events
            (source_topic, raw_payload, failure_reason, failed_at)
        VALUES (%s, %s, %s, %s)
    """

    conn = None
    try:
        conn = get_postgres_connection()
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, insert_sql, records)
        conn.commit()

        logger.info(json.dumps({
            "event": "dlq_write_success",
            "batch_id": batch_id,
            "rows_written": len(records),
        }))
        return len(records)

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(json.dumps({
            "event": "dlq_write_failed",
            "batch_id": batch_id,
            "error": str(e),
        }))
        raise
    finally:
        if conn:
            conn.close()


def write_pipeline_metrics(
    batch_id: int,
    input_rows: int,
    reconciled_rows: int,
    dlq_rows: int,
    processing_duration_ms: int,
) -> None:
    """
    Write one metrics row per batch to pipeline_metrics table.

    WHY A METRICS TABLE?
    Logs disappear. This table gives us a time-series of pipeline
    health without external monitoring tools. Airflow quality checks
    query this table to detect degradation trends.
    """
    conn = None
    try:
        conn = get_postgres_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_metrics (
                    batch_id,
                    batch_timestamp,
                    input_rows,
                    dlq_events,
                    reconciliation_failures,
                    reconciliation_failure_rate,
                    processing_duration_ms
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                batch_id,
                datetime.now(timezone.utc),
                input_rows,
                dlq_rows,
                dlq_rows,
                round(dlq_rows / max(input_rows, 1), 4),
                processing_duration_ms,
            ))
        conn.commit()

    except Exception as e:
        logger.error(json.dumps({
            "event": "metrics_write_failed",
            "batch_id": batch_id,
            "error": str(e),
        }))
    finally:
        if conn:
            conn.close()


# ── SINK 2: MINIO PARQUET ARCHIVE ─────────────────────────────
def write_raw_events_to_minio(
    batch_df: DataFrame,
    batch_id: int,
    topic_name: str,
) -> None:
    """
    Archive raw events to MinIO as Parquet files.

    PARTITIONING STRATEGY: year/month/day/hour
    WHY HOUR-LEVEL?
    - Minute-level: too many small files (Parquet overhead per file)
    - Day-level: large scans for hourly queries
    - Hour-level: balances query pruning vs file count

    APPEND-ONLY:
    We never overwrite Parquet files. mode("append") adds new files.
    This makes the archive immutable — safe for replay.

    S3A CONFIGURATION:
    Spark uses the s3a:// filesystem connector to write to MinIO.
    The hadoop-aws JAR handles the S3 API translation.
    MinIO_ENDPOINT in .env points to localhost:9000 for local dev.
    In production: change endpoint to real AWS S3, no code changes.

    WHY NOT WRITE DIRECTLY FROM KAFKA TO S3?
    We could use Kafka Connect S3 Sink. We don't because:
    1. Adds another service to operate
    2. We want Parquet (columnar), not raw JSON
    3. We want hour-level partitioning applied by Spark
    4. Spark is already reading this data — double the work for Connect
    """
    if batch_df.rdd.isEmpty():
        return

    # Add partition columns derived from event_timestamp
    # WHY DERIVED COLUMNS NOT JUST PARTITION BY TIMESTAMP?
    # Partitioning by a TimestampType creates one partition per
    # unique millisecond — millions of tiny files. Extracting
    # year/month/day/hour gives coarse, useful partitions.
    partitioned_df = batch_df \
        .withColumn("year",  year(col("event_timestamp"))) \
        .withColumn("month", month(col("event_timestamp"))) \
        .withColumn("day",   dayofmonth(col("event_timestamp"))) \
        .withColumn("hour",  hour(col("event_timestamp")))

    output_path = (
        f"s3a://{os.getenv('S3_BUCKET', 'ecommerce-raw-events')}"
        f"/{topic_name}"
    )

    try:
        (
            partitioned_df
            .write
            .mode("append")
            .partitionBy("year", "month", "day", "hour")
            .parquet(output_path)
        )

        logger.info(json.dumps({
            "event": "minio_write_success",
            "batch_id": batch_id,
            "topic": topic_name,
            "output_path": output_path,
        }))

    except Exception as e:
        logger.error(json.dumps({
            "event": "minio_write_failed",
            "batch_id": batch_id,
            "topic": topic_name,
            "error": str(e),
        }))
        # Do not re-raise — Parquet archive failure should not
        # crash the streaming job. PostgreSQL write is authoritative.
        # Archive failure is logged and monitored separately.