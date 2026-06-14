"""
spark/streaming_job.py — Streaming job: write each stream independently,
reconcile from PostgreSQL.

REVISED ARCHITECTURE:
Instead of stream-stream joins (which hit Spark constraints repeatedly),
we split into two concerns:

1. THREE INDEPENDENT WRITE STREAMS
   Each Kafka topic → parsed → validated → PostgreSQL raw table
   No joins. No streaming join constraints. Simple and reliable.

2. RECONCILIATION QUERY (runs every batch via foreachBatch on orders)
   After orders land in PostgreSQL, query all three raw tables and
   reconcile via SQL join. This is a batch operation on static data —
   no streaming restrictions apply.

WHY THIS IS ACTUALLY MORE CORRECT:
- Streaming joins require both events to arrive in the same micro-batch
  window. In reality, a payment may arrive 45 seconds after an order.
  PostgreSQL-based reconciliation naturally handles any timing gap.
- Each stream writes independently — a payment topic lag doesn't block
  order processing.
- Reconciliation can be re-run at any time without replaying Kafka.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, lit, to_timestamp
from dotenv import load_dotenv

from spark.schemas import ORDER_SCHEMA, INVENTORY_SCHEMA, PAYMENT_SCHEMA
from spark.validators import (
    validate_order_udf,
    validate_inventory_udf,
    validate_payment_udf,
)
from spark.normaliser import (
    normalise_order_stream,
    normalise_inventory_stream,
    normalise_payment_stream,
    apply_watermark,
)
from spark.sinks import (
    write_dlq_to_postgres,
    write_pipeline_metrics,
    write_raw_events_to_minio,
    get_postgres_connection,
)

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
logger = logging.getLogger("streaming_job")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CHECKPOINT_BASE         = os.getenv("SPARK_CHECKPOINT_LOCATION",
                                    "s3a://spark-checkpoints/streaming")
TRIGGER_INTERVAL        = "30 seconds"
WATERMARK_DELAY         = "90 seconds"
PAYMENT_SLA_SECONDS     = 60
INVENTORY_SLA_SECONDS   = 60


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ecommerce-reconciliation")
        .master("local[2]")
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            ])
        )
        .config("spark.hadoop.fs.s3a.endpoint",
                os.getenv("MINIO_ENDPOINT", "http://localhost:9000"))
        .config("spark.hadoop.fs.s3a.access.key",
                os.getenv("MINIO_ROOT_USER", "minio_admin"))
        .config("spark.hadoop.fs.s3a.secret.key",
                os.getenv("MINIO_ROOT_PASSWORD", "minio_password_dev"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.driver.extraJavaOptions",
                "-Dlog4j.configurationFile=log4j2.properties")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.streaming.metricsEnabled", "true")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession, topic: str) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_kafka_stream(raw_df: DataFrame, schema, topic: str):
    parsed = raw_df.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp"),
    )
    expanded = parsed.select("data.*", "kafka_timestamp")

    required_field_map = {
        "order-events":     "order_id",
        "inventory-events": "inventory_id",
        "payment-events":   "payment_id",
    }
    required_field = required_field_map.get(topic, "order_id")

    parsed_df = expanded.filter(col(required_field).isNotNull())
    failed_df = expanded.filter(col(required_field).isNull()) \
        .withColumn("failure_reason", lit(f"SCHEMA_PARSE_FAILED:{topic}")) \
        .withColumn("source_topic", lit(topic))

    return parsed_df, failed_df


def validate_stream(df: DataFrame, topic: str):
    if topic == "order-events":
        validated = df.withColumn(
            "validation",
            validate_order_udf(
                col("order_id"), col("event_type"),
                col("event_timestamp"), col("total_amount"),
                col("schema_version"),
            )
        )
    elif topic == "inventory-events":
        validated = df.withColumn(
            "validation",
            validate_inventory_udf(
                col("inventory_id"), col("event_type"),
                col("occurred_at"), col("sku"),
                col("quantity_delta"),
            )
        )
    elif topic == "payment-events":
        validated = df.withColumn(
            "validation",
            validate_payment_udf(
                col("payment_id"), col("event_type"),
                col("event_timestamp"), col("order_id"),
                col("amount"), col("gateway"),
            )
        )
    else:
        raise ValueError(f"Unknown topic: {topic}")

    valid_df   = validated.filter(col("validation.is_valid") == True).drop("validation")
    invalid_df = validated.filter(col("validation.is_valid") == False) \
        .withColumn("failure_reason", col("validation.reason")) \
        .withColumn("source_topic", lit(topic)) \
        .drop("validation")

    return valid_df, invalid_df


# ── RAW TABLE WRITERS ──────────────────────────────────────────

def write_orders_batch(batch_df: DataFrame, batch_id: int):
    rows = batch_df.collect()
    rows = [r for r in rows 
        if r.order_id is not None 
        and r.event_type is not None 
        and r.event_timestamp is not None]
    if not rows:
        return

    conn = None
    try:
        conn = get_postgres_connection()
        records = []
        for row in rows:
            records.append((
                row.order_id,
                row.event_type,
                row.event_timestamp,
                row.customer_id,
                float(row.total_amount) if row.total_amount else None,
                row.currency,
                row.status,
                json.dumps(row.asDict(), default=str),
            ))

        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO raw_order_events
                    (order_id, event_type, event_timestamp,
                     customer_id, total_amount, currency, status, raw_payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id, event_type, event_timestamp) DO NOTHING
            """, records)
        conn.commit()

        logger.info(json.dumps({
            "event": "raw_orders_written",
            "batch_id": batch_id,
            "rows": len(records),
        }))

        # After writing orders, run reconciliation
        reconcile_from_postgres(batch_id)

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"write_orders_batch failed: {e}")
        raise
    finally:
        if conn:
            conn.close()


def write_inventory_batch(batch_df: DataFrame, batch_id: int):
    rows = batch_df.collect()
    rows = [r for r in rows
            if r.inventory_id is not None
            and r.event_type is not None
            and r.occurred_at is not None]
    if not rows:
        return

    conn = None
    try:
        conn = get_postgres_connection()
        records = []
        for row in rows:
            records.append((
                row.inventory_id,
                row.event_type,
                row.occurred_at,
                row.sku,
                int(row.quantity_delta) if row.quantity_delta else None,
                row.warehouse_id,
                json.dumps(row.asDict(), default=str),
            ))

        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO raw_inventory_events
                    (inventory_id, event_type, event_timestamp,
                     sku, quantity_delta, warehouse_id, raw_payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (inventory_id, event_type, event_timestamp) DO NOTHING
            """, records)
        conn.commit()

        logger.info(json.dumps({
            "event": "raw_inventory_written",
            "batch_id": batch_id,
            "rows": len(records),
        }))

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"write_inventory_batch failed: {e}")
    finally:
        if conn:
            conn.close()


def write_payments_batch(batch_df: DataFrame, batch_id: int):
    rows = batch_df.collect()
    if not rows:
        return

    conn = None
    try:
        conn = get_postgres_connection()
        records = []
        for row in rows:
            records.append((
                row.payment_id,
                row.event_type,
                row.event_timestamp,
                row.order_id,
                float(row.amount) if row.amount else None,
                row.gateway,
                row.status,
                json.dumps(row.asDict(), default=str),
            ))

        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO raw_payment_events
                    (payment_id, event_type, event_timestamp,
                     order_id, amount, gateway, status, raw_payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (payment_id, event_type, event_timestamp) DO NOTHING
            """, records)
        conn.commit()

        logger.info(json.dumps({
            "event": "raw_payments_written",
            "batch_id": batch_id,
            "rows": len(records),
        }))

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"write_payments_batch failed: {e}")
    finally:
        if conn:
            conn.close()


# ── RECONCILIATION FROM POSTGRESQL ────────────────────────────

def reconcile_from_postgres(batch_id: int):
    """
    Join the three raw tables in PostgreSQL and write reconciled records.

    This runs after each order batch lands in raw_order_events.
    Uses SQL joins on static data — no Spark streaming constraints.
    Idempotent: ON CONFLICT DO UPDATE handles re-runs cleanly.
    """
    conn = None
    try:
        conn = get_postgres_connection()
        start = time.time()

        with conn.cursor() as cur:
            cur.execute("""
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
                )
                SELECT DISTINCT ON (o.order_id, o.event_timestamp)
                    o.order_id,
                    o.event_timestamp,
                    o.customer_id,
                    o.total_amount,
                    o.status                                          AS order_status,
                    p.payment_id,
                    p.event_timestamp                                 AS payment_confirmed_at,
                    p.gateway                                         AS payment_gateway,
                    p.amount                                          AS payment_amount,
                    CASE
                        WHEN p.payment_id IS NOT NULL
                         AND EXTRACT(EPOCH FROM (p.event_timestamp - o.event_timestamp))
                             <= %(payment_sla)s
                        THEN TRUE ELSE FALSE
                    END                                               AS payment_sla_met,
                    CASE
                        WHEN p.payment_id IS NOT NULL
                        THEN ROUND(EXTRACT(EPOCH FROM
                             (p.event_timestamp - o.event_timestamp))::numeric, 2)
                        ELSE NULL
                    END                                               AS payment_latency_seconds,
                    i.inventory_id,
                    i.event_timestamp                                 AS inventory_reserved_at,
                    i.sku,
                    CASE
                        WHEN i.inventory_id IS NOT NULL
                         AND EXTRACT(EPOCH FROM (i.event_timestamp - o.event_timestamp))
                             <= %(inventory_sla)s
                        THEN TRUE ELSE FALSE
                    END                                               AS inventory_sla_met,
                    CASE
                        WHEN i.inventory_id IS NOT NULL
                        THEN ROUND(EXTRACT(EPOCH FROM
                             (i.event_timestamp - o.event_timestamp))::numeric, 2)
                        ELSE NULL
                    END                                               AS inventory_latency_seconds,
                    NOW()                                             AS reconciled_at
                FROM raw_order_events o
                LEFT JOIN raw_payment_events p
                    ON p.order_id = o.order_id
                    AND p.event_type = 'PAYMENT_CONFIRMED'
                    AND p.event_timestamp >= o.event_timestamp
                    AND p.event_timestamp <= o.event_timestamp
                        + INTERVAL '60 seconds'
                LEFT JOIN raw_inventory_events i
                    ON i.order_id = o.order_id
                    AND i.event_type = 'INVENTORY_RESERVED'
                    AND i.event_timestamp >= o.event_timestamp
                    AND i.event_timestamp <= o.event_timestamp
                        + INTERVAL '60 seconds'
                WHERE o.event_type = 'ORDER_CREATED'
                ORDER BY o.order_id, o.event_timestamp, p.event_timestamp ASC
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
            """, {
                "payment_sla": PAYMENT_SLA_SECONDS,
                "inventory_sla": INVENTORY_SLA_SECONDS,
            })

            reconciled_count = cur.rowcount
        conn.commit()

        duration_ms = int((time.time() - start) * 1000)

        write_pipeline_metrics(
            batch_id=batch_id,
            input_rows=0,
            reconciled_rows=reconciled_count,
            dlq_rows=0,
            processing_duration_ms=duration_ms,
        )

        logger.info(json.dumps({
            "event": "batch_completed",
            "batch_id": batch_id,
            "reconciled_rows": reconciled_count,
            "duration_ms": duration_ms,
        }))

    except Exception as e:
        logger.error(json.dumps({
            "event": "reconciliation_failed",
            "batch_id": batch_id,
            "error": str(e),
        }))
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def run():
    logger.info("Starting ecommerce reconciliation streaming job")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ONE stream reading ALL three topics simultaneously
    # Spark assigns each message a "topic" column so we can route them
    all_streams = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", "order-events,payment-events,inventory-events")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    def process_all_topics(batch_df: DataFrame, batch_id: int):
        if batch_df.rdd.isEmpty():
            return

        start_time = time.time()

        # Split by topic
        orders_raw = batch_df.filter(col("topic") == "order-events")
        payments_raw = batch_df.filter(col("topic") == "payment-events")
        inventory_raw = batch_df.filter(col("topic") == "inventory-events")

        # Parse each topic's payload
        from pyspark.sql.functions import from_json
        orders_df = orders_raw.select(
            from_json(col("value").cast("string"), ORDER_SCHEMA).alias("d")
        ).select("d.*").filter(col("order_id").isNotNull())

        payments_df = payments_raw.select(
            from_json(col("value").cast("string"), PAYMENT_SCHEMA).alias("d")
        ).select("d.*").filter(col("payment_id").isNotNull())

        inventory_df = inventory_raw.select(
            from_json(col("value").cast("string"), INVENTORY_SCHEMA).alias("d")
        ).select("d.*").filter(col("inventory_id").isNotNull())

        # Write raw events to PostgreSQL
        order_count = 0
        payment_count = 0
        inventory_count = 0

        if not orders_df.rdd.isEmpty():
            write_orders_batch(orders_df, batch_id)
            order_count = orders_df.count()

        if not payments_df.rdd.isEmpty():
            write_payments_batch(payments_df, batch_id)
            payment_count = payments_df.count()

        if not inventory_df.rdd.isEmpty():
            write_inventory_batch(inventory_df, batch_id)
            inventory_count = inventory_df.count()

        # Reconcile via PostgreSQL SQL join
        if order_count > 0 or payment_count > 0 or inventory_count > 0:
            reconcile_from_postgres(batch_id)

        duration_ms = int((time.time() - start_time) * 1000)

        logger.info(json.dumps({
            "event": "batch_completed",
            "batch_id": batch_id,
            "orders": order_count,
            "payments": payment_count,
            "inventory": inventory_count,
            "duration_ms": duration_ms,
        }))

    # ONE query. One checkpoint. One set of executors.
    query = (
        all_streams
        .writeStream
        .foreachBatch(process_all_topics)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/main")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    logger.info(json.dumps({
        "event": "streaming_job_started",
        "trigger_interval": TRIGGER_INTERVAL,
        "kafka_bootstrap": KAFKA_BOOTSTRAP_SERVERS,
    }))

    query.awaitTermination()

if __name__ == "__main__":
    run()