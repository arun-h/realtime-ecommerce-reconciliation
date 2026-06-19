"""
daily_aggregations.py — Daily summary aggregation DAG

WHAT THIS DAG DOES:
Runs every day at 7 AM (after quality_checks at 6 AM) and builds
two summary tables from raw reconciliation data:

1. hourly_sla_summary — payment/inventory SLA compliance by hour
2. dlq_daily_summary — DLQ failure breakdown by topic/reason

WHY THESE TABLES EXIST:
Querying raw event tables for trend analysis is expensive at scale.
Pre-aggregating into summary tables makes dashboards and reports
fast, and gives a historical record even after raw events are
purged or archived.

IDEMPOTENCY:
Both tasks use ON CONFLICT DO UPDATE so re-running this DAG for
the same day (e.g. backfill or manual trigger) overwrites rather
than duplicates.
"""

import logging
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def get_connection():
    import os
    from dotenv import load_dotenv
    load_dotenv()
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB"),
    )


# ── TASK 1: HOURLY SLA SUMMARY ─────────────────────────────────
def build_hourly_sla_summary(**context):
    """
    Aggregate reconciled_order_events into hourly SLA buckets
    for the previous calendar day.

    WHY PREVIOUS DAY?
    This DAG runs at 7 AM. Running it against "today" would
    capture a partial day. We always aggregate yesterday's
    complete data, which is the standard pattern for daily
    batch reporting.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO hourly_sla_summary (
                    summary_date, hour, total_orders,
                    payment_sla_met, payment_sla_rate,
                    inventory_sla_met, inventory_sla_rate
                )
                SELECT
                    DATE(event_timestamp)                  AS summary_date,
                    EXTRACT(HOUR FROM event_timestamp)::int AS hour,
                    COUNT(*)                                AS total_orders,
                    COUNT(*) FILTER (WHERE payment_sla_met)
                        AS payment_sla_met,
                    ROUND(
                        COUNT(*) FILTER (WHERE payment_sla_met)::numeric
                        / NULLIF(COUNT(*), 0) * 100, 2
                    )                                       AS payment_sla_rate,
                    COUNT(*) FILTER (WHERE inventory_sla_met)
                        AS inventory_sla_met,
                    ROUND(
                        COUNT(*) FILTER (WHERE inventory_sla_met)::numeric
                        / NULLIF(COUNT(*), 0) * 100, 2
                    )                                       AS inventory_sla_rate
                FROM reconciled_order_events
                WHERE event_timestamp >= CURRENT_DATE - INTERVAL '1 day'
                  AND event_timestamp <  CURRENT_DATE
                GROUP BY DATE(event_timestamp), EXTRACT(HOUR FROM event_timestamp)
                ON CONFLICT (summary_date, hour)
                DO UPDATE SET
                    total_orders        = EXCLUDED.total_orders,
                    payment_sla_met     = EXCLUDED.payment_sla_met,
                    payment_sla_rate    = EXCLUDED.payment_sla_rate,
                    inventory_sla_met   = EXCLUDED.inventory_sla_met,
                    inventory_sla_rate  = EXCLUDED.inventory_sla_rate
            """)
            rows_affected = cur.rowcount
        conn.commit()
        logger.info(f"hourly_sla_summary: {rows_affected} hour buckets written")

    finally:
        conn.close()


# ── TASK 2: DLQ DAILY SUMMARY ──────────────────────────────────
def build_dlq_daily_summary(**context):
    """
    Aggregate dead_letter_events into a daily breakdown by
    topic and failure reason for the previous calendar day.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO dlq_daily_summary (
                    summary_date, source_topic, failure_reason, event_count
                )
                SELECT
                    DATE(failed_at)   AS summary_date,
                    source_topic,
                    failure_reason,
                    COUNT(*)          AS event_count
                FROM dead_letter_events
                WHERE failed_at >= CURRENT_DATE - INTERVAL '1 day'
                  AND failed_at <  CURRENT_DATE
                GROUP BY DATE(failed_at), source_topic, failure_reason
                ON CONFLICT (summary_date, source_topic, failure_reason)
                DO UPDATE SET
                    event_count = EXCLUDED.event_count
            """)
            rows_affected = cur.rowcount
        conn.commit()
        logger.info(f"dlq_daily_summary: {rows_affected} breakdown rows written")

    finally:
        conn.close()


# ── DAG DEFINITION ─────────────────────────────────────────────
with DAG(
    dag_id="daily_aggregations",
    default_args=default_args,
    description="Daily SLA and DLQ summary aggregation",
    schedule_interval="0 7 * * *",   # 7 AM daily, after quality checks
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["aggregation", "reporting", "daily"],
) as dag:

    t1 = PythonOperator(
        task_id="build_hourly_sla_summary",
        python_callable=build_hourly_sla_summary,
    )

    t2 = PythonOperator(
        task_id="build_dlq_daily_summary",
        python_callable=build_dlq_daily_summary,
    )

    [t1, t2]
