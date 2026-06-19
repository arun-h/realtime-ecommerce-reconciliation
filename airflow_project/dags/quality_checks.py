"""
quality_checks.py — Daily data quality checks DAG

WHAT THIS DAG DOES:
Runs every day at 6 AM and checks the health of the reconciliation
pipeline by querying PostgreSQL directly. Results are written to
quality_check_results table for trending and alerting.

WHY AIRFLOW FOR THIS?
These checks need to run on a schedule regardless of whether the
Spark streaming job is running. Airflow provides:
- Scheduled execution with retry logic
- Task dependency management
- Execution history and failure alerting
- Clear separation of batch orchestration from streaming logic

CHECKS PERFORMED:
1. Raw table row counts — are events still flowing?
2. Null rate on critical fields — is data quality degrading?
3. DLQ growth rate — is failure rate increasing?
4. Reconciliation rate — what % of orders are being reconciled?
5. Pipeline metrics check — are batches completing on time?
"""

import logging
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# ── DAG DEFAULT ARGS ───────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def get_connection():
    """Get PostgreSQL connection using environment variables."""
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


def write_check_result(
    conn, check_name: str, status: str,
    expected=None, actual=None, details: str = ""
):
    """Write one quality check result to PostgreSQL."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO quality_check_results
                (check_name, check_date, status,
                 expected_value, actual_value, details)
            VALUES (%s, CURRENT_DATE, %s, %s, %s, %s)
        """, (check_name, status, expected, actual, details))
    conn.commit()
    logger.info(f"Quality check: {check_name} → {status} "
                f"(expected={expected}, actual={actual})")


# ── TASK 1: ROW COUNT CHECKS ───────────────────────────────────
def check_row_counts(**context):
    """
    Verify each raw table received events in the last 24 hours.

    WHY 24 HOURS?
    We run this check daily. If a table has zero new rows in
    the last 24 hours, it indicates either the producer stopped
    or the Spark job stopped writing. Both need investigation.

    THRESHOLD:
    Minimum 10 events per table per day is a conservative floor.
    At our production rate (2 events/sec for orders), we expect
    ~170,000 per day. Zero is an outage. Below 10 is suspicious.
    """
    conn = get_connection()
    try:
        checks = {
            "raw_order_events_24h":     "raw_order_events",
            "raw_payment_events_24h":   "raw_payment_events",
            "raw_inventory_events_24h": "raw_inventory_events",
        }

        for check_name, table in checks.items():
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT COUNT(*)
                    FROM {table}
                    WHERE ingested_at >= NOW() - INTERVAL '24 hours'
                """)
                count = cur.fetchone()[0]

            status = "PASS" if count >= 10 else "FAIL"
            write_check_result(
                conn=conn,
                check_name=check_name,
                status=status,
                expected=10,
                actual=count,
                details=f"Events ingested in last 24h: {count}"
            )

        # Reconciliation coverage check
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) AS total_orders,
                    COUNT(payment_id) AS reconciled_with_payment,
                    ROUND(
                        COUNT(payment_id)::numeric / NULLIF(COUNT(*), 0) * 100,
                        2
                    ) AS reconciliation_rate
                FROM reconciled_order_events
                WHERE reconciled_at >= NOW() - INTERVAL '24 hours'
            """)
            row = cur.fetchone()
            total, reconciled, rate = row
            rate = rate or 0

        write_check_result(
            conn=conn,
            check_name="reconciliation_rate_24h",
            status="PASS" if rate > 0 else "WARN",
            expected=None,
            actual=float(rate),
            details=f"Total orders: {total}, "
                    f"Reconciled with payment: {reconciled}, "
                    f"Rate: {rate}%"
        )

    finally:
        conn.close()


# ── TASK 2: NULL RATE CHECKS ───────────────────────────────────
def check_null_rates(**context):
    """
    Check null rates on fields that must be populated.

    THRESHOLD: 10% null rate is acceptable given 5% fault injection
    in order producer and 3% in inventory producer. Beyond 10%
    suggests a producer schema change or validation bug.

    WHY CHECK NULLS?
    Silent null propagation is one of the hardest data quality
    problems to detect. An event with null event_type gets written
    to the raw table but is invisible to reconciliation. Trending
    null rates catch schema drift before it becomes an outage.
    """
    conn = get_connection()
    try:
        null_checks = [
            ("raw_order_events",     "event_type"),
            ("raw_order_events",     "event_timestamp"),
            ("raw_order_events",     "total_amount"),
            ("raw_payment_events",   "payment_id"),
            ("raw_payment_events",   "order_id"),
            ("raw_inventory_events", "sku"),
            ("raw_inventory_events", "quantity_delta"),
        ]

        for table, column in null_checks:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT
                        COUNT(*) AS total,
                        COUNT(*) FILTER (WHERE {column} IS NULL) AS nulls,
                        ROUND(
                            COUNT(*) FILTER (WHERE {column} IS NULL)::numeric
                            / NULLIF(COUNT(*), 0) * 100,
                            2
                        ) AS null_rate
                    FROM {table}
                    WHERE ingested_at >= NOW() - INTERVAL '24 hours'
                """)
                total, nulls, null_rate = cur.fetchone()
                null_rate = float(null_rate or 0)

            check_name = f"null_rate_{table}_{column}"
            status = "PASS" if null_rate <= 10.0 else "FAIL"
            write_check_result(
                conn=conn,
                check_name=check_name,
                status=status,
                expected=10.0,
                actual=null_rate,
                details=f"Total: {total}, Nulls: {nulls}, "
                        f"Null rate: {null_rate}%"
            )

    finally:
        conn.close()


# ── TASK 3: DLQ GROWTH CHECK ───────────────────────────────────
def check_dlq_growth(**context):
    """
    Compare today's DLQ volume to yesterday's.

    A growing DLQ indicates either:
    1. A producer started sending more malformed events
    2. A schema change broke validation
    3. An upstream system is degrading

    THRESHOLD: DLQ count more than 2x yesterday's count = WARN.
    DLQ count more than 5x yesterday's count = FAIL.

    WHY NOT A FIXED THRESHOLD?
    Event volume varies by time of day and day of week.
    A fixed threshold of "100 DLQ events = fail" would
    alert on Monday morning but miss a 10x spike on Sunday.
    Ratio-based alerting is more robust.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (
                        WHERE failed_at >= CURRENT_DATE
                    ) AS today_count,
                    COUNT(*) FILTER (
                        WHERE failed_at >= CURRENT_DATE - INTERVAL '1 day'
                        AND failed_at < CURRENT_DATE
                    ) AS yesterday_count
                FROM dead_letter_events
            """)
            today, yesterday = cur.fetchone()

        ratio = (today / yesterday) if yesterday > 0 else 0

        if yesterday == 0:
            status = "PASS"
            details = f"No DLQ events yesterday. Today: {today}"
        elif ratio > 5:
            status = "FAIL"
            details = (f"DLQ spike: {today} today vs "
                       f"{yesterday} yesterday ({ratio:.1f}x)")
        elif ratio > 2:
            status = "WARN"
            details = (f"DLQ growing: {today} today vs "
                       f"{yesterday} yesterday ({ratio:.1f}x)")
        else:
            status = "PASS"
            details = (f"DLQ stable: {today} today vs "
                       f"{yesterday} yesterday")

        write_check_result(
            conn=conn,
            check_name="dlq_growth_rate",
            status=status,
            expected=1.0,
            actual=round(ratio, 2),
            details=details
        )

        # Also break down by topic
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source_topic, failure_reason, COUNT(*)
                FROM dead_letter_events
                WHERE failed_at >= NOW() - INTERVAL '24 hours'
                GROUP BY source_topic, failure_reason
                ORDER BY COUNT(*) DESC
                LIMIT 10
            """)
            rows = cur.fetchall()

        if rows:
            breakdown = ", ".join(
                f"{r[0]}/{r[1]}:{r[2]}" for r in rows
            )
            write_check_result(
                conn=conn,
                check_name="dlq_breakdown_24h",
                status="INFO",
                actual=sum(r[2] for r in rows),
                details=f"Top failure reasons: {breakdown}"
            )

    finally:
        conn.close()


# ── TASK 4: PIPELINE METRICS CHECK ────────────────────────────
def check_pipeline_metrics(**context):
    """
    Verify the Spark streaming job is processing batches on time.

    CHECKS:
    - At least one batch completed in last 24 hours
    - Average batch duration is under 60 seconds
      (our trigger interval is 30 seconds — if processing
       takes longer than the interval, we fall behind)
    - Failure rate is under 5%
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) AS batch_count,
                    ROUND(AVG(processing_duration_ms)::numeric, 0)
                        AS avg_duration_ms,
                    ROUND(AVG(reconciliation_failure_rate)::numeric, 4)
                        AS avg_failure_rate,
                    MAX(batch_timestamp) AS last_batch_at
                FROM pipeline_metrics
                WHERE batch_timestamp >= NOW() - INTERVAL '24 hours'
            """)
            row = cur.fetchone()
            batch_count, avg_duration_ms, avg_failure_rate, last_batch = row

        # Check 1: batches ran
        write_check_result(
            conn=conn,
            check_name="pipeline_batches_24h",
            status="PASS" if (batch_count or 0) > 0 else "WARN",
            expected=1,
            actual=batch_count or 0,
            details=f"Batches completed in last 24h: {batch_count}, "
                    f"Last batch: {last_batch}"
        )

        # Check 2: processing time
        if avg_duration_ms:
            status = "PASS" if avg_duration_ms < 60000 else "WARN"
            write_check_result(
                conn=conn,
                check_name="pipeline_processing_time",
                status=status,
                expected=60000,
                actual=float(avg_duration_ms),
                details=f"Avg batch duration: {avg_duration_ms}ms"
            )

        # Check 3: failure rate
        if avg_failure_rate is not None:
            status = "PASS" if avg_failure_rate < 0.05 else "WARN"
            write_check_result(
                conn=conn,
                check_name="pipeline_failure_rate",
                status=status,
                expected=0.05,
                actual=float(avg_failure_rate),
                details=f"Avg reconciliation failure rate: "
                        f"{float(avg_failure_rate):.2%}"
            )

    finally:
        conn.close()


# ── DAG DEFINITION ─────────────────────────────────────────────
with DAG(
    dag_id="daily_quality_checks",
    default_args=default_args,
    description="Daily data quality checks for reconciliation pipeline",
    schedule_interval="0 6 * * *",   # 6 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["quality", "monitoring", "daily"],
) as dag:

    t1 = PythonOperator(
        task_id="check_row_counts",
        python_callable=check_row_counts,
    )

    t2 = PythonOperator(
        task_id="check_null_rates",
        python_callable=check_null_rates,
    )

    t3 = PythonOperator(
        task_id="check_dlq_growth",
        python_callable=check_dlq_growth,
    )

    t4 = PythonOperator(
        task_id="check_pipeline_metrics",
        python_callable=check_pipeline_metrics,
    )

    # Tasks run in parallel — no dependencies between checks
    # If one fails, others still run
    [t1, t2, t3, t4]
