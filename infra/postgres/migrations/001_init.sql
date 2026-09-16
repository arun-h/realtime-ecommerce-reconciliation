-- 001_init.sql — Database schema definition

CREATE TABLE IF NOT EXISTS raw_order_events (
    -- Composite primary key enforces idempotency at the DB layer.
    -- WHY COMPOSITE KEY over a single auto-increment ID?
    -- Auto-increment IDs are generated at insert time — duplicates
    -- get different IDs and both get stored. That's wrong.
    -- Composite keys on business fields are DETERMINISTIC:
    -- the same event always produces the same key → duplicate = conflict.
    order_id        VARCHAR(50)  NOT NULL,
    event_type      VARCHAR(30)  NOT NULL,  -- e.g. ORDER_CREATED, ORDER_CANCELLED
    event_timestamp TIMESTAMPTZ  NOT NULL,
    customer_id     VARCHAR(50),
    total_amount    NUMERIC(10,2),
    currency        VARCHAR(3),
    status          VARCHAR(20),
    raw_payload     JSONB,                  -- Full original event stored for auditability
    ingested_at     TIMESTAMPTZ  DEFAULT NOW(),

    PRIMARY KEY (order_id, event_type, event_timestamp)
);

CREATE TABLE IF NOT EXISTS raw_inventory_events (
    inventory_id    VARCHAR(50)  NOT NULL,
    event_type      VARCHAR(30)  NOT NULL,  -- e.g. INVENTORY_RESERVED, INVENTORY_RELEASED
    event_timestamp TIMESTAMPTZ  NOT NULL,
    sku             VARCHAR(50),
    quantity_delta  INTEGER,                -- Positive = added, Negative = removed
    warehouse_id    VARCHAR(30),
    order_id        VARCHAR(50),            -- Links back to order domain.
                                            -- Populated for INVENTORY_RESERVED events only.
                                            -- RESTOCKED/ADJUSTED events have no associated order.
    raw_payload     JSONB,
    ingested_at     TIMESTAMPTZ  DEFAULT NOW(),
    PRIMARY KEY (inventory_id, event_type, event_timestamp)
);

CREATE TABLE IF NOT EXISTS raw_payment_events (
    payment_id      VARCHAR(50)  NOT NULL,
    event_type      VARCHAR(50)  NOT NULL,  -- Widened to 50: Stripe event types exceed 30 chars
    event_timestamp TIMESTAMPTZ  NOT NULL,
    order_id        VARCHAR(50),            -- Links back to order domain
    amount          NUMERIC(10,2),
    gateway         VARCHAR(30),            -- e.g. STRIPE, PAYPAL
    status          VARCHAR(50),            -- Widened from 20->50: Stripe status strings
    raw_payload     JSONB,
    ingested_at     TIMESTAMPTZ  DEFAULT NOW(),
    PRIMARY KEY (payment_id, event_type, event_timestamp)
);

-- ── DEAD LETTER TABLE ──────────────────────────────────────────
-- Events that failed validation land here instead of being silently dropped.
-- WHY A TABLE AND NOT JUST LOGS?
-- Logs disappear. This table gives you:
--   1. Count of failures over time (trend alerting)
--   2. The actual bad payload (debugging)
--   3. The specific reason it failed (categorization)
-- In production you'd also write these to a Kafka DLQ topic for replay.

CREATE TABLE IF NOT EXISTS dead_letter_events (
    id              BIGSERIAL    PRIMARY KEY,
    source_topic    VARCHAR(50)  NOT NULL,
    raw_payload     TEXT         NOT NULL,  -- TEXT not JSONB — it might not be valid JSON
    failure_reason  VARCHAR(200) NOT NULL,
    failed_at       TIMESTAMPTZ  DEFAULT NOW()
);

-- ── RECONCILED EVENTS TABLE ────────────────────────────────────
-- This is the gold layer. Spark writes here after joining order +
-- payment + inventory events within a 60-second event-time window.
-- A record here means: we saw the order event. Payment and inventory
-- columns are null if those events did not arrive within the window.

CREATE TABLE IF NOT EXISTS reconciled_order_events (
    order_id                VARCHAR(50)  NOT NULL,
    event_timestamp         TIMESTAMPTZ  NOT NULL,  -- Order creation time (event-time)
    customer_id             VARCHAR(50),
    total_amount            NUMERIC(10,2),
    order_status            VARCHAR(20),            -- Order status at reconciliation time
    payment_id              VARCHAR(50),
    payment_confirmed_at    TIMESTAMPTZ,
    payment_gateway         VARCHAR(30),
    payment_amount          NUMERIC(10,2),          -- Amount from matched payment event
    inventory_id            VARCHAR(50),
    inventory_reserved_at   TIMESTAMPTZ,
    sku                     VARCHAR(50),

    -- SLA TRACKING: Did payment confirm within 60 seconds of order?
    -- WHY STORE THIS AS A COLUMN?
    -- Recalculating from timestamps at query time is expensive.
    -- Storing it at write time makes dashboard queries instant.
    payment_sla_met         BOOLEAN,
    payment_latency_seconds NUMERIC(10,2),

    -- Inventory SLA: Did inventory reserve within 60 seconds of order?
    inventory_sla_met       BOOLEAN,
    inventory_latency_seconds NUMERIC(10,2),

    reconciled_at           TIMESTAMPTZ  DEFAULT NOW(),
    raw_payload             JSONB,

    -- WHY THIS COMPOSITE KEY?
    -- order_id alone isn't enough — an order can have multiple events.
    -- order_id + event_timestamp uniquely identifies one reconciled snapshot.
    -- ON CONFLICT DO UPDATE means: if we replay the same event,
    -- we UPDATE the existing record instead of failing or duplicating.
    PRIMARY KEY (order_id, event_timestamp)
);

-- ── OBSERVABILITY TABLE ────────────────────────────────────────
-- Each Spark micro-batch writes one row here.
-- Gives you a time-series of pipeline health without external monitoring tools.

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id                          BIGSERIAL    PRIMARY KEY,
    batch_id                    BIGINT       NOT NULL,
    batch_timestamp             TIMESTAMPTZ  DEFAULT NOW(),
    input_rows                  INTEGER,
    dlq_events                  INTEGER,
    reconciliation_failures     INTEGER,
    reconciliation_failure_rate NUMERIC(6,4),
    consumer_lag_ms             BIGINT,
    sla_violations              INTEGER,
    processing_duration_ms      BIGINT
);

-- ── AIRFLOW QUALITY CHECK RESULTS ─────────────────────────────
-- Written by the daily_quality_checks Airflow DAG.
-- One row per check per day. Enables historical trending of
-- pipeline health across: row counts, null rates, DLQ growth,
-- and batch processing performance.
--
-- WHY NOT JUST READ pipeline_metrics DIRECTLY IN AIRFLOW?
-- Quality checks apply business-rule thresholds (e.g. null rate > 10%
-- = FAIL). Storing the evaluated PASS/FAIL/WARN result separately
-- means the check history is preserved even if the raw metric
-- tables are truncated or archived.

CREATE TABLE IF NOT EXISTS quality_check_results (
    id              BIGSERIAL    PRIMARY KEY,
    check_name      VARCHAR(100) NOT NULL,
    check_date      DATE         NOT NULL,
    status          VARCHAR(20)  NOT NULL,  -- PASS, FAIL, WARN, INFO
    expected_value  NUMERIC,
    actual_value    NUMERIC,
    details         TEXT,
    checked_at      TIMESTAMPTZ  DEFAULT NOW()
);

-- ── HOURLY SLA SUMMARY ─────────────────────────────────────────
-- Written by the daily_aggregations Airflow DAG.
-- Pre-aggregated hourly SLA compliance rates for the previous day.
--
-- WHY PRE-AGGREGATE?
-- Querying reconciled_order_events for trend analysis at scale is
-- expensive (full scan + GROUP BY). Pre-aggregation makes historical
-- SLA trend queries instant regardless of raw table size.
-- UNIQUE constraint on (summary_date, hour) makes the DAG idempotent —
-- re-running for the same day overwrites rather than duplicates.

CREATE TABLE IF NOT EXISTS hourly_sla_summary (
    id                  BIGSERIAL    PRIMARY KEY,
    summary_date        DATE         NOT NULL,
    hour                INTEGER      NOT NULL,
    total_orders        INTEGER,
    payment_sla_met     INTEGER,
    payment_sla_rate    NUMERIC(5,2),
    inventory_sla_met   INTEGER,
    inventory_sla_rate  NUMERIC(5,2),
    created_at          TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE(summary_date, hour)
);

-- ── DLQ DAILY SUMMARY ──────────────────────────────────────────
-- Written by the daily_aggregations Airflow DAG.
-- Daily breakdown of dead-letter events by topic and failure reason.
--
-- WHY SUMMARIZE THE DLQ?
-- Raw dead_letter_events table grows indefinitely. This summary gives
-- a compact, queryable view for trending: "is MISSING_SKU increasing
-- over time?" UNIQUE constraint makes the DAG idempotent.

CREATE TABLE IF NOT EXISTS dlq_daily_summary (
    id              BIGSERIAL    PRIMARY KEY,
    summary_date    DATE         NOT NULL,
    source_topic    VARCHAR(50),
    failure_reason  VARCHAR(200),
    event_count     INTEGER,
    created_at      TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE(summary_date, source_topic, failure_reason)
);

-- ── INDEXES ────────────────────────────────────────────────────
-- WHY INDEXES?
-- Without indexes, Postgres scans EVERY row to answer a query.
-- With indexes, it jumps directly to the relevant rows.
-- Rule of thumb: index columns you filter or JOIN on frequently.

-- Order lookups by customer (dashboard: "show orders for customer X")
CREATE INDEX IF NOT EXISTS idx_raw_orders_customer
    ON raw_order_events(customer_id);

-- Order lookups by time range (dashboard: "show orders from last hour")
CREATE INDEX IF NOT EXISTS idx_raw_orders_timestamp
    ON raw_order_events(event_timestamp DESC);

-- Payment lookups by order_id (reconciliation JOIN)
CREATE INDEX IF NOT EXISTS idx_raw_payments_order
    ON raw_payment_events(order_id);

-- Inventory lookups by order_id (reconciliation JOIN)
CREATE INDEX IF NOT EXISTS idx_raw_inventory_order
    ON raw_inventory_events(order_id);

-- SLA violation queries (dashboard: "show SLA failures today")
CREATE INDEX IF NOT EXISTS idx_reconciled_sla
    ON reconciled_order_events(payment_sla_met, event_timestamp DESC);

-- DLQ analysis by topic (debugging: "how many failures per topic?")
CREATE INDEX IF NOT EXISTS idx_dlq_topic_time
    ON dead_letter_events(source_topic, failed_at DESC);

-- Quality check history by date (Airflow: "show last 30 days")
CREATE INDEX IF NOT EXISTS idx_quality_checks_date
    ON quality_check_results(check_date DESC, check_name);


COMMENT ON TABLE reconciled_order_events IS
    'Gold layer: one row per order event processed by Spark. '
    'Payment and inventory columns are null if those events did not '
    'arrive within the 60-second SLA window. '
    'Idempotent via ON CONFLICT DO UPDATE.';

COMMENT ON TABLE dead_letter_events IS
    'Events that failed schema parsing or null-key filtering. '
    'Written by Spark streaming job. '
    'Never silently dropped. Use for debugging and failure rate trending.';

COMMENT ON TABLE quality_check_results IS
    'Daily pipeline health check results written by the '
    'daily_quality_checks Airflow DAG. One row per check per day.';

COMMENT ON TABLE hourly_sla_summary IS
    'Pre-aggregated hourly SLA compliance rates for the previous day. '
    'Written by the daily_aggregations Airflow DAG.';

COMMENT ON TABLE dlq_daily_summary IS
    'Daily dead-letter event breakdown by topic and failure reason. '
    'Written by the daily_aggregations Airflow DAG.';

COMMENT ON COLUMN reconciled_order_events.payment_sla_met IS
    'True if payment confirmed within 60 seconds of order creation '
    '(event-time comparison). False if payment arrived late or is absent.';

COMMENT ON COLUMN reconciled_order_events.order_status IS
    'Order status field at the time of reconciliation. '
    'Sourced from raw_order_events.status via the reconciliation SQL join.';