-- ─────────────────────────────────────────────────────────────────
-- 001_init.sql — Database schema definition
--
-- WHY SQL MIGRATION FILES?
-- Your schema should be version-controlled just like code.
-- If someone clones this repo, they get the EXACT same database.
-- In production, tools like Flyway or Alembic run these in order.
-- We use Postgres's init folder for simplicity in development.
-- ─────────────────────────────────────────────────────────────────

-- ── RAW EVENTS TABLES ──────────────────────────────────────────
-- We store raw events from each domain separately BEFORE reconciliation.
-- WHY? Separation of concerns. Raw = what Kafka gave us.
-- Reconciled = what we derived from combining domains.
-- If our reconciliation logic has a bug, we can replay from raw.

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
    raw_payload     JSONB,
    ingested_at     TIMESTAMPTZ  DEFAULT NOW(),

    PRIMARY KEY (inventory_id, event_type, event_timestamp)
);

CREATE TABLE IF NOT EXISTS raw_payment_events (
    payment_id      VARCHAR(50)  NOT NULL,
    event_type      VARCHAR(30)  NOT NULL,  -- e.g. PAYMENT_INITIATED, PAYMENT_CONFIRMED
    event_timestamp TIMESTAMPTZ  NOT NULL,
    order_id        VARCHAR(50),            -- Links back to order domain
    amount          NUMERIC(10,2),
    gateway         VARCHAR(30),            -- e.g. STRIPE, PAYPAL
    status          VARCHAR(20),
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
-- In production you'd also write these to a Kafka DLQ topic.

CREATE TABLE IF NOT EXISTS dead_letter_events (
    id              BIGSERIAL    PRIMARY KEY,
    source_topic    VARCHAR(50)  NOT NULL,
    raw_payload     TEXT         NOT NULL,  -- TEXT not JSONB — it might not be valid JSON
    failure_reason  VARCHAR(200) NOT NULL,
    failed_at       TIMESTAMPTZ  DEFAULT NOW()
);

-- ── RECONCILED EVENTS TABLE ────────────────────────────────────
-- This is the gold layer. Spark joins order + payment + inventory
-- within a 60-second event-time window and writes here.
-- A record here means: we saw all three events for this order.

CREATE TABLE IF NOT EXISTS reconciled_order_events (
    order_id                VARCHAR(50)  NOT NULL,
    event_timestamp         TIMESTAMPTZ  NOT NULL,  -- Order creation time (event-time)
    customer_id             VARCHAR(50),
    total_amount            NUMERIC(10,2),
    payment_id              VARCHAR(50),
    payment_confirmed_at    TIMESTAMPTZ,
    payment_gateway         VARCHAR(30),
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

-- SLA violation queries (dashboard: "show SLA failures today")
CREATE INDEX IF NOT EXISTS idx_reconciled_sla
    ON reconciled_order_events(payment_sla_met, event_timestamp DESC);

-- DLQ analysis by topic (debugging: "how many failures per topic?")
CREATE INDEX IF NOT EXISTS idx_dlq_topic_time
    ON dead_letter_events(source_topic, failed_at DESC);

-- ── COMMENTS ───────────────────────────────────────────────────
-- Self-documenting schema — future engineers (or you in 6 months)
-- understand intent without reading application code.

COMMENT ON TABLE reconciled_order_events IS
    'Gold layer: one row per successfully reconciled order event. '
    'Populated by Spark streaming job. Idempotent via ON CONFLICT DO UPDATE.';

COMMENT ON TABLE dead_letter_events IS
    'Events that failed Pydantic validation or schema enforcement. '
    'Never silently dropped. Use for debugging and failure rate trending.';

COMMENT ON COLUMN reconciled_order_events.payment_sla_met IS
    'True if payment confirmed within 60 seconds of order creation (event-time). '
    'Based on watermarked event-time window — may undercount for events '
    'arriving beyond the 90-second watermark threshold.';
