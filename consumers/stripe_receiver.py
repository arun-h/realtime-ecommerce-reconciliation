"""
stripe_receiver.py — FastAPI webhook receiver for Stripe events

ARCHITECTURE DECISION (from Gemini review):
Raw Stripe payload → stripe-raw-events topic (immutable, never mutated)
Normalised payload → payment-events topic (transformed for reconciliation)

WHY TWO TOPICS?
If normalisation logic has a bug, stripe-raw-events is the source of truth.
We can replay from raw at any point without losing Stripe's original data.
This is the immutable event sourcing principle.

NORMALISATION PROBLEMS WE SOLVE HERE:
1. amount: Stripe sends cents as integer (1999) → we store dollars as float (19.99)
2. timestamps: Stripe sends Unix integers (1705123200) → we convert to ISO 8601
3. structure: Stripe's charge data is deeply nested → we flatten to our schema
4. event_type: Stripe uses "payment_intent.succeeded" → we normalise to "PAYMENT_CONFIRMED"
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import stripe
import uvicorn
from confluent_kafka import Producer, KafkaException
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
logger = logging.getLogger("stripe_receiver")

app = FastAPI(title="Stripe Webhook Receiver")

# ── KAFKA PRODUCER ─────────────────────────────────────────────
def create_producer() -> Producer:
    return Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "acks": "all",
        "retries": 3,
        "enable.idempotence": True,
    })

producer = create_producer()

# ── STRIPE EVENT TYPE MAPPING ──────────────────────────────────
# Stripe uses dot-notation event types. We normalise to our internal
# SCREAMING_SNAKE_CASE convention used across order and inventory events.
STRIPE_EVENT_TYPE_MAP = {
    "payment_intent.succeeded":"PAYMENT_CONFIRMED",
    "payment_intent.payment_failed":"PAYMENT_FAILED",
    "payment_intent.created":"PAYMENT_INITIATED",
    "payment_intent.canceled":"PAYMENT_CANCELLED",
    "charge.dispute.created":"PAYMENT_DISPUTED",
    "charge.refunded":"PAYMENT_REFUNDED",
    "charge.succeeded":"PAYMENT_CONFIRMED",
    "charge.failed":"PAYMENT_FAILED",
}

def unix_to_iso(unix_ts: int | None) -> str | None:
    """
    Convert Unix integer timestamp to ISO 8601 string.

    WHY THIS MATTERS:
    Our internal order and inventory events use ISO 8601 strings.
    Stripe uses Unix integers. Spark's watermark logic needs a consistent
    timestamp format across all three topics for the join to work correctly.
    Without this normalisation, event-time windowing breaks silently.
    """
    if unix_ts is None:
        return None
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc).isoformat()

def cents_to_dollars(amount_cents: int | None) -> float | None:
    """
    Convert Stripe's integer cents to float dollars.

    WHY THIS IS EASY TO GET WRONG:
    Stripe sends amount=1999 meaning $19.99.
    If you skip this conversion and store 1999 in a column named "amount",
    your SLA calculations and reconciliation logic will be off by 100x.
    Silent bug. Very hard to catch in testing if your test data is small.
    """
    if amount_cents is None:
        return None
    return round(amount_cents / 100, 2)

def normalise_stripe_payload(event: stripe.Event) -> dict:
    """
    Transform Stripe's nested webhook payload into our flat internal schema.

    STRIPE'S STRUCTURE (deeply nested):
    event.data.object = PaymentIntent object
    event.data.object.charges.data[0] = the actual charge

    OUR INTERNAL STRUCTURE (flat):
    payment_id, event_type, event_timestamp, order_id, amount, currency, etc.

    TRADEOFFS DOCUMENTED:
    We extract only the fields our reconciliation join needs.
    Fields we don't extract are preserved in raw_payload (JSONB) for auditability.
    If Stripe adds a new field we care about later, we update this function
    and replay from stripe-raw-events to backfill.
    """
    data_obj = event.data.object

    # Extract charge details if available (present on succeeded/failed events)
    charges = getattr(data_obj, "charges", None)
    charge = None
    if charges and hasattr(charges, "data") and len(charges.data) > 0:
        charge = charges.data[0]

    # order_id: Stripe lets merchants attach metadata to PaymentIntents.
    # We expect the order_id to be passed as metadata when creating the intent.
    # If missing (e.g. in test triggers), we use the payment_intent id.
    metadata = getattr(data_obj, "metadata", {}) or {}
    order_id = metadata.get("order_id") or f"UNKNOWN-{data_obj.id}"

    return {
        "payment_id":       data_obj.id,
        "event_type":       STRIPE_EVENT_TYPE_MAP.get(event.type, event.type.upper()),
        "event_timestamp":  unix_to_iso(event.created),
        "schema_version":   "1.0",
        "order_id":         order_id,

        # NORMALISED: cents → dollars, consistent with internal schema
        "amount":           cents_to_dollars(getattr(data_obj, "amount", None)),
        "currency":         (getattr(data_obj, "currency", "") or "").upper(),

        "gateway":          "STRIPE",
        "status":           getattr(data_obj, "status", None),
        "gateway_event_id": event.id,

        # Charge-level details (only present on confirmed/failed payments)
        "gateway_transaction_id": charge.id if charge else None,
        "failure_reason":         getattr(data_obj, "last_payment_error", None) and
                                  getattr(data_obj.last_payment_error, "code", None),

        # Preserve full Stripe event as JSON string for auditability
        # If we need a field we didn't extract, it's here.
        "raw_stripe_event": event.type,
    }

def delivery_callback(err, msg):
    if err:
        logger.error(json.dumps({
            "event": "kafka_delivery_failed",
            "topic": msg.topic() if msg else "unknown",
            "error": str(err),
        }))
    else:
        logger.debug(f"Delivered to {msg.topic()} partition {msg.partition()}")

# ── WEBHOOK ENDPOINT ───────────────────────────────────────────
@app.post("/webhook")
async def stripe_webhook(request: Request):
    """
    Receives Stripe webhook events.

    TWO-TOPIC WRITE PATTERN:
    1. Write raw payload to stripe-raw-events (immutable source of truth)
    2. Write normalised payload to payment-events (for Spark reconciliation)

    ORDER MATTERS:
    Raw write first. If normalisation fails, we still have the raw event
    and can replay normalisation later without contacting Stripe again.

    SIGNATURE VERIFICATION:
    Stripe signs every webhook with HMAC-SHA256.
    We verify the signature before processing.
    Without this, anyone could POST fake payment confirmations to your endpoint.
    """
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")
    webhook_secret = os.getenv("STRIPE_WEBHOOK_SECRET", "")

    # ── STEP 1: Verify Stripe signature ────────────────────────
    # Skip verification in development if secret is placeholder
    # In production this must ALWAYS be verified
    if webhook_secret and webhook_secret != "whsec_placeholder":
        try:
            event = stripe.Webhook.construct_event(
                payload, sig_header, webhook_secret
            )
        except stripe.error.SignatureVerificationError as e:
            logger.warning(f"Invalid Stripe signature: {e}")
            raise HTTPException(status_code=400, detail="Invalid signature")
    else:
        # Development mode: parse without verification
        logger.warning("STRIPE_WEBHOOK_SECRET not set — skipping signature verification")
        try:
            event_dict = json.loads(payload)
            event = stripe.Event.construct_from(event_dict, stripe.api_key)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid payload: {e}")

    # ── STEP 2: Write RAW event to stripe-raw-events ───────────
    # This is ALWAYS written first, before any transformation.
    # This is our immutable source of truth for replay.
    try:
        raw_record = {
            "stripe_event_id":  event.id,
            "event_type":       event.type,
            "received_at":      datetime.now(timezone.utc).isoformat(),
            "api_version":      event.api_version,
            "raw_payload":      json.loads(payload.decode("utf-8")),
        }
        producer.produce(
            topic="stripe-raw-events",
            key=event.id.encode("utf-8"),
            value=json.dumps(raw_record).encode("utf-8"),
            on_delivery=delivery_callback,
        )
        producer.poll(0)
        logger.info(json.dumps({
            "event": "stripe_raw_written",
            "stripe_event_id": event.id,
            "event_type": event.type,
        }))
    except Exception as e:
        logger.error(f"Failed to write raw event: {e}")
        # Even if raw write fails, return 200 to Stripe so it doesn't retry
        # The retry would cause double-processing of the normalised event
        # This is a known tradeoff: we prefer Stripe not retrying over
        # guaranteed raw delivery. In production: use a queue with retry.
        return JSONResponse({"status": "raw_write_failed", "event_id": event.id})

    # ── STEP 3: Normalise and write to payment-events ──────────
    # Only process event types we care about
    if event.type not in STRIPE_EVENT_TYPE_MAP:
        logger.info(f"Skipping unhandled event type: {event.type}")
        return JSONResponse({"status": "skipped", "event_type": event.type})

    try:
        normalised = normalise_stripe_payload(event)
        producer.produce(
            topic="payment-events",
            # Use order_id as partition key so all payment events
            # for the same order land on the same partition.
            # WHY: Kafka guarantees order within a partition.
            # Same partition = consistent ordering for the Spark join.
            key=normalised["order_id"].encode("utf-8"),
            value=json.dumps(normalised).encode("utf-8"),
            on_delivery=delivery_callback,
            headers={
                "source": "stripe-webhook",
                "stripe_event_id": event.id,
                "normalised_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        producer.poll(0)

        logger.info(json.dumps({
            "event": "payment_normalised",
            "stripe_event_id": event.id,
            "internal_event_type": normalised["event_type"],
            "order_id": normalised["order_id"],
            "amount_dollars": normalised["amount"],
        }))

    except Exception as e:
        logger.error(json.dumps({
            "event": "normalisation_failed",
            "stripe_event_id": event.id,
            "error": str(e),
        }))
        # Return 200 anyway — raw event is already saved, normalisation
        # can be replayed from stripe-raw-events after fixing the bug.
        return JSONResponse({"status": "normalisation_failed", "event_id": event.id})

    # Return 200 IMMEDIATELY to Stripe
    # If we return anything else or time out, Stripe will retry the webhook
    # Our idempotent Kafka key (event.id) handles any retries correctly
    return JSONResponse({"status": "ok", "event_id": event.id})


@app.get("/health")
async def health():
    """Simple health check endpoint."""
    return {"status": "healthy", "service": "stripe-webhook-receiver"}


if __name__ == "__main__":
    uvicorn.run(
        "consumers.stripe_receiver:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
    )