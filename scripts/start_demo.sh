#!/bin/bash
# scripts/start_demo.sh — starts the full pipeline as background processes
set -e
cd "$(dirname "$0")/.."
source ecomvenv/bin/activate
mkdir -p logs

echo "Starting pipeline components..."

PYTHONPATH=. python producers/order_producer.py > logs/orders.log 2>&1 &
echo $! > logs/orders.pid
echo "✓ Order producer (PID $(cat logs/orders.pid))"

PYTHONPATH=. python producers/inventory_producer.py > logs/inventory.log 2>&1 &
echo $! > logs/inventory.pid
echo "✓ Inventory producer (PID $(cat logs/inventory.pid))"

PYTHONPATH=. python -m consumers.stripe_receiver > logs/stripe_receiver.log 2>&1 &
echo $! > logs/stripe_receiver.pid
echo "✓ Stripe receiver (PID $(cat logs/stripe_receiver.pid))"
sleep 3

stripe listen --forward-to localhost:8000/webhook > logs/stripe_listen.log 2>&1 &
echo $! > logs/stripe_listen.pid
echo "✓ Stripe listener (PID $(cat logs/stripe_listen.pid))"
sleep 3

PYTHONPATH=. python -m spark.streaming_job > logs/spark.log 2>&1 &
echo $! > logs/spark.pid
echo "✓ Spark streaming job (PID $(cat logs/spark.pid))"

# DLQ consumer — reads from Kafka dead-letter-events topic
# and provides real-time visibility into validation failures
PYTHONPATH=. python -m consumers.dlq_consumer > logs/dlq_consumer.log 2>&1 &
echo $! > logs/dlq_consumer.pid
echo "✓ DLQ consumer (PID $(cat logs/dlq_consumer.pid))"

echo ""
echo "All components running."
echo "Watch clean Spark output with:"
echo "  make demo-watch"
echo "Stop everything with:"
echo "  make demo-stop"
