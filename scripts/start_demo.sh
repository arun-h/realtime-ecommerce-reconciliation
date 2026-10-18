#!/bin/bash
# scripts/start_demo.sh — starts the full pipeline as background processes
set -e
cd "$(dirname "$0")/.."
source ecomvenv/bin/activate
mkdir -p logs

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Real-Time E-Commerce Reconciliation Pipeline"
echo "  Starting all components..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

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

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  All 5 components running"
echo ""
echo "  make demo-watch    → live pipeline output"
echo "  make demo-status   → check process health"
echo "  make db-counts     → current row counts"
echo "  make demo-stop     → graceful shutdown"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
