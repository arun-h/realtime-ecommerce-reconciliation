#!/bin/bash
# scripts/stop_demo.sh — gracefully stops all background pipeline processes
cd "$(dirname "$0")/.."
echo "Stopping pipeline components..."

for name in orders inventory stripe_receiver stripe_listen spark dlq_consumer; do
    pidfile="logs/${name}.pid"
    if [ -f "$pidfile" ]; then
        pid=$(cat "$pidfile")
        if kill -0 "$pid" 2>/dev/null; then
            kill -SIGINT "$pid"
            echo "✓ Sent shutdown signal to $name (PID $pid)"
        else
            echo "  $name (PID $pid) already stopped"
        fi
        rm -f "$pidfile"
    fi
done

echo "Waiting 5s for graceful shutdown..."
sleep 5
echo "Done."
