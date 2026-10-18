# ─────────────────────────────────────────────────────────────────
# Makefile — One-command operations for the entire platform
#
# WHY A MAKEFILE?
# Instead of memorizing 10 different docker commands, you run:
#   make up       → start everything
#   make topics   → create Kafka topics
#   make produce  → start all three producers
#   make down     → stop everything
#
# HOW TO USE:
#   In WSL2 terminal, from the project root: make <target>
# ─────────────────────────────────────────────────────────────────

.PHONY: help up down logs topics produce-orders produce-inventory \
        produce-payments produce-all dlq test clean status \
        demo demo-stop demo-watch demo-status init

# Default target — shows available commands
help:
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "  E-Commerce Event Platform — Available Commands"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "  make init            First-time setup (up + topics)"
	@echo "  make up              Start all Docker services"
	@echo "  make down            Stop all Docker services"
	@echo "  make topics          Create Kafka topics"
	@echo "  make status          Check all service health"
	@echo "  make produce-orders  Run order event producer"
	@echo "  make produce-all     Run all producers simultaneously"
	@echo "  make dlq             Run DLQ consumer"
	@echo "  make demo            Start full pipeline (background, one command)"
	@echo "  make demo-watch      Watch clean filtered Spark logs"
	@echo "  make demo-status     Check which demo processes are running"
	@echo "  make demo-stop       Stop all demo processes gracefully"
	@echo "  make logs            Tail logs from all services"
	@echo "  make test            Run test suite"
	@echo "  make clean           Remove all containers and volumes"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ── INFRASTRUCTURE ─────────────────────────────────────────────

# Copy .env.example to .env if .env doesn't exist
.env:
	@echo "Creating .env from .env.example..."
	cp .env.example .env
	@echo "✓ .env created. Edit it if you need custom values."

up: .env
	@echo "Starting all services..."
	docker compose up -d
	@echo "Waiting for services to be healthy..."
	@sleep 15
	@$(MAKE) status

init: up
	@echo "Waiting for services to be fully ready..."
	@sleep 20
	@$(MAKE) topics
	@echo ""
	@echo "✓ Infrastructure ready."
	@echo "  Run: make demo"	

down:
	docker compose down

clean:
	@echo "WARNING: This removes all data volumes!"
	@read -p "Are you sure? (y/N): " confirm && [ "$$confirm" = "y" ]
	docker compose down -v --remove-orphans

logs:
	docker compose logs -f

# ── KAFKA ──────────────────────────────────────────────────────

topics:
	@echo "Creating Kafka topics..."
	@docker exec kafka bash /scripts/create-topics.sh 2>/dev/null || \
	  docker exec kafka kafka-topics \
	    --bootstrap-server localhost:9093 \
	    --create --topic order-events --partitions 3 --replication-factor 1 --if-not-exists && \
	  docker exec kafka kafka-topics \
	    --bootstrap-server localhost:9093 \
	    --create --topic inventory-events --partitions 3 --replication-factor 1 --if-not-exists && \
	  docker exec kafka kafka-topics \
	    --bootstrap-server localhost:9093 \
	    --create --topic payment-events --partitions 3 --replication-factor 1 --if-not-exists && \
	  docker exec kafka kafka-topics \
            --bootstrap-server localhost:9093 \
            --create --topic dead-letter-events \
            --partitions 1 --replication-factor 1 \
            --if-not-exists && \
       docker exec kafka kafka-topics \
            --bootstrap-server localhost:9093 \
            --create --topic stripe-raw-events \
            --partitions 3 --replication-factor 1 \
            --config retention.ms=604800000 \
            --if-not-exists
	@echo "✓ Topics created. View at http://localhost:8080"

list-topics:
	docker exec kafka kafka-topics --list --bootstrap-server localhost:9093

# ── STATUS CHECK ───────────────────────────────────────────────

status:
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "Service Health Check"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" \
	  --filter "name=kafka" \
	  --filter "name=zookeeper" \
	  --filter "name=postgres" \
	  --filter "name=minio"
	@echo ""
	@echo "Interfaces:"
	@echo "  Kafka UI:       http://localhost:8080"
	@echo "  MinIO Console:  http://localhost:9001"
	@echo "  Postgres:       localhost:5432"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ── PRODUCERS ──────────────────────────────────────────────────

produce-orders:
	PYTHONPATH=. python producers/order_producer.py

produce-inventory:
	PYTHONPATH=. python producers/inventory_producer.py

produce-payments:
	PYTHONPATH=. python producers/payment_producer.py

# Run all three producers simultaneously in background
# Each runs in its own terminal-friendly background process
produce-all:
	@echo "Starting all producers (Ctrl+C to stop all)..."
	PYTHONPATH=. python producers/order_producer.py &
	PYTHONPATH=. python producers/inventory_producer.py &
	PYTHONPATH=. python producers/payment_producer.py &
	wait

# ── CONSUMERS ──────────────────────────────────────────────────

dlq:
	PYTHONPATH=. python consumers/dlq_consumer.py


# ── DEMO ────────────────────────────────────────────────────────
# One-command start/stop for the full pipeline — avoids opening
# 5-6 separate terminal windows manually every time.

demo:
	@bash scripts/start_demo.sh

demo-stop:
	@bash scripts/stop_demo.sh

demo-watch:
	@echo "==================================================="
	@echo "  Real-Time E-Commerce Reconciliation Pipeline"
	@echo "  Watching live batch output (Ctrl+C to stop)"
	@echo "==================================================="
	@tail -f logs/spark.log | grep --line-buffered -E \
	  "batch_completed|streaming_job_started|dlq_write_success|dlq_kafka_published|failed|ERROR" \
	  | while IFS= read -r line; do \
	    echo "  $$(date '+%H:%M:%S') $$line"; \
	  done

# Quick check on whether demo processes are still alive
demo-status:
	@echo "==================================================="
	@echo "  Pipeline Component Status"
	@echo "==================================================="
	@for name in orders inventory stripe_receiver stripe_listen spark; do \
		pidfile="logs/$$name.pid"; \
		if [ -f "$$pidfile" ]; then \
			pid=$$(cat "$$pidfile"); \
			if kill -0 "$$pid" 2>/dev/null; then \
				echo "  [OK] $$name (PID $$pid)"; \
			else \
				echo "  [STALE] $$name (PID $$pid)"; \
			fi; \
		else \
			echo "  [NOT STARTED] $$name"; \
		fi; \
	done
	@echo "==================================================="


# ── TESTING ────────────────────────────────────────────────────

test:
	PYTHONPATH=. pytest tests/ -v

test-fast:
	PYTHONPATH=. pytest tests/ -v -x  # Stop on first failure

# ── DATABASE ───────────────────────────────────────────────────

# Connect to Postgres via psql (useful for manual inspection)
psql:
	docker exec -it postgres psql -U ecommerce_user -d ecommerce_events

# Show table row counts (quick sanity check)
db-counts:
	docker exec postgres psql -U ecommerce_user -d ecommerce_events -c \
	  "SELECT 'raw_order_events' as table_name, COUNT(*) FROM raw_order_events \
	   UNION ALL SELECT 'raw_inventory_events', COUNT(*) FROM raw_inventory_events \
	   UNION ALL SELECT 'raw_payment_events', COUNT(*) FROM raw_payment_events \
	   UNION ALL SELECT 'dead_letter_events', COUNT(*) FROM dead_letter_events \
	   UNION ALL SELECT 'reconciled_order_events', COUNT(*) FROM reconciled_order_events;"

# ── PYTHON ENVIRONMENT ─────────────────────────────────────────

install:
	pip install -r requirements.txt

venv:
	python3 -m venv venv
	@echo "Run: source venv/bin/activate"
	@echo "Then: make install"
